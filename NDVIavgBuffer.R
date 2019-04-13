############# Average NDVI within Buffer Analysis ####################

# load necessary packages
library(plyr)
library(tidyverse)
library(sf)
library(tmap)
library(maptools)
library(lwgeom)
library(parallel)
library(foreach)
library(doParallel)
library(itertools)
library(raster)


# READ IN AND PREP DATA
# download town shapefile
download.file(url = "http://download.massgis.digital.mass.gov/shapefiles/state/townssurvey_shp.zip",
              destfile = "shapefiles/townssurvey_shp.zip")

# unzip the downloaded shapefile into the shapefiles directory
unzip(zipfile = "shapefiles/townssurvey_shp.zip", exdir = "./shapefiles")

# read in towns polygons
towns <- st_read(dsn = "shapefiles", layer = "TOWNSSURVEY_POLYM")

# download county shapefile
download.file(url = "http://download.massgis.digital.mass.gov/shapefiles/state/counties.zip",
              destfile = "shapefiles/counties.zip")

# read in counties polygons
counties <- st_read(dsn = "shapefiles/counties", layer = "COUNTIES_POLYM")

# read in MAPC parcel layer from geodatabase from https://datacommon.mapc.org/ 
parcels.mapc <- st_read(dsn = "shapefiles/MassachusettsLandParcelDatabase.gdb") %>% 
  dplyr::select(parloc_id) %>% 
  st_set_precision(1000000) %>% # set high precision and call st_make_valid() to avoid topology errors
  lwgeom::st_make_valid() # fix geometric errors

# Convert parcels.mapc to MA state plane
parcels.mapc <- st_transform(parcels.mapc,crs = st_crs(towns))

# Set up version of parcels layer so that we can subset by county.
# spatially join counties to towns
townsj <- st_join(towns, counties, largest = TRUE) %>% 
  dplyr::select(TOWN, COUNTY)

# change muni names to upper case to make join with townsj
parcels.mapc$muni <- str_to_upper(parcels.mapc$muni)

# join townsj to parcels.mapc so that we can assign counties to every parcel
parcels.mapc <- townsj %>% 
  st_set_geometry(NULL) %>% 
  left_join(parcels.mapc, ., by = c("muni" = "TOWN"))


# Landsat images are Landsat 8 OLI/TIRS for August 2018 Tier 1 surface reflectance.
# Downloaded from https://earthexplorer.usgs.gov/
# Create a list of directories with landsat imagery
dirs <- list.dirs("shapefiles/NDVI")
dirs <- dirs[ grepl("LC", dirs) ]


# Iterate through directories to create ndvi images in MA state plane NAD83 and write to disk
for (dir in dirs) {
  # Create list of band tif files, excluding non-band files
  all_landsat_bands <- list.files(dir, pattern = glob2rx("*band*.tif$"), full.names = TRUE)
  # stack the data
  landsat_stack <- stack(all_landsat_bands)
  # then turn it into a brick
  landsat_br <- brick(landsat_stack)
  # calculate ndvi from red (band 4) and near-infrared (band 5) channel
  ndvi <- overlay(landsat_br[[4]], landsat_br[[5]], fun = function(x, y) {
    (y-x) / (y+x)
  })
  # reproject image to MA state plane NAD83. Note that st_crs() creates a two-item list but projectRaster() needs a character vector, so we extract second item in list.
  ndvi_NAD83 <- projectRaster(ndvi, crs = st_crs(counties)[[2]])
  # write output raster to disk
  writeRaster(ndvi_NAD83, filename = paste0(dir,"/ndvi_NAD83.tif"), format="GTiff", overwrite=TRUE)
}

# Create list of projected ndvi images and read in as rasters
ndviList <- lapply(dirs, function(x) paste0(x,"/ndvi_NAD83.tif"))
ndviList <- lapply(ndviList, raster)

# merge a subset of rasters that cover the state. set tolerance to 100m to deal with error of different origins. 
# These selected ndvi rasters were inspected in ArcGIS 10.6.1 for extent coverage. Final list item selection based on consistency of NDVI values across rasters. 
ndviMerge <- raster::merge(ndviList[[5]],ndviList[[10]],ndviList[[12]],ndviList[[14]],ndviList[[16]], tolerance = 100)

# crop merged raster to extent of state + 50m to allow for buffers. 
ndviMA <- counties %>% 
  st_buffer(., 50) %>% 
  crop(ndviMerge, .)

# reclassify cell values < -1 and > 1 to NA
ndviMA <- reclassify(ndviMA, c(-Inf, -1, NA,  1, Inf, NA))

# check to see how many NA values we have
sum(is.na(values(ndviMA)))

# use moving window and focal statististic to estimate NDVI values of NA cells
ndviMA_mf <- focal(ndviMA, w=matrix(1,nrow=5,ncol=5), fun=mean, NAonly = TRUE, na.rm=TRUE)
# repeat focal window
ndviMA_mf <- focal(ndviMA_mf, w=matrix(1,nrow=5,ncol=5), fun=mean, NAonly = TRUE, na.rm=TRUE)

# check to see how many NA values we have now
sum(is.na(values(ndviMA_mf)))

# save raster with reduced number of NA values; na.rm=TRUE should clean up remainder
writeRaster(ndviMA_mf, filename = "shapefiles/NDVI/ndviMA.tif", format="GTiff", overwrite=TRUE)

# Force raster into memory (rather than reading from disk) so that we can save to Rds format
NDVIall <- readAll(NDVI)
save(NDVIall, file = "shapefiles/NDVI/ndviMA.Rds")


# SET UP FUNCTION TO CALCULATE AVERAGE NDVI WITHIN BUFFER
dist <- c(25,50)
# Function to iterate through parcels, create buffer, and extract avg NDVI value
BuffNDVI <- function(NDVI, parcels, dist){
  # create an empty vector list to hold output
  bfList <- vector("list", length(dist))
  # iterate through each buffer distance from bfList and execute raster::extract
  for (i in seq_along(dist)) {
    bfList[[i]] <- parcels %>% 
      st_buffer(., dist[[i]]) %>%
      raster::extract(NDVI, ., fun=mean, sp=TRUE, na.rm=TRUE) %>% 
      as.data.frame() %>% 
      mutate(avgNDVI = ndviMA, buffer_m = dist[[i]]) %>% 
      dplyr::select(parloc_id, avgNDVI, buffer_m)
  }
  # unlist tables, rbind into one table, and make wide
  finalTable <- do.call("rbind", bfList) %>% 
    spread(key = buffer_m, # set column 3 name as key name
           value = avgNDVI,
           sep = "AvgNDVI")
  return(finalTable)
}

# Function to select set of parcels one town at a time from a given county, run through foreach() and write output of town parcels to CSV
# The purpose of this function is reduce memory load AND to keep tabs on process by monitoring periodic output
NDVIbyTown <- function(county,parcels,NDVI,dist, BuffNDVI) {
  # Select parcels within the county
  parcels.county <- parcels %>% 
    filter(COUNTY == county)
  # create an empty vector list to hold output
  bfList <- vector("list", length(dist))
  # create a vector of unique town names in county
  county.towns <- unique(parcels.county$muni)
  for (i in seq_along(county.towns)){
    #town.name <- i
    parcels.town <- filter(parcels.county, muni == county.towns[[i]])
    ndvi.town <- crop(NDVI, parcels.town)
    NDVIbuff <- foreach(x = isplitRows(parcels.town, chunks = numCores), .combine='bind_rows') %dopar% BuffNDVI(ndvi.town, x, dist)
    write.csv(NDVIbuff, file = paste0("shapefiles/NDVI/OUTPUT/NDVI_",county.towns[[i]],".csv"), row.names=FALSE)
  }
}


# SET UP PARALLEL PROCESSING
# check for number of cores
detectCores()

# set number of cores to use
numCores <- detectCores() - 1

# Create a Parallel Socket Cluster. Creates a set of copies of R running in parallel and communicating over sockets.
cl <- makeCluster(numCores)

# register the parallel backend with the foreach package
registerDoParallel(cl)

# confirm number of workers
getDoParWorkers()

# get name of current backend
getDoParName()

# use ClusterEvalQ to load needed packages in each cluster
clusterEvalQ(cl, {
  library(tidyverse)
  library(sf)
  library(plyr)
  library(dplyr)
  library(raster)
})


# RUN CODE
# Working county by county to keep tabs on process
NDVIbyTown("NANTUCKET",parcels.mapc,NDVI,dist,BuffNDVI)

NDVIbyTown("DUKES",parcels.mapc,NDVI,dist,BuffNDVI)

NDVIbyTown("FRANKLIN",parcels.mapc,NDVI,dist,BuffNDVI)

NDVIbyTown("HAMPSHIRE",parcels.mapc,NDVI,dist,BuffNDVI)

NDVIbyTown("BERKSHIRE",parcels.mapc,NDVI,dist,BuffNDVI)

# For Suffolk County, eliminate duplicate parcels or else it errors out
# create a subset of Suffolk parcels
parcels.suffolk <- parcels.mapc %>% 
  filter(COUNTY == "SUFFOLK") %>% 
  distinct(parloc_id, .keep_all = TRUE) # keep only unique parcel IDS
# run function wtih parcel subset
NDVIbyTown("SUFFOLK",parcels.suffolk,NDVIall,dist,BuffNDVI)

NDVIbyTown("HAMPDEN",parcels.mapc,NDVIall,dist,BuffNDVI)

NDVIbyTown("BARNSTABLE",parcels.mapc,NDVIall,dist,BuffNDVI)

NDVIbyTown("BRISTOL",parcels.mapc,NDVIall,dist,BuffNDVI)

NDVIbyTown("PLYMOUTH",parcels.mapc,NDVIall,dist,BuffNDVI)

NDVIbyTown("NORFOLK",parcels.mapc,NDVIall,dist,BuffNDVI)

NDVIbyTown("ESSEX",parcels.mapc,NDVIall,dist,BuffNDVI)

NDVIbyTown("WORCESTER",parcels.mapc,NDVIall,dist,BuffNDVI)

NDVIbyTown("MIDDLESEX",parcels.mapc,NDVIall,dist,BuffNDVI)

# Stop clusters when done
stopCluster(cl)