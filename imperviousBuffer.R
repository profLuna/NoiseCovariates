############# Percent Impervious within Buffer Analysis ####################

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

# read in csv with URLs for impervious surface tiles from https://docs.digital.mass.gov/dataset/massgis-data-impervious-surface-2005 
impSurflinks <- read_csv("shapefiles/GISDATA.DOWNLOADLINKS_IMP.csv")

# extract links as vector of characters from aws_link column
urls <- pull(impSurflinks,aws_link)

# iterate through vector to download all 33 files
for (url in urls) {
  download.file(url, destfile = paste0("shapefiles/impervious/",basename(url)))
}

# create vector of all of the zipped files
files <- list.files(path = "shapefiles/impervious", pattern = "\\.zip$")

# iterate through vector to unzip all 33 files
for (file in files) {
  unzip(zipfile = paste0("shapefiles/impervious/",file), exdir = "./shapefiles/impervious")
}

# create list of img files and read in to raster
rFiles <- list.files(path = "shapefiles/impervious", pattern = "\\.img$", full.names = TRUE) %>% 
  lapply(.,raster)

# merge rasters
imperviousMerge <- do.call(merge,rFiles)

# Arrggh. Impervious tiles merged in ArcMap 10.6.1 because took too long in R!
impervious <- raster("shapefiles/impervious/imperviousMerge.tif")

# read in MAPC parcel layer from geodatabase from https://datacommon.mapc.org/ 
parcels.mapc <- st_read(dsn = "shapefiles/MassachusettsLandParcelDatabase.gdb") %>% 
  dplyr::select(parloc_id) %>% 
  st_set_precision(1000000) %>% # set high precision and call st_make_valid() to avoid topology errors
  lwgeom::st_make_valid() # fix geometric errors

# Convert parcels.mapc to MA state plane
parcels.mapc <- st_transform(parcels.mapc,crs = st_crs(busStop))

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

# save these versions of impervious and parcels to use later
save(impervious,parcels.mapc,file="impLayers.Rds")


# SET UP FUNCTION TO CALCULATE PERCENT IMPERVIOUS WITHIN BUFFER
# function to extract impervious raster values within buffers around parcels and calculate percentage imp
BuffImp <- function(impervious, parcels){
  buffImpProp <- parcels %>% 
    st_buffer(., 50) %>%
    raster::extract(impervious, ., fun=mean, sp=TRUE) %>% 
    as.data.frame() %>% 
    mutate(pctImp50mBuff = imperviousMerge*100) %>% 
    dplyr::select(parloc_id, pctImp50mBuff)
  return(buffImpProp)
}

# Function to select set of parcels one town at a time from a given county, run through foreach() and write output of town parcels to CSV
# The purpose of this function is reduce memory load AND to keep tabs on process by monitoring periodic output
ImpbyTown <- function(county,parcels,impervious,BuffImp) {
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
    imp.town <- crop(impervious, parcels.town)
    Impbuff <- foreach(x = isplitRows(parcels.town, chunks = numCores), .combine='bind_rows') %dopar% BuffImp(imp.town, x)
    write.csv(Impbuff, file = paste0("shapefiles/impervious/OUTPUT/IMPERV_",county.towns[[i]],".csv"), row.names=FALSE)
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
ImpbyTown("NANTUCKET",parcels.mapc,impervious,BuffImp)

ImpbyTown("DUKES",parcels.mapc,impervious,BuffImp)

# For Franklin County, remove New Salem because it errors out
# identify towns in Franklin excluding New Salem
parcels.franklin <- parcels.mapc %>% 
  filter(COUNTY == "FRANKLIN" & muni != "NEW SALEM")
ImpbyTown("FRANKLIN",parcels.franklin,impervious,BuffImp)

# Fix problem with New Salem separately
# Isolate New Salem parcels
parcels.newsalem <- parcels.mapc %>% 
  filter(muni == "NEW SALEM")
# Create buffer around New Salem parcels
buffer.newsalem <- st_buffer(parcels.newsalem,50)
# Crop impervious raster to New Salem parcels
impervious.newsalem <- crop(impervious,parcels.newsalem)
# Aggregate raster to lower resolution
impervious.newsalem2 <- aggregate(impervious.newsalem,fact = 10, fun = mean)
# Run BuffImp on New Salem parcels using aggregated impervious raster
newsalem_imp <- foreach(x = isplitRows(parcels.newsalem, chunks = numCores), .combine='bind_rows') %dopar% BuffImp(impervious.newsalem2, x)
# write it out
write_csv(newsalem_imp, file = "shapefiles/impervious/OUTPUT/IMPERV_NEW SALEM.csv")

ImpbyTown("HAMPDEN",parcels.mapc,impervious,BuffImp)

ImpbyTown("BRISTOL",parcels.mapc,impervious,BuffImp)

ImpbyTown("HAMPSHIRE",parcels.mapc,impervious,BuffImp)

ImpbyTown("BERKSHIRE",parcels.mapc,impervious,BuffImp)

ImpbyTown("SUFFOLK",parcels.mapc,impervious,BuffImp)

ImpbyTown("BARNSTABLE",parcels.mapc,impervious,BuffImp)

ImpbyTown("PLYMOUTH",parcels.mapc,impervious,BuffImp)

ImpbyTown("NORFOLK",parcels.mapc,impervious,BuffImp)

ImpbyTown("ESSEX",parcels.mapc,impervious,BuffImp)

ImpbyTown("WORCESTER",parcels.mapc,impervious,BuffImp)

ImpbyTown("MIDDLESEX",parcels.mapc,impervious,BuffImp)

# Stop clusters when done
stopCluster(cl)