############# Land Use Buffer Analysis ####################

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

# download and read in statewide land use shapefile
download.file(url = "http://download.massgis.digital.mass.gov/shapefiles/state/landuse2005_poly.zip",
              destfile = "shapefiles/landuse2005_poly.zip")

# unzip the downloaded shapefile into the shapefiles directory
unzip(zipfile = "shapefiles/landuse2005_poly.zip", exdir = "./shapefiles")

# read in the statewide landuse shapefile
landuse.ma <- st_read(dsn = "shapefiles", layer = "LANDUSE2005_POLY") %>% 
  st_set_precision(1000000) %>% # set high precision and call st_make_valid() to avoid topology errors
  lwgeom::st_make_valid()

# unzip MAPC parcel layer geodatabase from https://datacommon.mapc.org/
unzip(zipfile = "shapefiles/MassachusettsLandParcelDatabase.gdb.zip", exdir = "./shapefiles")

# read in MAPC parcel layer from geodatabase from https://datacommon.mapc.org/ 
parcels.mapc <- st_read(dsn = "shapefiles/MassachusettsLandParcelDatabase.gdb") %>% 
  dplyr::select(muni, parloc_id, realesttyp) %>% 
  st_set_precision(1000000) %>% # set high precision and call st_make_valid() to avoid topology errors
  lwgeom::st_make_valid() # fix geometric errors

# Convert parcels.mapc to MA state plane
parcels.mapc <- st_transform(parcels.mapc,crs = st_crs(landuse.ma))

# Recode landuse.ma descriptions to match landuse.mapc; limit to significant variables
landuse.ma <- landuse.ma %>% 
  mutate(landuse = case_when(
    LUCODE %in% c(3,37) ~ "Forest",
    LUCODE == 6 ~ "Open Land",
    LUCODE %in% c(7:9) ~ "Recreation",
    LUCODE %in% c(10:13,38) ~ "Residential",
    LUCODE == 15 ~ "Commercial",
    LUCODE == 16 ~ "Industrial",
    LUCODE == 18 ~ "Transportation",
    LUCODE %in% c(1:2,4:5,14,17,19:36,39:40) ~ "Other"
  ))

# Createa composite landuse layer using landuse.ma and landuse.mapc
# extract areas of landuse.ma that DO NOT intersect/overlap landuse.mapc and then merge the result with landuse.mapc to create a composite landuse layer
# define function to “erase” one geometry from another. The st_erase() function defined below is not exported by the sf package, but is defined in the documentation for st_difference().
st_erase <- function(x, y) {
  st_difference(x, st_union(st_combine(y)))
}
# extract geometric difference (i.e. landuse.ma that does not overlap with landuse.mapc). 
# WARNING. THIS TAKES FOREVER IN R.
landuse.diff <- st_erase(landuse.ma,landuse.mapc)

# merge/append the two layers to create a composite landuse layer
# first, ensure that the two layers have identical variables
landuse.mapc <- landuse.mapc %>% 
  transmute(landuse = landuse)
landuse.diff <- landuse.diff %>% 
  transmute(landuse = landuse)

# Reset geometry of landuse.mapc so that it has 'geometry' column rather than 'Shape' column; allow rbind
tmp_sfc <- st_geometry(landuse.mapc) # extract geometry

tmp_df <- st_set_geometry(landuse.mapc, NULL) # remove geometry, coerce to data.frame

landuse.mapc <- st_set_geometry(tmp_df,tmp_sfc) # set geometry, return sf

# finally, merge/append the two polygon layers
landuse.merge <- rbind(landuse.mapc,landuse.diff)

# READ IN LANDUSE.MERGE FROM ARC BECAUSE IT TAKES TOO DAMN LONG IN R!
# landuse_merge_dissolved created in ArcMap 10.6.1 by repairing geometry, differencing, merging, and dissolving
landuse.merge <- st_read(dsn = "ArcVersion/shapefiles", layer = "landuse_merge_dissolved") %>% 
  st_set_precision(1000000) %>% # set high precision and call st_make_valid() to avoid topology errors
  lwgeom::st_make_valid() # fix geometric errors


# SET UP FUNCTIONS TO EXTRACT LAND USE PERCENTAGES AROUND EACH PARCEL
# Iterate across all parcels at each buffer distance, create buffers, intersect buffers with landuse, calculate percentage of landuse within each buffer, output to wide format table with one record per parcel and one column per bufferXlanduse
# set buffer distances
dist <- c(25,50,100,150,200,250,300,500,1000)

# Function to iterate through each buffer distance from bfList and create buffer around each parcel,intersect each buffer with landuse, calculate pct area of landuse within each buffer, convert to wide table, and output resulting table to list item
pBuff <- function(parcels, dist, landuse){
  # create an empty vector list to hold output
  bfList <- vector("list", length(dist))
  # iterate through each buffer distance from bfList and create buffer around each parcel,intersect each buffer with landuse, calculate pct area of landuse within each buffer, convert to wide table, and output resulting table to list item
  for (i in seq_along(dist)) {
    # result of each iteration is stored in bfList
    bfList[[i]] <- st_buffer(parcels,dist[[i]]) %>% # create buffer
      st_intersection(.,landuse) %>% # intersect buffer with landuse
      # add buffer_m label and calculate areas of landuse polys within buffer
      mutate(buffer_m = dist[[i]], aream2 = st_area(.)) %>% 
      # use ddply to split records by parcel, aggregate and calculate total area of unique landuse types for each parcel, and recombine into df; retain buffer_m label
      ddply(.,c("parloc_id","landuse"), summarize, 
            sum_area = sum(aream2), 
            buffer_m = unique(buffer_m)) %>% 
      # group by parcel to calculate percentage of LU for each parcel
      group_by(parloc_id) %>% 
      transmute(landuse = landuse, 
                pct_area = sum_area/sum(sum_area)*100, 
                buffer_m = unique(buffer_m)) %>% 
      # convert to wide format with one column per LUxbuffer distance
      spread(key = landuse, 
             value = pct_area, 
             sep = paste0("_",unique(.$buffer_m),"mBuff_"), fill = 0) %>% 
      dplyr::select(-starts_with("buffer_m")) # remove buffer_m* columns
    # check to see if nrow of bfList[[i]] = parcels; if not, add rows for missing parcels
  }
  # unlist tables with reduce and join into one table; full_join allows for different number of records in joined tables
  LandUseTable <- bfList %>% 
    purrr::reduce(full_join, by = "parloc_id") %>%
    dplyr::select(-num_range(prefix = "parloc_id", range = 1:10)) # remove duplicate parloc_id columns
  rm(bfList) # free up memory from this large object
  return(LandUseTable)
}

# Set up function to execute buffer function one town at a time and output to csv; allows monitoring of incremental progress and ensures some work is saved if it code breaks or crashes
LUbyTown <- function(county,parcels,landuse,dist, pBuff) {
  # Select parcels within the county
  parcels.county <- parcels %>% 
    filter(COUNTY == county)
  # create an empty vector list to hold output
  bfList <- vector("list", length(dist))
  # create a vector of unique town names in county
  county.towns <- unique(parcels.county$muni)
  for (i in seq_along(county.towns)){
    parcels.town <- filter(parcels.county, muni == county.towns[[i]])
    buffer.town <- parcels.town %>% 
      st_union() %>% 
      st_buffer(.,1000)
    landuse.town <- landuse %>% 
      st_crop(., st_bbox(buffer.town))
    LUbuff <- foreach(x = isplitRows(parcels.town, chunks = numCores), .combine='bind_rows') %dopar% pBuff(x, dist, landuse.town)
    write.csv(LUbuff, file = paste0("shapefiles/Landuse/OUTPUT/LU_",county.towns[[i]],".csv"), row.names=FALSE)
  }
}


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

# save these versions of landuse and parcels to use later
save(landuse.merge,parcels.mapc,file="lulayers.Rds")


# SET UP PARALLEL PROCESSING
# check for number of cores
detectCores()

# set number of cores to use. save one core to keep system running. 
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
})


# RUN CODE
# Working county by county to keep tabs on process and not overwhelm memory. 
# Note that with 6-core parallel 16GB RAM at 3.1GHz is 1hr/town. 
# On AWS EC2 x1.16xlarge 64-core parallel 976GB RAM at 2.3GHz is 7 - 9 min/town!
LUbyTown("NANTUCKET",parcels.mapc,landuse.merge,dist,pBuff)

LUbyTown("DUKES",parcels.mapc,landuse.merge,dist,pBuff)

LUbyTown("FRANKLIN",parcels.mapc,landuse.merge,dist,pBuff)

LUbyTown("HAMPDEN",parcels.mapc,landuse.merge,dist,pBuff)

LUbyTown("BRISTOL",parcels.mapc,landuse.merge,dist,pBuff)

LUbyTown("HAMPSHIRE",parcels.mapc,landuse.merge,dist,pBuff)

LUbyTown("BERKSHIRE",parcels.mapc,landuse.merge,dist,pBuff)

LUbyTown("NORFOLK",parcels.mapc,landuse.merge,dist,pBuff)

LUbyTown("ESSEX",parcels.mapc,landuse.merge,dist,pBuff)

LUbyTown("WORCESTER",parcels.mapc,landuse.merge,dist,pBuff)

LUbyTown("MIDDLESEX",parcels.mapc,landuse.merge,dist,pBuff)

LUbyTown("SUFFOLK",parcels.suffolk,landuse.merge2,dist,pBuff)

LUbyTown("BARNSTABLE",parcels.barnstable,landuse.merge2,dist,pBuff)

LUbyTown("PLYMOUTH",parcels.mapc,landuse.merge,dist,pBuff)

# Stop clusters when done
stopCluster(cl)