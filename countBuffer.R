############# Count within Buffer Analysis ####################

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
# download and read in Building Structures Statewide Shapefile
download.file(url = "http://download.massgis.digital.mass.gov/shapefiles/structures/structures_poly.zip",
              destfile = "shapefiles/structures_poly.zip")

# unzip the downloaded shapefile into the shapefiles directory
unzip(zipfile = "shapefiles/structures_poly.zip", exdir = "./shapefiles")

# read in the buildings shapefile
buildings <- st_read(dsn = "shapefiles", layer = "STRUCTURES_POLY")

# read in bus stops shapefile
busStop <- st_read(dsn = "shapefiles/mbtabus", layer = "MBTABUSSTOPS_PT")

# read in big box stores, convert to point layer, and transform CRS
bboxStore <- read_excel("tables/1292_794456_Output/Order_794456.xlsx") %>% 
  transmute(Latitude = as.numeric(Latitude), Longitude = as.numeric(Longitude)) %>% 
  st_as_sf(., coords = c("Longitude", "Latitude"), crs=4326) %>% 
  st_transform(., crs = st_crs(busStop))

# read in entertainment establishments, convert to point layer, and transform CRS
entertain <- read_excel("tables/1292_794472_Output/Order_794472.xlsx") %>% 
  transmute(Latitude = as.numeric(Latitude), Longitude = as.numeric(Longitude)) %>% 
  st_as_sf(., coords = c("Longitude", "Latitude"), crs=4326) %>% 
  st_transform(., crs = st_crs(busStop))

# read in restaurants, convert to point layer, and transform CRS
restaurant <- read_excel("tables/1292_794481_Output/Order_794481.xlsx") %>% 
  transmute(Latitude = as.numeric(Latitude), Longitude = as.numeric(Longitude)) %>% 
  st_as_sf(., coords = c("Longitude", "Latitude"), crs=4326) %>% 
  st_transform(., crs = st_crs(busStop))

# read in MAPC parcel layer from geodatabase from https://datacommon.mapc.org/ 
parcels.mapc <- st_read(dsn = "shapefiles/MassachusettsLandParcelDatabase.gdb") %>% 
  dplyr::select(parloc_id) %>% 
  st_set_precision(1000000) %>% # set high precision and call st_make_valid() to avoid topology errors
  lwgeom::st_make_valid() # fix geometric errors

# Convert parcels.mapc to MA state plane
parcels.mapc <- st_transform(parcels.mapc,crs = st_crs(busStop))


# SET UP FUNCTIONS TO COUNT FEATURES WITHIN BUFFERS
# Function to intersect buffers with objects, calculate count, and ouput wide table. dist is a vector of buffer distances. layer is a sf layer. 
BuffCount <- function(parcels, dist, layer) {
  # create an empty vector list to hold output
  bfList <- vector("list", length(dist))
  # iterate through each buffer distance from bfList and create buffer around each parcel,intersect each buffer with objects layer, calculate count of objects within each buffer, convert to wide table, and output resulting table to list item
  for (i in seq_along(dist)) {
    # result of each iteration is stored in bfList
    bfList[[i]] <- st_buffer(parcels,dist[[i]]) %>% # create buffer
      # add buffer_m label
      mutate(buffer_m = dist[[i]]) %>% 
      st_intersection(.,layer) %>% # intersect buffer with layer objects
      # group by parcel to count objects for each parcel
      group_by(parloc_id) %>% 
      dplyr::summarize(count = n(), 
                       buffer_m = unique(buffer_m)) %>% 
      st_set_geometry(., NULL) # remove geometry to allow join
  }
  # unlist tables and rbind into one table
  finalTable <- do.call("rbind", bfList)
  if(nrow(finalTable) >= 1){ # check for instances of no object intersections with buffers
    # replace buffer_m with relevant variable name
    name <- deparse(substitute(layer)) # extract object name
    names(finalTable)[3] <- name # assign object name as column name
    # convert to wide format with one column per buff distance
    finalTable <- finalTable %>% 
      spread(key = names(finalTable[3]), # set column 3 name as key name
             value = count,
             sep = "_Buf")
    return(finalTable)
  } else {} # return nothing if no object intersections
}


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
# Calculate count of buildings
dist <- c(50,100,150)
buildingCounts <- foreach(x = isplitRows(parcels.mapc, chunks = numCores), .combine='bind_rows') %dopar% BuffCount(x,dist,buildings)

# Calculate count of bus stops
dist <- c(50,100,150)
# busStopCounts <- BuffCount(dist,busStop)
busStopCounts <- foreach(x = isplitRows(parcels.mapc, chunks = numCores), .combine='bind_rows') %dopar% BuffCount(x,dist,busStop)

# Calculate count of big box stores
dist <- 300
# bboxStoreCounts <- BuffCount(dist,bboxStore)
bboxStoreCounts <- foreach(x = isplitRows(parcels.mapc, chunks = numCores), .combine='bind_rows') %dopar% BuffCount(x,dist,bboxStore)

# Calculate count of entertainment establishments
dist <- 500
# entertainCounts <- BuffCount(dist,entertain)
entertainCounts <- foreach(x = isplitRows(parcels.mapc, chunks = numCores), .combine='bind_rows') %dopar% BuffCount(x,dist,entertain)

# Calculate count of restaurants
dist <- 100
restaurantCounts <- foreach(x = isplitRows(parcels.mapc, chunks = numCores), .combine='bind_rows') %dopar% BuffCount(x,dist,restaurant)

# Stop clusters when done
stopCluster(cl)

# Join count tables together
countTable <- full_join(buildingCounts, busStopCounts, by = "parloc_id") %>% 
  full_join(., bboxStoreCounts, by = "parloc_id") %>% 
  full_join(., entertainCounts, by = "parloc_id") %>% 
  full_join(., restaurantCounts, by = "parloc_id")

# write results out to a csv
write.csv(countTable, file = "OUTPUT/countTable.csv",row.names=FALSE)