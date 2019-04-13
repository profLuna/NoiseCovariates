############# Nearest Road Condition Analysis ####################
# Isolate road conditions from road nearest to each parcel: surface width, terrain, traffic counts, and heavy vehicle counts. NOTE: NO HEAVY VEHICL COUNTS FOR STATE

# load necessary packages
library(plyr)
library(tidyverse)
library(sf)
library(tmap)
library(maptools)
library(lwgeom)
library(nngeo) # for nearest neighbor analysis
library(parallel)
library(foreach)
library(doParallel)
library(itertools)


# READ IN AND PREP DATA
# download in roads shapefile
download.file(url = "http://download.massgis.digital.mass.gov/shapefiles/state/MassDOT_Roads_SHP.zip",
              destfile = "shapefiles/MassDOT_Roads_SHP.zip")

# unzip the downloaded shapefile into the shapefiles directory
unzip(zipfile = "shapefiles/MassDOT_Roads_SHP.zip", exdir = "./shapefiles")

# read in the roads shapefile
roadAll <- st_read(dsn = "shapefiles", layer = "EOTROADS_ARC") %>% 
  select(CLASS, SURFACE_WD, TERRAIN, AADT) %>% 
  st_set_precision(1000000) %>% 
  lwgeom::st_make_valid()

# read in MAPC parcel layer from geodatabase from https://datacommon.mapc.org/ 
parcels.mapc <- st_read(dsn = "shapefiles/MassachusettsLandParcelDatabase.gdb") %>% 
  dplyr::select(parloc_id) %>% 
  st_set_precision(1000000) %>% # set high precision and call st_make_valid() to avoid topology errors
  lwgeom::st_make_valid() # fix geometric errors

# Convert parcels.mapc to MA state plane
parcels.mapc <- st_transform(parcels.mapc,crs = st_crs(roadAll))


# SET UP FUNCTION TO EXTRACT CONDITIONS OF NEAREST ROAD
# Function: for each parcel, find the road conditions of the nearest road
roadCond <- function(parcels, roadAll){
  nearestRoadCond <- st_nn(parcels,roadAll) %>% 
    unlist() %>% # decompose list output from st_nn() to vector
    roadAll[.,] %>% # use vector indices to select records from roadAll
    select(SURFACE_WD, TERRAIN, AADT) %>% # isolate relevant variables
    st_set_geometry(., NULL) # remove geometry
  # extract just parcel ids to cbind
  nearestRoadCond <- parcels %>% 
    dplyr::select(parloc_id) %>% 
    st_set_geometry(., NULL) %>% 
    cbind(.,nearestRoadCond) # cbind parcels back to count column
  return(nearestRoadCond)
}

# Set up parallel processing
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
  library(nngeo)
})


# RUN CODE
nearRoadcond <- foreach(x = isplitRows(parcels.mapc, chunks = numCores), .combine='bind_rows') %dopar% roadCond(x,roadAll)

# Stop clusters when done
stopCluster(cl)

# write results out to a csv
write_csv(nearRoadcond, file = "OUTPUT/nearRoadcond.csv")