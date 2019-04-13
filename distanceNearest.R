############# Distance to Nearest Analysis ####################

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

# download trains (including commuter rail) shapefile
download.file(url = "http://download.massgis.digital.mass.gov/shapefiles/state/trains.zip",
              destfile = "shapefiles/trains.zip")

# download fire station Shapefile
download.file(url = "http://download.massgis.digital.mass.gov/shapefiles/state/firestations_pt.zip",
              destfile = "shapefiles/firestations_pt.zip")

# download police station Shapefile
download.file(url = "http://download.massgis.digital.mass.gov/shapefiles/state/policestations.zip",
              destfile = "shapefiles/policestations.zip")

# download open space Shapefile
download.file(url = "http://download.massgis.digital.mass.gov/shapefiles/state/openspace.zip",
              destfile = "shapefiles/openspace.zip")

# unzip the downloaded shapefile into the shapefiles directory
unzip(zipfile = "shapefiles/MassDOT_Roads_SHP.zip", exdir = "./shapefiles")

# unzip the downloaded shapefile into the shapefiles directory
unzip(zipfile = "shapefiles/trains.zip", exdir = "./shapefiles")

# unzip the downloaded shapefile into the shapefiles directory
unzip(zipfile = "shapefiles/firestations_pt.zip", exdir = "./shapefiles")

# unzip the downloaded shapefile into the shapefiles directory
unzip(zipfile = "shapefiles/policestations.zip", exdir = "./shapefiles")

# unzip the downloaded shapefile into the shapefiles directory
unzip(zipfile = "shapefiles/openspace.zip", exdir = "./shapefiles")

# read in the roads shapefile
roadAll <- st_read(dsn = "shapefiles", layer = "EOTROADS_ARC") %>% 
  select(CLASS, SURFACE_WD, TERRAIN, AADT) %>% 
  st_set_precision(1000000) %>% 
  lwgeom::st_make_valid()

# read in the trains shapefile
train <- st_read(dsn = "shapefiles", layer = "TRAINS_ARC") %>% 
  select(TYPE, RT_CLASS, COMMRAIL) %>% 
  filter(TYPE %in% c(1,9) & RT_CLASS != 7 & !(COMMRAIL %in% c("P","C"))) %>% # retain only active rail and MBTA
  st_set_precision(1000000) %>% 
  lwgeom::st_make_valid()

# read in the fire station shapefile
fire <- st_read(dsn = "shapefiles", layer = "FIRESTATIONS_PT_MEMA")

# read in police station shapefile
police <- st_read(dsn = "shapefiles", layer = "POLICESTATIONS_PT_MEMA")

# read in open space shapefile
ospace <- st_read(dsn = "shapefiles", layer = "OPENSPACE_POLY")

# read in MAPC parcel layer from geodatabase from https://datacommon.mapc.org/ 
parcels.mapc <- st_read(dsn = "shapefiles/MassachusettsLandParcelDatabase.gdb") %>% 
  dplyr::select(parloc_id) %>% 
  st_set_precision(1000000) %>% # set high precision and call st_make_valid() to avoid topology errors
  lwgeom::st_make_valid() # fix geometric errors

# Convert parcels.mapc to MA state plane
parcels.mapc <- st_transform(parcels.mapc,crs = st_crs(roadAll))


# SET UP FUNCTION TO CALCULATE DISTANCE TO NEAREST
# Function: for each parcel, find distance to nearest layer object
nDist <- function(parcels, layer){
  df <- st_nn(parcels,layer,returnDist = TRUE) %>% # identify nearest features index with distance
    unlist() %>% # decompose list output from st_nn()
    matrix(., ncol = 2, dimnames = list(NULL,c("index","count"))) %>% # convert vector to matrix
    as.data.frame() %>% # convert matrix to data frame
    select(count)  # isolate count column
  # extract just parcel ids to cbind
  df <- parcels %>% 
    dplyr::select(parloc_id) %>% 
    st_set_geometry(., NULL) %>% 
    cbind(.,df) # cbind parcels back to count column
  # replace count with relevant variable name
  name <- paste0(deparse(substitute(layer)),"Dist_m") # extract object name
  names(df)[2] <- name # assign object name as column name
  return(df)
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
  library(nngeo)
})


# RUN CODE
# Find distance to nearest road for each parcel
roadAllDist <- foreach(x = isplitRows(parcels.mapc, chunks = numCores), .combine='bind_rows') %dopar% nDist(x,roadAll)

# Find distance to nearest rail line for each parcel
railDist <- foreach(x = isplitRows(parcels.mapc, chunks = numCores), .combine='bind_rows') %dopar% nDist(x,train)

# Find distance to nearest open space for each parcel
ospaceDist <- foreach(x = isplitRows(parcels.mapc, chunks = numCores), .combine='bind_rows') %dopar% nDist(x,ospace)

# Find distance to nearest fire station for each parcel
fireDist <- foreach(x = isplitRows(parcels.mapc, chunks = numCores), .combine='bind_rows') %dopar% nDist(x,fire)

# Find distance to nearest police station for each parcel
policeDist <- foreach(x = isplitRows(parcels.mapc, chunks = numCores), .combine='bind_rows') %dopar% nDist(x,police)

# Stop clusters when done
stopCluster(cl)

# Join distance tables together
nDistTable <- full_join(roadAllDist, ospaceDist, by = "parloc_id") %>% 
  full_join(., fireDist, by = "parloc_id") %>% 
  full_join(., policeDist, by = "parloc_id") %>% 
  full_join(., railDist, by = "parloc_id")


# write results out to a csv
write_csv(nDistTable, file = "OUTPUT/nDistTable.csv")