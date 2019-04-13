############# Length within Buffer Analysis ####################

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
# download and read in bus route shapefile
download.file(url = "http://download.massgis.digital.mass.gov/shapefiles/state/mbtabus.zip",
              destfile = "shapefiles/mbtabus.zip")

# download and read in roads shapefile
download.file(url = "http://download.massgis.digital.mass.gov/shapefiles/state/MassDOT_Roads_SHP.zip",
              destfile = "shapefiles/MassDOT_Roads_SHP.zip")

# download and read in trains (including commuter rail) shapefile
download.file(url = "http://download.massgis.digital.mass.gov/shapefiles/state/trains.zip",
              destfile = "shapefiles/trains.zip")

# unzip the downloaded shapefile into the shapefiles directory
unzip(zipfile = "shapefiles/mbtabus.zip", exdir = "./shapefiles")

# unzip the downloaded shapefile into the shapefiles directory
unzip(zipfile = "shapefiles/MassDOT_Roads_SHP.zip", exdir = "./shapefiles")

# unzip the downloaded shapefile into the shapefiles directory
unzip(zipfile = "shapefiles/trains.zip", exdir = "./shapefiles")

# read in the bus route shapefile
busRoute <- st_read(dsn = "shapefiles/mbtabus", layer = "MBTABUSROUTES_ARC") %>% 
  st_zm() %>%  # GEOS does not support XYM or XYZM geometries; use st_zm() to drop M
  select(MBTA_ROUTE) %>% 
  st_set_precision(1000000) %>% 
  lwgeom::st_make_valid()

# read in the roads shapefile
roadAll <- st_read(dsn = "shapefiles", layer = "EOTROADS_ARC") %>% 
  select(CLASS, SURFACE_WD, TERRAIN, AADT) %>% 
  st_set_precision(1000000) %>% 
  lwgeom::st_make_valid()

# separate out Class 3 roads
roadC3 <- roadAll %>% 
  select(CLASS) %>% 
  filter(CLASS == 3)

# separate out Class 5 roads
roadC5 <- roadAll %>% 
  select(CLASS) %>%
  filter(CLASS == 5)

# read in major roads
roadMjr <- st_read(dsn = "shapefiles", layer = "EOTMAJROADS_ARC") %>% 
  select(CLASS) %>% 
  st_set_precision(1000000) %>% 
  lwgeom::st_make_valid()

# read in the trains shapefile
train <- st_read(dsn = "shapefiles", layer = "TRAINS_ARC") %>% 
  select(TYPE, RT_CLASS, COMMRAIL) %>% 
  filter(TYPE %in% c(1,9) & RT_CLASS != 7 & !(COMMRAIL %in% c("P","C"))) %>% # retain only active rail and MBTA
  st_set_precision(1000000) %>% 
  lwgeom::st_make_valid()

# read in MAPC parcel layer from geodatabase from https://datacommon.mapc.org/ 
parcels.mapc <- st_read(dsn = "shapefiles/MassachusettsLandParcelDatabase.gdb") %>% 
  dplyr::select(parloc_id) %>% 
  st_set_precision(1000000) %>% # set high precision and call st_make_valid() to avoid topology errors
  lwgeom::st_make_valid() # fix geometric errors

# Convert parcels.mapc to MA state plane
parcels.mapc <- st_transform(parcels.mapc,crs = st_crs(busRoute))


# SET UP FUNCTIONS TO EXTRACT CUMULATIVE LENGTHS WITHIN BUFFERS
# Function to intersect buffers with lines, calculate cumulative length, and ouput wide table. dist is a vector of buffer distances. lineFile is a sf line layer. 
BuffLength <- function(parcels, dist, lineFile) {
  # create an empty vector list to hold output
  bfList <- vector("list", length(dist))
  # iterate through each buffer distance from bfList and create buffer around each parcel,intersect each buffer with lines, calculate length of lines within each buffer, convert to wide table, and output resulting table to list item
  for (i in seq_along(dist)) {
    # result of each iteration is stored in bfList
    bfList[[i]] <- st_buffer(parcels,dist[[i]]) %>% # create buffer
      mutate(buffer_m = dist[[i]]) %>% # add buffer distance label. must precede st_intersection in parallel version.
      # st_set_precision(1e12) %>% 
      st_intersection(.,lineFile) %>%  # intersect buffer with lines 
      # calculate length of lines within buffer
      mutate(length_m = st_length(.)) %>% 
      # group by parcel to sum line lengths for each parcel
      group_by(parloc_id) %>%
      dplyr::summarize(length_m = sum(length_m),
                       buffer_m = unique(buffer_m)) %>%
      st_set_geometry(., NULL)  # remove geometry to allow join
  }
  # unlist tables and rbind into one table
  finalTable <- do.call("rbind", bfList)
  if(nrow(finalTable) >= 1){ # check for instances of no line intersections with buffers
    # replace buffer_m with relevant variable name
    name <- deparse(substitute(lineFile)) # extract object name
    names(finalTable)[3] <- name # assign object name as column name
    # convert to wide format with one column per buff distance
    finalTable <- finalTable %>%
      spread(key = names(finalTable[3]), # set column 3 name as key name
             value = length_m,
             sep = "Len_Buf")
    return(finalTable)
  } else {} # return nothing if no line intersections
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
# Calculate lengths of bus routes
dist <- c(50,250,300,500)
# busLengths <- BuffLength(parcels.mapc, dist,busRoute)
busLengths <- foreach(x = isplitRows(parcels.mapc, chunks = numCores), .combine='bind_rows') %dopar% BuffLength(x,dist,busRoute)

# Calculate lengths of rail lines
dist <- c(500,1000)
# railLengths <- BuffLength(dist,train)
railLengths <- foreach(x = isplitRows(parcels.mapc, chunks = numCores), .combine='bind_rows') %dopar% BuffLength(x,dist,train)

# Calculate lengths of all roads
dist <- c(25,50)
# roadLengths <- BuffLength(dist,roadAll)
roadLengths <- foreach(x = isplitRows(parcels.mapc, chunks = numCores), .combine='bind_rows') %dopar% BuffLength(x,dist,roadAll)

# Calculate lengths of Class 3 roads
dist <- 25
# roadC3Lengths <- BuffLength(dist,roadC3)
roadC3Lengths <- foreach(x = isplitRows(parcels.mapc, chunks = numCores), .combine='bind_rows') %dopar% BuffLength(x,dist,roadC3)

# Calculate lengths of Class 5 roads
dist <- 1000
# roadC5Lengths <- BuffLength(dist,roadC5)
roadC5Lengths <- foreach(x = isplitRows(parcels.mapc, chunks = numCores), .combine='bind_rows') %dopar% BuffLength(x,dist,roadC5)

# Calculate lengths of major roads
dist <- 50
# roadC5Lengths <- BuffLength(dist,roadC5)
roadMjrLengths <- foreach(x = isplitRows(parcels.mapc, chunks = numCores), .combine='bind_rows') %dopar% BuffLength(x,dist,roadMjr)

# Stop clusters when done
stopCluster(cl)

# Join length tables together
lengthTable <- full_join(busLengths, roadLengths, by = "parloc_id") %>% 
  full_join(., roadC3Lengths, by = "parloc_id") %>% 
  full_join(., roadC5Lengths, by = "parloc_id") %>% 
  full_join(., railLengths, by = "parloc_id") %>% 
  full_join(., roadMjrLengths, by = "parloc_id")

# save out to csv
write_csv(lengthTable, "OUTPUT/lengthTable.csv")