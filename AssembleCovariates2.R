# Assemble covariates into a single file

# load necessary packages
library(plyr)
library(tidyverse)
library(sf)
library(tmap)
library(maptools)
library(lwgeom)

### READ IN AND PREP LANDUSE BUFFER PERCENTAGES ####
# Read in list of csv files
# create a list of the files with their full path names
all_lu_files_list <- list.files("shapefiles/Landuse/OUTPUT", pattern = glob2rx("*LU*.csv$"), full.names = TRUE)

# create collectors to allow correct import of data, otherwise read_csv misinterprets columns with NA values
dbl <- col_double()
chr <- col_character()

# generate list of 'd' to input as col_types
noquote(rep("dbl,",72))

# read in csv files from list of all files
all_lu_list <- lapply(all_lu_files_list, function(x) read_csv(x, progress = TRUE, col_types = list(chr, dbl, dbl, dbl, dbl, dbl, dbl, dbl, dbl, dbl, dbl, dbl, dbl, dbl, dbl, dbl, dbl, dbl, dbl, dbl, dbl, dbl, dbl, dbl, dbl, dbl, dbl, dbl, dbl, dbl, dbl, dbl, dbl, dbl, dbl, dbl, dbl, dbl, dbl, dbl, dbl, dbl, dbl, dbl, dbl, dbl, dbl, dbl, dbl, dbl, dbl, dbl, dbl, dbl, dbl, dbl, dbl, dbl, dbl, dbl, dbl, dbl, dbl, dbl, dbl, dbl, dbl, dbl, dbl, dbl, dbl, dbl, dbl)))

# rbind the files of the list into one data frame. rbind.fill handles missing columns. 
lu_buffs <- do.call(rbind.fill, all_lu_list)

# put them in desired order, retaining only the variables we want
FINAL_lu_buffs <- lu_buffs %>% 
  select(parloc_id, 
         landuse_25mBuff_Commercial, 
         landuse_100mBuff_Commercial, 
         landuse_150mBuff_Commercial, 
         landuse_200mBuff_Commercial, 
         landuse_500mBuff_Industrial, 
         landuse_50mBuff_Transportation, 
         landuse_100mBuff_Transportation, 
         landuse_1000mBuff_Transportation, 
         landuse_100mBuff_Residential, 
         landuse_150mBuff_Residential, 
         landuse_200mBuff_Residential, 
         landuse_250mBuff_Residential, 
         landuse_100mBuff_Recreation, 
         landuse_1000mBuff_Recreation, 
         `landuse_25mBuff_Open Land`, 
         `landuse_50mBuff_Open Land`, 
         `landuse_200mBuff_Open Land`, 
         `landuse_250mBuff_Open Land`, 
         `landuse_300mBuff_Open Land`, 
         landuse_100mBuff_Forest)

# NA counts for each column
sapply(FINAL_lu_buffs, function(x) sum(is.na(x)))

# replace NAs with 0s
FINAL_lu_buffs[is.na(FINAL_lu_buffs)] <- 0

# write it out to final output
write_csv(x = FINAL_lu_buffs, path = "OUTPUT/FINAL_lu_buffs.csv")
FINAL_lu_buffs <- read.csv("OUTPUT/FINAL_lu_buffs.csv")



#### READ IN LENGTH WITHIN BUFFER ###
# read in the data
length_busroutes <- read_csv(file = "OUTPUT/busLengths.csv")
length_rail <- read_csv(file = "OUTPUT/railLengths.csv")
length_allRoads <- read_csv(file = "OUTPUT/roadLengths.csv")
length_mjrRoads <- read_csv(file = "OUTPUT/mjrRdlengths.txt")
length_c3Roads <- read_csv(file = "OUTPUT/roadC3Lengths.csv")
length_c5Roads <- read_csv(file = "OUTPUT/roadC5Lengths.csv")

# clean up column names for consistency
names(length_mjrRoads)
length_mjrRoads <- length_mjrRoads %>% 
  select(parloc_id, mjrRoadLenBuf50 = Sum_Shape_Length)

# full join them together to create one object
FINAL_buff_lengths <- full_join(length_allRoads, length_c5Roads, by = "parloc_id") %>% 
  full_join(., length_c3Roads, by = "parloc_id") %>% 
  full_join(., length_mjrRoads, by = "parloc_id") %>% 
  full_join(., length_busroutes, by = "parloc_id") %>%
  full_join(., length_rail, by = "parloc_id")

# get rid of duplicate cases
FINAL_buff_lengths <- FINAL_buff_lengths %>% distinct(parloc_id, .keep_all = TRUE)

# save it
write_csv(x = FINAL_buff_lengths, path = "OUTPUT/FINAL_buff_lengths.csv")



#### READ IN COUNTS WITHIN BUFFER ###
# read in the data
# count_busstops <- read_csv(file = "OUTPUT/busStopCounts.csv")
FINAL_buff_counts <- read_csv(file = "OUTPUT/countTable.csv", col_types = list(chr,dbl,dbl,dbl,dbl,dbl,dbl,dbl,dbl,dbl))
# NA counts for each column
sapply(FINAL_buff_counts, function(x) sum(is.na(x)))
# replace NAs with 0s
FINAL_buff_counts[is.na(FINAL_buff_counts)] <- 0


#### READ IN DISTANCE TO NEAREST ###
# read in data
ndists <- read_csv("OUTPUT/nDists.txt")

# remove objectid column and rename other columns for consistency
ndists <- ndists %>% 
  select(parloc_id, fireDist_m = FIRE_DIST, policeDist_m = POLICE_DIST, oSpaceDist_m = OS_DIST, railDist_m = RAIL_DIST)

# create a list of the files with their full path names
all_dist_files_list <- list.files("OUTPUT", pattern = glob2rx("*AllDist*.csv$"), full.names = TRUE)

# read in all files in list
all_dist_list <- lapply(all_dist_files_list, function(x) read_csv(x, progress = TRUE))

# change name of second column to a common name
all_dist_list2 <- lapply(all_dist_list, function(x) {colnames(x)[2] <- "roadsDist_m"; x})

# rbind the files of the list into one data frame. rbind.fill handles missing columns. 
all_dist_parcels <- do.call(rbind.fill, all_dist_list2)

# remove duplicates
all_dist_parcels <- all_dist_parcels %>% distinct(parloc_id, .keep_all = TRUE)

# finally, join the distance dfs together
FINAL_dists <- full_join(ndists, all_dist_parcels, by = "parloc_id")

# remove duplicates
FINAL_dists <- FINAL_dists %>% distinct(parloc_id, .keep_all = TRUE)

# save it
write_csv(x = FINAL_dists, path = "OUTPUT/FINAL_dists.csv")
FINAL_dists <- read.csv("OUTPUT/FINAL_dists.csv", header = T)
# FINAL_dists <- read_csv("OUTPUT/FINAL_dists.csv", col_types = list(chr,dbl,dbl,dbl,dbl))


#### READ IN NEAREST ROAD CONDITION ###
# read in data and eliminate OBJECTID column
FINAL_nearRoadCond <- read_csv("OUTPUT/roadCond.txt") %>% 
  select(-OBJECTID)


#### READ IN NDVI ###
# Read in list of csv files
# create a list of the files with their full path names
all_NDVI_files_list <- list.files("shapefiles/NDVI/OUTPUT", pattern = glob2rx("*NDVI*.csv$"), full.names = TRUE)

# read in csv files from list of all files
all_NDVI_list <- lapply(all_NDVI_files_list, function(x) read_csv(x, progress = TRUE))

# rbind the files of the list into one data frame. rbind.fill handles missing columns. 
FINAL_NDVI <- do.call(rbind.fill, all_NDVI_list)

# remove duplicates
FINAL_NDVI <- FINAL_NDVI %>% distinct(parloc_id, .keep_all = TRUE)

# save it
write_csv(x = FINAL_NDVI, path = "OUTPUT/FINAL_NDVI.csv")
FINAL_NDVI <- read_csv("OUTPUT/FINAL_NDVI.csv")


#### READ IN IMPERVIOUS SURFACE ###
# Read in list of csv files
# create a list of the files with their full path names
all_IMPERV_files_list <- list.files("shapefiles/impervious/OUTPUT", pattern = glob2rx("*IMPERV*.csv$"), full.names = TRUE)

# read in csv files from list of all files
all_IMPERV_list <- lapply(all_IMPERV_files_list, function(x) read_csv(x, progress = TRUE))

# rbind the files of the list into one data frame. rbind.fill handles missing columns. get rid of X1 column
FINAL_IMPERV <- do.call(rbind.fill, all_IMPERV_list) %>% 
  select(-X1)

# save it
write_csv(x = FINAL_IMPERV, path = "OUTPUT/FINAL_IMPERV.csv")
FINAL_IMPERV <- read_csv("OUTPUT/FINAL_IMPERV.csv")


### SAVE TO RDS
save(FINAL_buff_counts, FINAL_buff_lengths, FINAL_dists, FINAL_lu_buffs, FINAL_nearRoadCond, FINAL_NDVI, FINAL_IMPERV, file = "OUTPUT/FINAL_tables.Rds")
load("OUTPUT/FINAL_tables.Rds")



##### PUT IT ALL TOGETHER!

FINAL_COVARIATES <- parcels.mapc %>% 
  st_set_geometry(NULL) %>% # strip out geometry and convert to df
  distinct(parloc_id) %>% # remove duplicates and keep only parloc_id field
  # dplyr::select(parloc_id) %>% 
  left_join(.,FINAL_lu_buffs, by = "parloc_id") %>% 
  left_join(.,FINAL_buff_lengths, by = "parloc_id") %>% 
  left_join(.,FINAL_buff_counts, by = "parloc_id") %>%
  left_join(.,FINAL_dists, by = "parloc_id") %>%
  left_join(.,FINAL_nearRoadCond, by = "parloc_id") %>%
  left_join(.,FINAL_NDVI, by = "parloc_id") %>%
  left_join(.,FINAL_IMPERV, by = "parloc_id")


save(FINAL_COVARIATES, file = "OUTPUT/FINAL_COVARIATES.Rds")
write_csv(FINAL_COVARIATES, path = "OUTPUT/FINAL_COVARIATES.csv")

load("OUTPUT/FINAL_COVARIATES.Rds")

parcels_short_cov <- left_join(parcels_short, FINAL_COVARIATES3, by = "parloc_id")
# this does not work
st_write(obj = parcels_short_cov, dsn = "OUTPUT/shapes/FinalCovariates.gdb", layer = "FinalCovariates", driver = "OpenFileGDB")

# create a short named variable version for use in ArcGIS shapefiles
FINAL_COVARIATES_SHORT <- FINAL_COVARIATES3 %>% 
  rename(lu25Com = landuse_25mBuff_Commercial, 
         lu100Com = landuse_100mBuff_Commercial, 
         lu150Com = landuse_150mBuff_Commercial, 
         lu200Com = landuse_200mBuff_Commercial,
         lu500Indus = landuse_500mBuff_Industrial,
         lu50Trans = landuse_50mBuff_Transportation,
         lu100Trans = landuse_100mBuff_Transportation,
         lu1kTrans = landuse_1000mBuff_Transportation, 
         lu100Res = landuse_100mBuff_Residential, 
         lu150Res = landuse_150mBuff_Residential, 
         lu200Res = landuse_200mBuff_Residential, 
         lu250Res = landuse_250mBuff_Residential, 
         lu100Rec = landuse_100mBuff_Recreation, 
         lu1kRec = landuse_1000mBuff_Recreation, 
         lu25OL = landuse_25mBuff_Open.Land, 
         lu50OL = landuse_50mBuff_Open.Land, 
         lu200OL = landuse_200mBuff_Open.Land, 
         lu250OL = landuse_250mBuff_Open.Land, 
         lu300OL = landuse_300mBuff_Open.Land, 
         lu100Fors = landuse_100mBuff_Forest, 
         len25Rd = roadAllLen_Buf25, 
         len50Rd = roadAllLen_Buf50, 
         len1kRdC5 = roadC5Len_Buf1000, 
         len25RdC3 = roadC3Len_Buf25, 
         len50MjrRd = mjrRoadLenBuf50, 
         len50Bus = busRouteLen_Buf50, 
         len250Bus = busRouteLen_Buf250, 
         len300Bus = busRouteLen_Buf300, 
         len500Bus = busRouteLen_Buf500, 
         len500Tran = trainLen_Buf500, 
         len1kTrain = trainLen_Buf1000, 
         build50 = buildings_Buf50, 
         build100 = buildings_Buf100, 
         build150 = buildings_Buf150, 
         bStop50 = busStop_Buf50, 
         bStop100 = busStop_Buf100, 
         bStop150 = busStop_Buf150, 
         bbox300 = bboxStore_Buf300, 
         entertn500 = entertain_Buf500, 
         restrnt100 = restaurant_Buf100, 
         poliDist_m = policeDist_m, 
         OSDist_m = oSpaceDist_m, 
         rdsDist_m = roadsDist_m, 
         NDVI25 = buffer_mAvgNDVI25, 
         NDVI50 = buffer_mAvgNDVI50, 
         imp50 = pctImp50mBuff)

# write it out for use in ArcGIS
write_csv(FINAL_COVARIATES_SHORT, "OUTPUT/finalCovariatesShort.csv")
write.dbf(FINAL_COVARIATES_SHORT, "OUTPUT/finalCovariatesShort.dbf")

# import clean version of parcel layer
# parcels_short <- st_read(dsn = "shapefiles/MassachusettsLandParcelDatabase.gdb") %>% 
#   dplyr::select(parloc_id) %>% 
#   st_set_precision(1000000) %>% # set high precision and call st_make_valid() to avoid topology errors
#   lwgeom::st_make_valid() # fix geometric errors
# save(parcels_short, file = "OUTPUT/parcels_short.Rds")
# load("OUTPUT/parcels_short.Rds")
# join variables to parcels_short
# parcels_short2 <- left_join(parcels_short, FINAL_COVARIATES_SHORT, by = "parloc_id")
# get rid of feature 29672 which causes problems
# parcels_short3 <- parcels_short2[-29672,]
# write it out to a shapefile. generally too large and errors out.
# st_write(obj = parcels_short3, dsn = "OUTPUT/shapes", layer = "FinalCovariates.shp", driver = "ESRI Shapefile")
