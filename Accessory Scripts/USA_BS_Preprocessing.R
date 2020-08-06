require(sf)
require(fasterize)
require(raster)
require(data.table)
require(gdalUtils)
require(rgdal)
require(snow)
# require(doParallel)

root <- "E:/Research/Global_Relative_Inequalities/Data/Built_Settlement/"

## Level 1 USA mastergrid path:
l1_master_usa_path <- "E:/Research/Global_Relative_Inequalities/Data/L1_Zonal_100m/usa_grid_100m_L1_mosaic_fix.tif"
## USA shapefile paths:
usa_shp_dir <- "E:/Research/Global_Relative_Inequalities/Data/USA_shp/"
#state_l1_grid_dir <- "E:/Research/Global_Relative_Inequalities/Data/L1_Zonal_100m/"
usa_shp_paths <- list.files(paste0(usa_shp_dir),recursive = T,full.names = T)
usa_shp_paths <- grep(".*//.*/.*[.]shp", usa_shp_paths, value = T, perl = T)

##  Years to interpolate between 2000 and 2012:
interp_years <- c(2003,2006,2009)

##  Years to extrapolate past 2000 and 2012:
extrap_years <- c(2015,2018)

##  Reprocess masks?
mask_reprocess <- F

##  Reprocess 2000 and 2012 binaries?
reprocess_binaries <- F

##  Reaggregate 2000 and 2012 binaries to 1km?
reprocess_resample <- F

##  Do we interpolate and or extrapolate?
process_interp <- T
process_extrap <- T

if(process_interp){
  process_years <- interp_years
}
if(process_extrap){
  process_years <- extrap_years
}
if(process_extrap & process_interp){
  process_years <- c(interp_years,extrap_years)  
}




####  FUNCTION DEFINITIONS -----------------------------------------------------




ensure_dir <- function(dir_path){
  if(!file.exists(dir_path)){
    dir.create(dir_path,recursive = T)
  }
  return(dir_path)
}

wpTimeDiff <- function(start, end, frm="hms") {
  
  dsec <- as.numeric(difftime(end, start, units = c("secs")))
  hours <- floor(dsec / 3600)
  
  if (frm == "hms" ){
    minutes <- floor((dsec - 3600 * hours) / 60)
    seconds <- dsec - 3600*hours - 60*minutes
    
    out=paste0(
      sapply(c(hours, minutes, seconds), function(x) {
        formatC(x, width = 2, format = "d", flag = "0")
      }), collapse = ":")
    
    return(out)
  }else{
    return(hours)
  }
}


wpProgressMessage <- function (x, max = 100, label=NULL) {
  
  if (is.null(label)) label=''
  if (x != max) ar = '>' else ar=''
  
  percent <- x / max * 100
  cat(sprintf('\r[%-50s] %d%% %s',
              paste(paste(rep('=', percent / 2), collapse = ''),'',sep = ar),
              floor(percent),
              label))
  if (x == max)
    cat('\n')
}




##  Define the parallel task farm for masking tiles:
parallel_masking <- function(tile_list,
                             mask_tile_list,
                             ...) {
  
  tStart <- Sys.time()
  
  
  cl <- getCluster()
  on.exit( returnCluster() )  
  
  nodes <- length(cl)-1
  n_tasks <- length(tile_list)
  print(paste0("",n_tasks," tiles to mask."))
  
  clusterEvalQ(cl, {
    require(raster)
  })  
  
  
  clusterExport(cl, "n_tasks", envir=environment())
  clusterExport(cl, "tile_list", envir=environment()) 
  clusterExport(cl, "mask_tile_list", envir=environment()) 
  
  #################################################################################
  #################################################################################
  ##	Define the function that will be run on each cluster to do the predictions:
  #
  call_masking <- function (i) {
    
    tilemask_path <- tile_list[i]
    tilebs_path <- tile_list[i]
    
    ##  Load those rasters
    tilemask_ras <- raster(tilemask_path)
    tilebs_ras <- raster(tilebs_ras)
    
    ##  Carry out the masking to file:
    mask(tilebs_ras,
         tilemask_ras,
         filename = tilebs_path,
         format = "GTiff",
         overwrite=T,
         options =c("COMPRESS = LZW"))
    
    return("Done!")
  } 
  #
  ##
  #################################################################################
  #################################################################################
  
  
  ##	Start all nodes on a prediction:
  for (i in 1:nodes) {
    sendCall(cl[[i]], call_masking, i, tag=i)
  }  
  
  
  ########################################################################
  ##
  ## Create our primary cluster processing loop, recalling that we already
  ## have clusters running:
  #
  for (i in 1:n_tasks) {
    ##	Receive results from a node:
    predictions <- recvOneData(cl)
    ##	Check to see if we are at the end of our block list:
    ni <- nodes + i
    if (ni <= n_tasks) {
      sendCall(cl[[predictions$node]], call_masking, ni, tag=ni)
    }
    tEnd <-  Sys.time()
    
    wpProgressMessage(i, 
                      max=n_tasks, 
                      label= paste0("Received Task ",ni,
                                    " Processing Time: ",
                                    wpTimeDiff(tStart,tEnd)))
  }
}  




cluster_lin_interp <- function(prediction_raster_path_list, 
                               prediction_raster,
                               blocks,
                               ...){
  tStart <- Sys.time()
  
  cl <- getCluster()
  on.exit( returnCluster() )  
  
  nodes <- length(cl)
  
  clusterEvalQ(cl, {
    require(raster)
  })  
  
  
  clusterExport(cl, c("usa_stack",
                      "prediction_ras_path_list",
                      "prediction_raster",
                      "process_years"))
  clusterExport(cl, "blocks", envir=environment())
  
  #################################################################################
  #################################################################################
  ##	Define the function that will be run on each cluster to do the predictions:
  #
  call_predictions <- function (i) {
    row_data <- data.frame( getValues(usa_stack, 
                                      row=blocks$row[i], 
                                      nrows=blocks$nrows[i]) )
    
    ## Convert field names to something more manageable and 
    ## that matches our popfit variable list:
    ## Full covariate stack:
    #
    names(row_data) <- c("Y2000","Y2012")
    
    ##	Detect if we have any NA or Inf values, and that the values are 
    ##		covered by our census administrative units:
    #
    na_present <- is.na(row_data[,1])|is.na(row_data[,2])
    
    ## Use the first if you want to mask out water pixels, this can greatly
    ## speed up predictions over areas with a lot of water, however, you
    ## run the risk of having no predictions in the resulting dataset
    ## if you have a census block small enough that it might only have
    ## water cover (GeoCover/GlobCover is what determines the water mask):
    #
    roi_subset <- (!na_present)
    
    
    ##	Create a set of predictions based on our covariates:
    #
    predictions <- numeric(length=length(row_data[,1]))
    predictions[] <- NA
    ##  Set up our expression string to be parsed:
    eval_str <- "predictions <- data.frame("
    for(t in 1:length(process_years)){
      eval_str <- paste0(eval_str,
                         sprintf("'Y%d\' = predictions", 
                                 process_years[t]),
                         ifelse(t!=length(process_years),", ",""))
    }
    eval_str <- paste0(eval_str,")")
    
    ##  Create our data.frame to hold our predictions
    eval(parse(text=eval_str))
    
    
    
    ## If we have data where NAs or Inf values are not present then we 
    ## predict for those cells (where we subset our data according to the 
    ## roi_subset and remove the census zone and water mask columns (length(row_data) - 2):
    #
    if(any(roi_subset)){
      ##  Get the linear slope by subtracting the 2000 proportion values from 
      ##  the 2012 proporton values and dividing by the number of years:
      slope_set <- {row_data[roi_subset,2]-row_data[roi_subset,1]}/{2012-2000}
      
      for(y in process_years){
        eval_str <- paste0(eval_str,")")
        predictions[roi_subset,paste0("Y",y)] <- row_data[roi_subset,1] + slope_set*{y-2000}
        if(y > 2012){
          ##  If values are greater than 1, cap them at 1
          if(any(predictions[,paste0("Y",y)] > 1,na.rm=T)){
            predictions[which(predictions[,paste0("Y",y)] > 1),
                        paste0("Y",y)] <- 1
          }
        }
      }
    }
    eval_str <- "c("
    for(t in 1:length(process_years)){
      eval_str <- paste0(eval_str,
                         sprintf("max(predictions$Y%d, na.rm=T) > 1", 
                                 process_years[t]),
                         ifelse(t!=length(process_years),", ",")"))
    }
    
    ##  If any of our values are greater than 1 (out of bounds) toss an error message.
    if(any(eval(parse(text=eval_str)),na.rm=T)){
      stop(paste0("Predictions greater than 1; figure it out."))
    }else{return(predictions)}
  } 
  #
  ##
  #################################################################################
  #################################################################################
  
  
  
  ##	Start all nodes on a prediction:
  for (i in 1:nodes) {
    sendCall(cl[[i]], call_predictions, i, tag=i)
  }  
  
  ## Start the raster writer object so we can store our results as they
  ## come back from our cluster:  
  for(t in 1:length(process_years)){
    eval_str_1 <- sprintf("prediction_raster_%d <- prediction_raster",
                          process_years[t])
    eval_str_2 <- paste0(sprintf("prediction_raster_%d <- ",process_years[t]),
                         "raster::writeStart(prediction_raster, ",
                         "filename=prediction_raster_path_list[[t]],",
                         " format='GTiff', datatype='FLT4S',",
                         " overwrite=TRUE, options=c('COMPRESS=LZW'))")
    
    ##  Assign a year specific copy of the prediction raster:
    eval(parse(text=eval_str_1))
    ##  Begin the raster writing operations for the year specific raster:
    eval(parse(text=eval_str_2))
  }
  # 
  # prediction_raster_2003 <- prediction_raster
  # prediction_raster_2003 <- raster::writeStart(prediction_raster, 
  #                                              filename=prediction_raster_path_list[[1]], 
  #                                              format="GTiff", 
  #                                              datatype="FLT4S", 
  #                                              overwrite=TRUE, 
  #                                              options=c("COMPRESS=LZW"))
  # prediction_raster_2006 <- prediction_raster
  # prediction_raster_2006 <- raster::writeStart(prediction_raster, 
  #                                              filename=prediction_raster_path_list[[2]], 
  #                                              format="GTiff", 
  #                                              datatype="FLT4S", 
  #                                              overwrite=TRUE, 
  #                                              options=c("COMPRESS=LZW"))
  # prediction_raster_2009 <- prediction_raster
  # prediction_raster_2009 <- raster::writeStart(prediction_raster, 
  #                                              filename=prediction_raster_path_list[[3]], 
  #                                              format="GTiff", 
  #                                              datatype="FLT4S", 
  #                                              overwrite=TRUE, 
  #                                              options=c("COMPRESS=LZW"))
  # prediction_raster_2015 <- prediction_raster
  # prediction_raster_2015 <- raster::writeStart(prediction_raster, 
  #                                              filename=prediction_raster_path_list[[4]], 
  #                                              format="GTiff", 
  #                                              datatype="FLT4S", 
  #                                              overwrite=TRUE, 
  #                                              options=c("COMPRESS=LZW"))
  # prediction_raster_2018 <- prediction_raster
  # prediction_raster_2018 <- raster::writeStart(prediction_raster, 
  #                                              filename=prediction_raster_path_list[[5]], 
  #                                              format="GTiff", 
  #                                              datatype="FLT4S", 
  #                                              overwrite=TRUE, 
  #                                              options=c("COMPRESS=LZW"))
  
  
  ########################################################################
  ##
  ## Create our primary cluster processing loop, recalling that we already
  ## have clusters running:
  #
  
  
  for (i in 1:blocks$n) {
    
    ##	Receive results from a node:
    predictions <- recvOneData(cl)
    
    ##	Check if there was an error:
    if (!predictions$value$success) {
      stop("ERROR: Cluster barfed...\n\n", predictions)
    }
    
    ##	Which block are we processing:
    block <- predictions$value$tag
    
    for(t in 1:length(process_years)){
      eval_str <- paste0(sprintf("prediction_raster_%d <- ",process_years[t]),
                         sprintf("writeValues(prediction_raster_%d,",
                                 process_years[t]),
                         sprintf(" predictions$value$value$Y%d,",
                                 process_years[t]),
                         " blocks$row[block])")
      
      
      ##  Write the returned values into the year-specific raster:
      eval(parse(text=eval_str))
    }
    # prediction_raster_2003 <- writeValues(prediction_raster_2003, 
    #                                       predictions$value$value$Y2003, 
    #                                       blocks$row[block])
    # prediction_raster_2006 <- writeValues(prediction_raster_2006, 
    #                                       predictions$value$value$Y2006, 
    #                                       blocks$row[block])
    # prediction_raster_2009 <- writeValues(prediction_raster_2009, 
    #                                       predictions$value$value$Y2009, 
    #                                       blocks$row[block])
    # prediction_raster_2015 <- writeValues(prediction_raster_2015, 
    #                                       predictions$value$value$Y2015, 
    #                                       blocks$row[block])
    # prediction_raster_2018 <- writeValues(prediction_raster_2018, 
    #                                       predictions$value$value$Y2018, 
    #                                       blocks$row[block])
    
    ##	Check to see if we are at the end of our block list:
    ni <- nodes + i
    if (ni <= blocks$n) {
      sendCall(cl[[predictions$node]], call_predictions, ni, tag=ni)
    }
    tEnd <-  Sys.time()
    
    wpProgressMessage(i, max=blocks$n, 
                      label= paste0("Received block ",ni, 
                                    " Processing Time: ", wpTimeDiff(tStart,tEnd)))
  }
  
  for(t in 1:length(process_years)){
    eval_str <- paste0(sprintf("prediction_raster_%d <- ",process_years[t]),
                       sprintf("writeStop(prediction_raster_%d)",
                               process_years[t]))
    
    
    ##  Stop the raster writing operations:
    eval(parse(text=eval_str))
  }
  # prediction_raster_2003 <- writeStop(prediction_raster_2003)
  # prediction_raster_2006 <- writeStop(prediction_raster_2006)
  # prediction_raster_2009 <- writeStop(prediction_raster_2009)
  # prediction_raster_2015 <- writeStop(prediction_raster_2015)
  # prediction_raster_2018 <- writeStop(prediction_raster_2018)
}




##  Creation of masks ----
##  Here I will create two type of state level masks: 
##  one for removing values outside of the state of interest
##  and one for removing the original US state values in the global layer in 
##  order to overwrite the values with interpolated values (i.e. if Colorado is 
##  our area of interest, then we put a value of zero in the mask and 1 
##  everywhere else so we multiply the mask by the original layer.

for(s in usa_shp_paths){
  shp <- usa_shp_paths[s]
  state <- strsplit(basename(shp),".shp")[[1]]
  print(paste0("Working on state: ", state))
  if(mask_reprocess == T | 
     !file.exists(paste0(root,"Masks/",state, "_",
                         "ccidadminl1",
                         ".tif"))){
    ##  Create the state level mask (1 in areas of interests, NAs everywhere 
    ##  else):
    if(state=="Alaska"){
      shp <- paste0(root,"Alaska.shp") 
    }
    
    shpf <- st_read(shp, state)
    ##  Bring in the USA level1 mastergrid:
    l1_master_usa <- raster(l1_master_usa_path)
    
    ##  Crop to the extents of the shapefile:
    l1_master_usa_crop <- crop(l1_master_usa,shpf)
    ##  Replace all values that are nonzero with a 1:
    l1_master_usa_crop[values(l1_master_usa_crop)>0]<-1
    
    writeRaster(l1_master_usa_crop,
                filename = paste0(root,"Masks/",state,"_ccidadminl1.tif"),
                format = "GTiff",
                datatype = "INT1U",
                overwrite=T,
                options=c("COMPRESS=LZW"))
    
  }
  if(mask_reprocess == T |
     !file.exists(paste0(root,"Masks/",state, "_",
                         "eraser_mask",
                         ".tif"))){
    ##  Create a mask where we have zero values for areas of interest and 1 for
    ##  everywhere else:
    if(state=="Alaska"){
      shp <- paste0(root,"Alaska.shp") 
    }
    shpf <- st_read(shp, state)
    ##  Bring in the USA level1 mastergrid:
    l1_master_usa <- raster(l1_master_usa_path)
    rm(l1_master_usa)
    
    ##  Crop to the extents of the shapefile:
    l1_master_usa_crop <- crop(l1_master_usa,shpf)
    
    ##  Replace all values that are zero or NA with a 1:
    start_t <- Sys.time()
    foo_ind_1 <- values(l1_master_usa_crop)==0
    l1_master_usa_crop[foo_ind_1] <- 1
    rm(foo_ind_1)
    gc()
    print(paste0("     Start: ", start_t))
    print(paste0("     End: ", Sys.time()))
    
    start_t <- Sys.time()
    foo_ind_2 <- is.na(values(l1_master_usa_crop))
    l1_master_usa_crop[foo_ind_2] <- 1
    rm(foo_ind_2)
    gc()
    print(paste0("     Start: ", start_t))
    print(paste0("     End: ", Sys.time()))
    
    ##  Replace all values greater than 1 with a zero:
    start_t <- Sys.time()
    foo_ind_3 <- values(l1_master_usa_crop)>1
    l1_master_usa_crop[foo_ind_3]<-0
    rm(foo_ind_3)
    gc()
    print(paste0("     Start: ", start_t))
    print(paste0("     End: ", Sys.time()))
    writeRaster(l1_master_usa_crop,
                filename = paste0(root,"Masks/",state,"_eraser_mask.tif"),
                format = "GTiff",
                datatype = "INT1U",
                overwrite=T,
                options=c("COMPRESS=LZW"))
    rm(l1_master_usa_crop)
  }
}


####  CREATE 2000 AND 2012 BINARIES  -------------------------------------------
for(y in c(2000,2012)){
  for(shp in usa_shp_paths){
    ##  Pull the state name:
    state <- strsplit(basename(shp),".shp")[[1]]
    if(reprocess_binaries == T | !file.exists(paste0(root,"USA_CutData/",state, "_",
                                                     ifelse(y==2000,
                                                            "GHSL_ESA_2000_USA",
                                                            "GUF_GHSL_2012_USA"),
                                                     ".tif"))){
      
      ##  Bring in the USA shapefile:
      if(state!="Alaska"){
        foo_shp <- st_read(shp,
                           layer = state,
                           stringsAsFactors = F)
      }else{
        foo_shp <- st_read(paste0(root,"Alaska.shp"),
                           layer = state,
                           stringsAsFactors = F)
      }
      mask_ras <- raster(paste0(root,"Masks/",state,
                                "_ccidadminl1.tif"))
      
      ##  Bring in the full BS layer at 100m resolution:
      full_bs_ras <- raster(Sys.glob(paste0(root, "Binary/GP/",
                                            ifelse(y==2000,
                                                   "backfiltered*.tif",
                                                   "GUF12*.tif")))[1])
      
      print(paste0("Cropping to ", state," ", y, " Extents: ", Sys.time()))
      
      ##  Crop it to the extents of the US shapefile:
      crop_bs_ras <- crop(full_bs_ras,
                          mask_ras,
                          snap="near")
      rm(full_bs_ras)
      
      
      if(state == "Alaska"){
        mask_ras <- fasterize(foo_shp,crop_bs_ras,"DN",fun= "first")
      }
      
      ##  Check that the extents and the number of pixels match between the l1 
      ##  raster and the BS cropped raster:
      extent_match <- ifelse(any(!(extent(crop_bs_ras) == extent(mask_ras)),
                                 na.rm = T),
                             "Extents do not match.",
                             "Extents match.")
      n_pixels <- ifelse(ncell(crop_bs_ras) == ncell(mask_ras),
                         "Same number of pixels",
                         "Different numbers of pixels")
      if(n_pixels == "Different numbers of pixels"){
        mask_ras <- fasterize(foo_shp,crop_bs_ras,"DN",fun= "first")
        extent_match <- ifelse(any(!(extent(crop_bs_ras) == extent(mask_ras)),
                                   na.rm = T),
                               "Extents do not match.",
                               "Extents match.")
      }
      if(extent_match != "Extents match."){
        ymin(mask_ras) <- ymin(crop_bs_ras)
        ymax(mask_ras) <- ymax(crop_bs_ras)
        xmin(mask_ras) <- xmin(crop_bs_ras)
        xmax(mask_ras) <- xmax(crop_bs_ras)
      }      
      
      print(paste0("Masking to ", state," ", y, " Extents: ", Sys.time()))
      
      
      
      ##  Mask the raster, turning pixels outside of the mask to NA values:
      crop_mask_bs_ras <- mask(crop_bs_ras,
                               mask = mask_ras)
      
      outname <- paste0(state, "_",
                        ifelse(y==2000,
                               "GHSL_ESA_2000_USA",
                               "GUF_GHSL_2012_USA"),
                        ".tif")
      print(paste0("Writing to file: ", Sys.time()))
      
      ##  Write the cropped and masked layer to file
      writeRaster(crop_mask_bs_ras,
                  filename = paste0(root, "USA_CutData/",outname),
                  format = "GTiff",
                  datatype = "INT1U",
                  overwrite = T,
                  options = c("COMPRESS = LZW"))
      
      print(paste0("Finished for year ", y, " : ", Sys.time()))
      
      rm(crop_mask_bs_ras)
      gc()}
  }
}


##  Built Settlement Resampling  ----
##  Here I need to mosaic the individual state rasters and then resample to 
##  1km resolution (using average function) prior to interpolating. Will mosaic,
##  aggregate using mean, tile for interpolation, interpolate linearly, remosaic, 
##  and then write into the larger global file using a maximum function. I would
##  assume that only possible overlap issues would happen around Mexico border 
##  and then we'll assume that the larger proportion is better than assumming an 
##  underestimate.

##  By year, retrieve all of the state rasters for mosaicing:
if(reprocess_resample == T){
  for(y in c(2000,2012)){
    state_ras_list <- Sys.glob(paste0(root, "USA_CutData/*_",
                                      ifelse(y==2000,
                                             "GHSL_ESA_2000_USA",
                                             "GUF_GHSL_2012_USA"),
                                      ".tif"))
    #state_ras_list <- grep(".*(?:Alaska|Texas).*[.]tif",state_ras_list, perl=T,invert = T, value = T)
    start_t <- Sys.time()
    
    ##  Now mosaic that bad boy back together:
    mosaic_rasters(gdalfile=state_ras_list, 
                   dst_dataset = paste0(root,"USA_CutData/","USA_",
                                        ifelse(y==2000,
                                               "GHSL_ESA_2000_clipped",
                                               "GUF_GHSL_2012_clipped"),".tif"), 
                   of = "GTiff", 
                   ot = "Byte", 
                   co = "COMPRESS=LZW",
                   force_ot = "Int16",
                   verbose = T,
                   scale = c(0,1,0,1))
    print(paste0("     Start: ", start_t))
    print(paste0("     End: ", Sys.time()))
    
    ##   Resample the entire USA for the given year:
    outname <- paste0("USA_",ifelse(y==2000,
                                    "GHSL_ESA_2000_clipped",
                                    "GUF_GHSL_2012_clipped"),
                      "_1km.tif")
    usa_in_ras <- raster(paste0(root,"USA_CutData/","USA_",
                                ifelse(y==2000,
                                       "GHSL_ESA_2000_clipped",
                                       "GUF_GHSL_2012_clipped"),".tif"))
    #  Note: the below takes about 6 hours to aggregate.
    raster::aggregate(usa_in_ras,
                      fact = 10,
                      fun = "mean",
                      expand = T,
                      na.rm=T,
                      filename = paste0(root, "USA_CutData/",outname),
                      format = "GTiff",
                      datatype = "FLT4S",
                      overwrite = T,
                      options = c("COMPRESS = LZW"))
    rm(usa_in_ras)
    gc()
    
  }
}




##  INTERPOLATION  -----

##  Prepare to tile the whole USA rasters for interpolation
##  Get the two rasters and bring them in:
usa_2000_path <- paste0(root,"USA_CutData/",
                        "USA_GHSL_ESA_2000_clipped_1km.tif")
usa_2012_path <- paste0(root,"USA_CutData/",
                        "USA_GUF_GHSL_2012_clipped_1km.tif")

usa_2000_ras <- raster(usa_2000_path)
usa_2012_ras <- raster(usa_2012_path)



##  Check that the extents and the number of pixels are equal prior to any 
##  tiling shenanigans:
extent_match <- ifelse(any(!(extent(usa_2000_ras) == extent(usa_2012_ras)),
                           na.rm = T),
                       "Extents do not match.",
                       "Extents match.")
n_pixels <- ifelse(ncell(usa_2000_ras) == ncell(usa_2012_ras),
                   "Same number of pixels",
                   "Different numbers of pixels")

if(extent_match == "Extents do not match." | 
   n_pixels == "Different numbers of pixels"){
  ##  If this specific case:
  if(n_pixels == "Same number of pixels" &
     extent_match != "Extents match."){
    ymin(usa_2012_ras) <- ymin(usa_2000_ras)
    ymax(usa_2012_ras) <- ymax(usa_2000_ras)
    xmin(usa_2012_ras) <- xmin(usa_2000_ras)
    xmax(usa_2012_ras) <- xmax(usa_2000_ras)
    
    extent_match <- ifelse(any(!(extent(usa_2012_ras) == extent(usa_2012_ras)),
                               na.rm = T),
                           "Extents do not match.",
                           "Extents match.")
    n_pixels <- ifelse(ncell(usa_2000_ras) == ncell(usa_2012_ras),
                       "Same number of pixels",
                       "Different numbers of pixels")
  }else{
    stop(n_pixels)
  }
}


if(extent_match == "Extents match." & 
   n_pixels == "Same number of pixels"){
  
  start_t <- Sys.time()
  ##  Make the log transformed versions:
  usa_2000_ras <- log({usa_2000_ras+0.001},
                      base=exp(1))
  usa_2012_ras <- log({usa_2012_ras+0.001},
                      base=exp(1))
  usa_log_slope <- {usa_2012_ras-usa_2000_ras}/12
  
  
  ##  Write the raster to file:
  writeRaster(usa_log_slope,
              filename = paste0(root,"USA_logistic_slope_raster_2012_2000.tif"),
              datatype=dataType(usa_2000_ras),
              overwrite=T,
              format="GTiff",
              options=c("COMPRESS = LZW"))
  
  
  for(y in process_years){
    ##  Calculate the amount of change for our year and add it to the base year:
    if(y < 2012){
      foo_log_ras <- usa_2000_ras + {usa_log_slope*{y-2000}}
    }
    if(y > 2012){
      foo_log_ras <- usa_2012_ras + {usa_log_slope*{y-2012}}
    }
    
    ##  Back transform our data:
    foo_ras <- exp(foo_log_ras)-0.001
    rm(foo_log_ras)
    
    ##  Make sure any extremely small value artifacts are set to 0:
    foo_ras[foo_ras < 0.00001 ] <- 0
    
    ##  If we are looking at extrapolation, make sure any values over 1 are 
    ##  capped at 1:
    if(y > 2012){
      foo_ras[foo_ras > 1] <- 1
    }
    
    writeRaster(foo_ras,
                filename = paste0(root,"USA_",y,"_BS_1km.tif"),
                datatype=dataType(usa_2000_ras),
                overwrite=T,
                format="GTiff",
                options=c("COMPRESS = LZW"),
                verbose=T)
    rm(foo_ras)
  }
  
  
  
  
  # ##  Create 3 template rasters to hold our predictions:
  # prediction_raster <- raster(usa_2000_path)
  # 
  # ##  Figure out our number of blocks:
  # blocks <- blockSize(prediction_raster,
  #                     minblocks=850, 
  #                     n = length(process_years))
  # 
  # 
  # ##  Construct the list of our output raster paths:
  # ##  NOTE:  These files will be at 0.008333 resolution and have values 
  # ##  representing interpolated BS as constructed from 100m data.
  # eval_str <- paste0("prediction_ras_path_list <- list(")
  # for(t in 1:length(process_years)){
  #   foo_str <- paste0("paste0(root,",
  #                     sprintf("'USA_%d_BS_1km.tif')",process_years[t]),
  #                     ifelse(t!=length(process_years),", ",""))
  #   eval_str <- paste0(eval_str, foo_str)
  # }
  # eval_str <- paste0(eval_str, ")")
  # 
  # ##  Create a list of the prediction raster paths:
  # eval(parse(text=eval_str))
  # # prediction_ras_path_list <- list(paste0(root,"USA_2003_BS_1km.tif"),
  # #                                  paste0(root,"USA_2006_BS_1km.tif"),
  # #                                  paste0(root,"USA_2009_BS_1km.tif"),
  # #                                  paste0(root,"USA_2015_BS_1km.tif"),
  # #                                  paste0(root,"USA_2018_BS_1km.tif"))
  # 
  # ##  Stack the 2 USA rasters together:
  # usa_stack <- stack(list(usa_2000_path,usa_2012_path))

  # ##  Carry out the interpolation using the cluster farm:
  # beginCluster()
  # cluster_lin_interp(prediction_raster_path_list = prediction_ras_path_list,
  #                    prediction_raster = prediction_raster,
  #                    blocks = blocks)
  # endCluster()
}
rm(usa_2000_ras, usa_2000_ras, usa_log_slope)
gc()

print(paste0("     Start: ", start_t))
print(paste0("     End: ", Sys.time()))



##  REINTEGRATE WITH GLOBAL SETS  ----
##  ---  Create USA Zero Mask  ---  ##
##  Bring in the USA Shapefile:

for(y in c(process_years)){
  usa_shp_50 <- st_read(paste0(usa_shp_dir,"840_USA.shp"),
                        layer= "840_USA",stringsAsFactors = F)
  usa_shp_50$mask_val <- 0
  
  
  print(paste0("Rasterizing USA '0 1 NA' Mask..."))
  ##  Create a mask (global in extent) where the USA portion has a value of zero
  ##  and the rest is 1 (this is at 0.008333 res):
  global_ras <- raster(paste0(root,"Prp_1km/BS_",y,"_1km_PRP.tif"))
  usa_mask_0_1_NA <- fasterize(usa_shp_50,
                               global_ras,
                               field = "mask_val",
                               fun = "sum",
                               background = 1)
  
  print(paste0("Masking global layer for year ",y ,"..."))
  ##  If we we are using 2018, the base global layer from worldpop has all no 
  ##  data in the US. Need to turn those into 0 values instead otherwise we end 
  ##  up adding new values to NA producing more NA values.
  if(y == 2018){
    indx <- which(values(usa_mask_0_1_NA)==0)
    global_ras[indx] <- 0
    rm(indx)
  }
  ##  Multiply the mask by the original global BS PRP layer, i.e. erase the 
  ##  existing US values which remained static between 2000 and 2012 in the 
  ##  worldpop files:
  global_ras_masked <- global_ras * usa_mask_0_1_NA
  
  
  rm(global_ras, usa_mask_0_1_NA)
  gc()
  
  print(paste0("Overwriting USA interpolated values for year ", y, "..."))
  ##  Add the interpolated USA raster values to a raster that has the extents of 
  ##  the globe, but has all zero values.
  orig_bs_1km_usa <- raster(paste0(root,"USA_",y,"_BS_1km.tif"))
  ##  Replace the NA values with 0:
  orig_bs_1km_usa[is.na(orig_bs_1km_usa)] <- 0
  ##  Change the exents of the original interpolated US 1km PRP BS raster 
  ##  (0.008333 res) to that of the global:
  new_bs_1km_usa <- extend(orig_bs_1km_usa,global_ras_masked, value = 0) 
  
  rm(orig_bs_1km_usa)
  
  ##  Make sure the extents align:
  ymin(new_bs_1km_usa) <- ymin(global_ras_masked)
  ymax(new_bs_1km_usa) <- ymax(global_ras_masked)
  xmin(new_bs_1km_usa) <- xmin(global_ras_masked)
  xmax(new_bs_1km_usa) <- xmax(global_ras_masked)
  
  ##  Add the interpolated USA layers to the rest of the correct global layer 
  ##  (0.008333 res):
  global_ras_redone <- global_ras_masked + new_bs_1km_usa
  
  print(paste0("Writing the raster to file..."))
  ##  Write it to file:
  writeRaster(global_ras_redone,
              file = paste0(root, "Prp_1km/BS_",y,"_1km_PRP_w_USA_interp.tif"),
              format = "GTiff",
              datatype = "FLT4S", 
              overwrite = T,
              options=c("COMPRESS = LZW"))
  rm(global_ras_redone, global_ras_masked, new_bs_1km_usa)
  gc()
}
for(y in c(2015,2018)){
  print(y)
  ##TODO:  JJN 2020-08 ISSUE with the shapefile not lining up with the US 
  ##       extents and not zeroing out their values. Need to see if I can get 
  ##       the L0 raster to line up with it
  ##  For now, since they are literally a handful of pixels on the coast, we 
  ##  will simply max them out at 1 for now:
  global_ras_redone <- raster(paste0(root, "Prp_1km/BS_",y,"_1km_PRP_w_USA_interp.tif"))
  global_ras_redone[global_ras_redone>1] <- 1
  writeRaster(global_ras_redone,
              file = paste0(root, "Prp_1km/BS_",y,"_1km_PRP_w_USA_interp.tif"),
              format = "GTiff",
              datatype = "FLT4S", 
              overwrite = T,
              options=c("COMPRESS = LZW"))
  rm(global_ras_redone)
  gc()
}
