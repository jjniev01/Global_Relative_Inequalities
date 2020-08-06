require(raster)
require(snow)
require(wpUtilities)




####
##  GENERAL STATEMENTS  ----
root <- "E:/Research/Global_Relative_Inequalities/Data/"
pop_raster_dir <- paste0(root,"Population/")
urbdens_raster_dir <- paste0(root,"Built_Settlement/")
lan_raster_dir <- paste0(root,"LAN_1992_2018/")

core_number <- 7
threshold_val <- 5
years <- c(2000,2003,2006,2009,2012,2015,2018)
##  The year to base the mask off of, i.e. apply the threshold:
mask_year <- 2012
use_log <- F
make_mask <- F
##  END:  GENERAL STATEMENTS
####




####
##  FUNCTION DEFINITIONS  ----
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




wpProgressMessage <- function(x, max = 100, label=NULL){
  # x integer
  # max maximum for progress bvar
  # label additional text for progress  bar  
  if(is.null(label)){
    label=''}
  if(x != max){
    ar = '>'
  }else{ar=''}
  percent <- x/max * 100
  cat(sprintf('\r[%-50s] %d%% %s',
              paste(paste(rep('=', percent / 2), collapse = ''),'',sep = ar),
              floor(percent),
              label))
  if(x == max){
    cat('\n')}
}




# wpSetAllValuesTo <- function(x,
#                              v,
#                              filename=rasterTmpFile(),
#                              NAflag=NULL, 
#                              datatype=NULL, 
#                              overwrite=TRUE, 
#                              cores=NULL, 
#                              minblk=NULL, 
#                              cblk=NULL, 
#                              silent=TRUE) {
#   
#   if (!file.exists(dirname(filename))){
#     stop(paste0("Directory  ",dirname(filename)," for file ", basename(filename) ," does not exist"))
#   }
#   
#   if (is.null(NAflag)) NAflag=255
#   if (is.null(datatype)) datatype='INT1U'
#   if (is.null(cblk)) cblk=1
#   
#   if (!is(NAflag, "numeric")) stop(paste0("NAflag should be  numeric"))
#   if (!is(overwrite, "logical")) stop(paste0("overwrite should be  logical (e.g., TRUE, FALSE)"))
#   if (!is(silent, "logical")) stop(paste0("silent should be logical (e.g., TRUE, FALSE)"))
#   if (!is(v, "numeric")) stop(paste0("v should be numeric"))
#   
#   datatype <- toupper(datatype)
#   
#   if (!(datatype %in% c('INT1S', 'INT2S', 'INT4S', 'FLT4S', 'LOG1S', 'INT1U', 'INT2U', 'INT4U', 'FLT8S'))) {
#     stop('not a valid data type. Avalible are INT1S/INT2S/INT4S/FLT4S/LOG1S/INT1U/INT2U/INT4U/FLT8S')
#   }
#   
#   if (!cblk%%1==0) stop(paste0("cblk should be integer"))
#   
#   
#   if ( file.exists(filename) & overwrite==FALSE) {
#     stop(paste0("File ",filename," exist. Use option overwrite=TRUE"))
#   } else{
#     if ( file.exists(filename) ) file.remove(filename)
#   }
#   
#   stopifnot(hasValues(x))
#   
#   # get real physical cores in a computer
#   max.cores <- parallel:::detectCores(logical = TRUE)
#   
#   if (is.null(cores)) {
#     cores <- max.cores - 1
#   }
#   
#   if (cores > max.cores) {
#     stop(paste0("Number of cores ",cores," more then real physical cores in PC ",max.cores ))
#   }
#   
#   
#   if (is.null(minblk)) {
#     minblk <- wpGetBlocksNeed(x,cores,n=cblk)
#   }
#   
#   beginCluster(n=cores)
#   
#   tStart <- Sys.time()
#   
#   blocks <- raster:::blockSize(x,minblocks=minblk)
#   
#   if (!silent) { 
#     cat(paste0('\nTotal blocks ', blocks$n))
#     cat('\n')
#   }        
#   
#   cl <- getCluster()
#   
#   nodes <- length(cl)
#   
#   if (is.null(minblk)) {
#     minblk <- nodes
#   }   
#   
#   clusterExport(cl, c("blocks", "x","v"), envir=environment())
#   
# 
#   
#     
#   # wpSetAllValue <- function(i) {
#   #   
#   #   tryCatch({
#   #     
#   #     r.val <- raster:::getValues(x, row=blocks$row[i], nrows=blocks$nrows[i])
#   #     
#   #     nncol <- ncol(x)
#   #     
#   #     if (i==1){
#   #       start.df <- 1
#   #       end.df <- blocks$nrows[i]*nncol
#   #     }else{
#   #       start.df <- nncol*blocks$row[i] - nncol + 1
#   #       end.df <- (nncol*blocks$row[i] + blocks$nrows[i]*nncol) - nncol
#   #     }
#   #     
#   #     df <- data.frame(CellIndex = as.numeric(start.df:end.df) )
#   #     df$v <- as.numeric(r.val)  
#   #     
#   #     df[!is.na(df$v),"v"] <- v
#   #     
#   #   }, error = function(e) stop(paste0("The block '", blocks$row[i], "'",
#   #                                      " caused the error: '", e, "'")))
#   #   
#   #   return(df$v)
#   # }     
#   
#   for (i in 1:nodes) {
#     parallel:::sendCall(cl[[i]], wpSetAllValue, i, tag=i)
#   }      
#   
#   out <- x
#   
#   out <- raster:::writeStart(out, 
#                              filename=filename, 
#                              format="GTiff", 
#                              datatype=datatype, 
#                              overwrite=overwrite, 
#                              options=c("COMPRESS=LZW"),
#                              NAflag=NAflag)     
#   
#   for (i in 1:blocks$n) {
#     
#     d <- parallel:::recvOneData(cl)
#     
#     if (! d$value$success ) {
#       stop('cluster error')
#     }
#     
#     tEnd <-  Sys.time()
#     
#     b <- d$value$tag
#     
#     if (!silent) { 
#       wpProgressMessage(i, max=blocks$n, label= paste0("received block ",b, " Processing Time: ", wpTimeDiff(tStart,tEnd)))
#     }
#     out <- raster:::writeValues(out, d$value$value, blocks$row[b])
#     
#     # need to send more data
#     #
#     ni <- nodes + i
#     if (ni <= blocks$n) {
#       parallel:::sendCall(cl[[d$node]], wpSetAllValue, ni, tag=ni)
#     }
#   }
#   
#   out <- raster:::writeStop(out)      
#   
#   endCluster()
#   
#   return(out)
# }




wpSetValueWhichindexes <- function(x, 
                                   y,
                                   v, 
                                   filename=rasterTmpFile(), 
                                   NAflag=NULL, 
                                   datatype=NULL, 
                                   overwrite=TRUE, 
                                   cores=NULL, 
                                   minblk=NULL, 
                                   cblk=NULL, 
                                   silent=TRUE){
  # x Raster* object
  # indexes of the pixels to be replace with value v
  # v value of the pixel we would like to get indexes
  # filename File of a new raster file.
  # NAflag NO data value will be used for a new raster
  # datatype Type of raster. Avalible are INT1S/INT2S/INT4S/FLT4S/LOG1S/INT1U/INT2U/INT4U/FLT8S
  # overwrite Overwrite existing file
  # cores Integer. Number of cores for parallel calculation
  # minblk Integer. Minimum number of blocks. If NULL then it will be calculated automaticly
  # cblk Integer. param to controle min number of blocks during paralisation
  # silent If FALSE then the progress will be shown  
  if(!file.exists(dirname(filename))){
    stop(paste0("Directory  ",dirname(filename)," for file ", 
                basename(filename), " does not exist"))
  }
  
  if(is.null(NAflag)){NAflag=255}
  if(is.null(datatype)){datatype='INT1U'}
  if(is.null(cblk)){cblk=1}
  
  if(!is(NAflag, "numeric")){stop(paste0("NAflag should be  numeric"))}
  if(!is(overwrite, "logical")){stop(paste0("overwrite should be  logical (e.g., TRUE, FALSE)"))}
  if(!is(silent, "logical")){stop(paste0("silent should be logical (e.g., TRUE, FALSE)"))}
  #if (!is(y, "integer")) stop(paste0("y should be integer"))
  if(!is(v, "numeric")){stop(paste0("v should be numeric"))}
  
  datatype <- toupper(datatype)
  
  if(!(datatype %in% c('INT1S', 'INT2S', 'INT4S', 'FLT4S', 'LOG1S', 'INT1U', 'INT2U', 'INT4U', 'FLT8S'))){
    stop('not a valid data type. Avalible are INT1S/INT2S/INT4S/FLT4S/LOG1S/INT1U/INT2U/INT4U/FLT8S')
  }
  
  if(!cblk%%1==0){stop(paste0("cblk should be integer"))}
  
  if(file.exists(filename) & overwrite==FALSE){
    stop(paste0("File ",filename," exist. Use option overwrite=TRUE"))
  }else{
    if(file.exists(filename)){file.remove(filename)}
  }
  
  stopifnot(hasValues(x))
  
  # get real physical cores in a computer
  max.cores <- parallel:::detectCores(logical = TRUE)
  
  if(is.null(cores)){
    cores <- max.cores - 1
  }
  if(cores > max.cores){
    stop(paste0("Number of cores ",cores," more then real physical cores in PC ",max.cores ))
  }
  if(is.null(minblk)){
    #minblk <- wpGetBlocksNeed(x,cores,n=cblk)
    minblk <-cores*4
  }
  
  beginCluster(n=cores)
  tStart <- Sys.time()
  
  blocks <- raster:::blockSize(x, chunksize = 500000, minblocks=cores*4)
  if(!silent){ 
    cat(paste0('\nTotal blocks ', blocks$n))
    cat('\n')
  }        
  
  cl <- getCluster()
  nodes <- length(cl)
  if(is.null(minblk)){
    minblk <- nodes
  }   
  
  clusterExport(cl, c("blocks", "x","y","v"), envir=environment())
  
  wpSetValue <- function(i){
    tryCatch({
      r.val <- raster:::getValues(x, row=blocks$row[i], nrows=blocks$nrows[i])
      nncol <- ncol(x)
      if (i==1){
        start.df <- 1
        end.df <- blocks$nrows[i]*nncol
      }else{
        start.df <- nncol*blocks$row[i] - nncol + 1
        end.df <- (nncol*blocks$row[i] + blocks$nrows[i]*nncol) - nncol
      }
      df <- data.frame(CellIndex = as.numeric(start.df:end.df) )
      df$v <- as.numeric(r.val)  
      #df[!is.na(df$v),"v"] <- 0
      df[df$CellIndex %in% y,"v"] <- v},
      error = function(e) stop(paste0("The block '", blocks$row[i], "'",
                                      " caused the error: '", e, "'")))
    return(df$v)
  }     
  
  for(i in 1:nodes){
    parallel:::sendCall(cl[[i]], wpSetValue, i, tag=i)
  }      
  
  out <- x
  out <- raster:::writeStart(out, 
                             filename=filename, 
                             format="GTiff", 
                             datatype=datatype, 
                             overwrite=overwrite, 
                             options=c("COMPRESS=LZW"),
                             NAflag=NAflag)     
  for(i in 1:blocks$n){
    d <- parallel:::recvOneData(cl)
    if(! d$value$success ){
      stop('cluster error')}
    tEnd <-  Sys.time()
    
    b <- d$value$tag
    if(!silent){ 
      wpProgressMessage(i, 
                        max=blocks$n, 
                        label=paste0("received block ",b, 
                                     " Processing Time: ",
                                     wpTimeDiff(tStart,tEnd)))}
    out <- raster:::writeValues(out, d$value$value, blocks$row[b])
    # need to send more data
    #
    ni <- nodes + i
    if (ni <= blocks$n) {
      parallel:::sendCall(cl[[d$node]], wpSetValue, ni, tag=ni)
    }
  }
  out <- raster:::writeStop(out)      
  endCluster()
  
  return(out)
}




##  END:  FUNCTION DEFINITIONS
####









####
##  DETERMINE MASK BASED UPON POPULATION THRESHOLD
##  This function sets up a task farm and, block by block, replaces values 
##  under or equal to the threshold with values of NA and values of 1 should 
##  they be greater than the threshold. Returns a raster of equal size and 
##  cell number, but with replaced values.
##  NOTE:  Make sure that the ppp has been converted to population densities of 
##         people per sq km prior to any mask creation.
if(make_mask){
  ##  Load the popualtion raster:
  pop_raster_path <- Sys.glob(paste0(pop_raster_dir,"Raw/ppp_2012*.tif"))[1]
  pop_ras <- raster(pop_raster_path)
  
  mask_ras <-pop_ras
  ##  NOTE:  YESSSS! 10 min to do it this way.
  print("Setting below threshold cells to NA...")
  print(paste0("     Start: ", Sys.time()))
  mask_ras[mask_ras < threshold_val] <- NA
  print(paste0("     End: ", Sys.time()))
  
  print("Setting above threshold cells to one...")
  print(paste0("     Start: ", Sys.time()))
  mask_ras[mask_ras >= threshold_val] <- 1
  print(paste0("     End: ", Sys.time()))
  
  
  mask_path <- paste0(root,"ThresholdMask_",threshold_val,"_",year,".tif")
  print("Writing Mask to File...")
  print(paste0("     Start: ", Sys.time()))
  writeRaster(mask_ras,
              file=mask_path,
              format="GTiff",
              datatype="INT2U",
              overwrite=T,
              options=c("COMPRESS=LZW"))
  print(paste0("     End: ", Sys.time()))
  ##  Get the indices of non-na pixels:
  ##  Note: Took about 1 min for 808704000 cells in a global raster.
  #nonna_idx <- which(!is.na(values(pop_ras)))
  
  
  ##  Get the values of the non-na pixels:
  ##  TODO: Parallelize a task farm?
  #nonna_vals <- extract(pop_ras, nonna_idx)
  ##  Remove our cells that fall below the threshold:
  ##  Start -- 15:06
  ##  End --   15:14
  # pop_ras[pop_ras < threshold_val] <- NA
  # 
  # 
  # ##  Create a logical vector resulting from the application of the threshold 
  # ##  inequality,  then use the logical vector to retrieve the corresponding cell 
  # ##  indices:
  # ##  Note:  About 1 minute to calculate
  # mask_idx <-  which(!is.na(values(pop_ras)))
  # 
  # ##  Create a new raster (a mask) of equal extent and resolution full of 0s:
  # ##  Create a raster of NAs:
  # tmp_ras <- raster(nrow=nrow(pop_ras), 
  #                   ncol= ncol(pop_ras),
  #                   ext = extent(pop_ras),
  #                   crs = crs(pop_ras))
  # values(tmp_ras) <- NA
  # 
  # 
  # 
  ##  Memory management:
  rm(pop_ras)
  gc()
  
  ##  Set all the values in the mask to zero:
  # wpSetAllValuesTo(tmp_ras,
  #                  v = 0,
  #                  filename = mask_path,
  #                  cores = core_number,
  #                  overwrite = T,
  #                  minblk = 10000,
  #                  datatype="INT2U",
  #                  silent = F)
  ##  Above could take as long as 15 minutes with default blocking; this is 
  ##  quicker at 1 min.
  #tmp_ras <- tmp_ras * 0
  
  ##  Replace the above determined cell indices with a value of 1:
  
  ##  JFC takes about 3 hours on UoS machine...
  # wpSetValueWhichindexes(tmp_ras, 
  #                        mask_idx,
  #                        1, 
  #                        filename = mask_path, 
  #                        minblk=1000,
  #                        overwrite = TRUE, 
  #                        cores = core_number,
  #                        silent = F)
}  
#rm(mask_idx, tmp_ras)
#gc()

##  END: DETERMINE MASK BASED UPON POPULATION THRESHOLD
####




####
##  DATA IMPORT RESCALE AND TRANSFORM  ----
##  NOTE: ALL USE LOG TRANSFORM EXCEPT THE URBAN PRP DATA WHICH DOESNT NEED 
##        RESCALING.
##  Load the mask:
mask_path <- paste0(root,"ThresholdMask_",threshold_val,"_2012.tif")
mask_ras <- raster(mask_path)

##  Population Rescale  ----
##  Load all the population rasters and record their min and max values:
max_vec <- c()
# sd_vec <- c()
min_vec <- c()
for(y in years){
  print(paste0("Working on pop distb. stats for year ",y,"..."))
  ##  Load the population raster:
  pop_raster_path <- Sys.glob(paste0(pop_raster_dir,"Raw/*",
                                     y,"*.tif"))[1]
  pop_ras <- raster(pop_raster_path)
  
  if(extent(pop_ras)!= extent(mask_ras)){
    ymin(pop_ras) <- ymin(mask_ras)
    ymax(pop_ras) <- ymax(mask_ras)
    xmin(pop_ras) <- xmin(mask_ras)
    xmax(pop_ras) <- xmax(mask_ras)
  }
  ##  Mask the population using the threshold mask:
  s_time <- Sys.time()
  print(paste0("Masking population ", y,"..."))
  pop_ras <- pop_ras * mask_ras
  pop_ras[pop_ras == 0] <- NA
  print(paste0("     Start: ", s_time,"     End: ",Sys.time()))
  
  if(use_log){
    #  Log transofrm the population prior to getting the descriptive stats:
    s_time <- Sys.time()
    print(paste0("Log transforming ", y,"..."))
    pop_ras <- log({pop_ras+1})
    print(paste0("     Start: ", s_time,"     End: ",Sys.time()))
  }
  ##  Write the log trans and masked pop ras to file:
  s_time <- Sys.time()
  print(paste0("Writing log transformed and masked population ",y,
               " to file..."))
  writeRaster(pop_ras,
              filename = paste0(pop_raster_dir,"Derived/ppkm_",y,
                                "_thresh_",threshold_val,
                                ifelse(use_log,"_LOG",""),
                                ".tif"),
              overwrite = T,
              datatype = "FLT8S",
              format = "GTiff",
              options=c("COMPRESS = LZW"))
  
  print(paste0("     Start: ", s_time,"     End: ",Sys.time()))
  ##  Calc and add the value statistics to the vector:
  max_vec <- c(max_vec, cellStats(pop_ras,stat="max",na.rm=T))
  # sd_vec <- c(sd_vec, cellStats(pop_ras,stat="sd",na.rm=T))
  min_vec <- c(min_vec, cellStats(pop_ras,stat="min",na.rm=T))
  
  rm(pop_ras)
}
sink(file=paste0(pop_raster_dir,"Derived/Population_Min_Max.txt"))
print(data.frame(YEAR = years, 
                 POP_MAX = max_vec,
                 # POP_SD = sd_vec)) #,
                 POP_MIN = min_vec))
sink()
##  Take the max of the maximums and set the minimum value:
pop_max <- max(max_vec, na.rm = T)
# pop_sd <- max(sd_vec, na.rm = T)
pop_min <- min(min_vec,na.rm=T)

##  Store the range of values so we aren't recalculating it many times below.
pop_range <- pop_max - pop_min

for(year in years){
  ##  Load the population raster:
  pop_raster_path <- paste0(pop_raster_dir,"Derived/ppkm_",year,
                            "_thresh_",threshold_val,
                            ifelse(use_log,"_LOG",""),
                            ".tif")
  pop_ras <- raster(pop_raster_path)
  
  
  # if(extent(pop_ras)!= extent(mask_ras)){
  #   ymin(pop_ras) <- ymin(mask_ras)
  #   ymax(pop_ras) <- ymax(mask_ras)
  #   xmin(pop_ras) <- xmin(mask_ras)
  #   xmax(pop_ras) <- xmax(mask_ras)
  # }
  # ##  Multiply by the mask to retain only the original values of interest:
  # ##  start 9:38
  # ##  End 9:40
  # s_time <- Sys.time()
  # print(paste0("Masking population ", year,"..."))
  # pop_ras <- pop_ras * mask_ras
  # print(paste0("     Start: ", s_time,"     End: ",Sys.time()))
  
  
  # s_time <- Sys.time()
  # print(paste0("Log transforming ", year,"..."))
  # pop_ras <- log(pop_ras)
  # print(paste0("     Start: ", s_time,"     End: ",Sys.time()))
  
  
  ##  Rescale the values:
  ##  Start 9:41
  ##  End 9:44
  s_time <- Sys.time()
  print(paste0("Rescaling population ",year,"..."))
  
  pop_rescale_ras <- {pop_ras-pop_min}/pop_range
  print(paste0("     Start: ", s_time,"     End: ",Sys.time()))
  print(pop_rescale_ras)
  ##  Write to file:
  ##  Start 9:44
  ##  End  9:47
  s_time <- Sys.time()
  print(paste0("Writing rescaled population ",year," to file..."))
  writeRaster(pop_rescale_ras,
              filename = paste0(pop_raster_dir,"Derived/ppkm_",year,
                                "_rescaled_thresh_",threshold_val,
                                ifelse(use_log,"_LOG",""),
                                ".tif"),
              overwrite = T,
              datatype = "FLT8S",
              format = "GTiff",
              options=c("COMPRESS = LZW"))
  print(paste0("     Start: ", s_time,"     End: ",Sys.time()))
  
  rm(pop_ras, pop_rescale_ras)
  gc()
}

##  Option: Check of values of those rasters:
for(y in years){
  print(raster(paste0(pop_raster_dir,"Derived/ppkm_",y,
                      "_rescaled_thresh_",threshold_val,
                      ifelse(use_log,"_LOG",""),
                      ".tif")))
}


##  Built/Urban Rescale  ----
##  NOTE:  JJN 2020-03-23  I don't think I actually need to rescale this because
##         it already falls between 0 and 1. Therefore I don't need to log 
##         transform it( to use the +-2SD approximation of maximum) to then 
##         rescale to zero and 1 like with the population data
max_vec <- c()
# sd_vec <- c()
min_vec <- c()
##  Pull the mask indices:
for(y in years){
  print(paste0("Working on urb density stats for year ",y,"..."))
  ##  Load the urbdensn raster:
  urbdens_raster_path <- paste0(urbdens_raster_dir,"Derived/BS_",
                                y,"_1km_PRP.tif")
  urbdens_ras <- raster(urbdens_raster_path)
  ##  Resample the data because they don't truely have 0.008333333 resolution, 
  ##  in fact the x and y resolutions don't even match:
  if(!file.exists(paste0(urbdens_raster_dir,"Derived/","BS_",y,"_1km_PRP_corr.tif"))&
     {res(urbdens_ras)[1]!=res(urbdens_ras)[2]}){
    t_start <- Sys.time()
    print(paste0("Resampling the BS PRP to uniform grid..."))
    print(paste0("    Start: ", t_start))
    resample(urbdens_ras,
             mask_ras,
             method="bilinear",
             filename = paste0(urbdens_raster_dir,"Derived/","BS_",y,
                               "_1km_PRP_corr.tif"),
             format="GTiff",
             datatype=dataType(urbdens_ras),
             overwrite=T,
             options=c("COMPRESS=LZW"))
    print(paste0("    End: ", Sys.time()))
    urbdens_ras <- raster(paste0(urbdens_raster_dir,"Derived/","BS_",y,
                                 "_1km_PRP_corr.tif"))
  }
  
  if(extent(urbdens_ras)!= extent(mask_ras)){
    ymin(urbdens_ras) <- ymin(mask_ras)
    ymax(urbdens_ras) <- ymax(mask_ras)
    xmin(urbdens_ras) <- xmin(mask_ras)
    xmax(urbdens_ras) <- xmax(mask_ras)
  }
  ##  Mask the urbdens using the threshold mask:
  s_time <- Sys.time()
  print(paste0("Masking urb dens ", y,"..."))
  urbdens_ras <- urbdens_ras * mask_ras
  print(paste0("     Start: ", s_time,"     End: ",Sys.time()))
  
  
  if(use_log){
    #  Log transofrm the urbdens prior to getting the descriptive stats:
    s_time <- Sys.time()
    print(paste0("Log transforming ", y,"..."))
    urbdens_ras <- log({urbdens_ras+0.01})
    print(paste0("     Start: ", s_time,"     End: ",Sys.time()))
  }
  ##  Write the log trans and masked pop ras to file:
  s_time <- Sys.time()
  print(paste0("Writing ", ifelse(use_log,"log transformed and",""),
               " masked urbdens ",y,
               " to file..."))
  writeRaster(urbdens_ras,
              filename = paste0(urbdens_raster_dir,"Derived/urbdens_",y,
                                "_thresh_",threshold_val,
                                ifelse(use_log,"_LOG",""),
                                ".tif"),
              overwrite = T,
              datatype = "FLT4S",
              format = "GTiff",
              options=c("COMPRESS = LZW"))
  
  print(paste0("     Start: ", s_time,"     End: ",Sys.time()))
  
  
  ##  Calc and add the value statistics to the vector:
  max_vec <- c(max_vec, cellStats(urbdens_ras,stat="max",na.rm=T))
  # sd_vec <- c(sd_vec, cellStats(urbdens_ras,stat="sd",na.rm=T))
  min_vec <- c(min_vec, cellStats(urbdens_ras,stat="min",na.rm=T))
  
  rm(urbdens_ras)
}
sink(file=paste0(urbdens_raster_dir,"Derived/UrbDensity_Min_Max.txt"))
print(data.frame(YEAR = years,
                 URB_MAX = max_vec,
                 # URB_SD = sd_vec,
                 URB_MIN = min_vec))
sink()
##  Take the max of the maximums and set the minimum value:
urbdens_max <- max(max_vec, na.rm = T)
# pop_sd <- max(sd_vec, na.rm = T)
urbdens_min <- min(min_vec,na.rm=T)

##  Store the range of values so we aren't recalculating it many times below.
urbdens_range <- urbdens_max - urbdens_min

for(year in years){
  ##  Load the urbdens raster:
  urbdens_raster_path <- paste0(urbdens_raster_dir,"Derived/urbdens_",year,
                                "_thresh_",threshold_val,
                                ifelse(use_log,"_LOG",""),
                                ".tif")
  urbdens_ras <- raster(urbdens_raster_path)
  
  
  s_time <- Sys.time()
  print(paste0("Rescaling urbdens ",year,"..."))
  
  urbdens_rescale_ras <- {urbdens_ras-urbdens_min}/urbdens_range
  print(paste0("     Start: ", s_time,"     End: ",Sys.time()))
  print(urbdens_rescale_ras)
  ##  Write to file:
  ##  Start 9:44
  ##  End  9:47
  s_time <- Sys.time()
  print(paste0("Writing rescaled urbdens ",year," to file..."))
  writeRaster(urbdens_rescale_ras,
              filename = paste0(urbdens_raster_dir,"Derived/urbdens_",year,
                                "_rescaled_thresh_",threshold_val,
                                ifelse(use_log,"_LOG",""),
                                ".tif"),
              overwrite = T,
              datatype = "FLT8S",
              format = "GTiff",
              options=c("COMPRESS = LZW"))
  print(paste0("     Start: ", s_time,"     End: ",Sys.time()))
  
  rm(urbdens_ras, urbdens_rescale_ras)
  gc()
}

##  Option: Check of values of those rasters:
for(y in years){
  print(raster(paste0(urbdens_raster_dir,"Derived/urbdens_",year,
                      "_rescaled_thresh_",threshold_val,
                      ifelse(use_log,"_LOG",""),
                      ".tif")))
} 


##  Lights at Night Rescale  ----
max_vec <- c()
#sd_vec <- c()
min_vec <- c()
light_threshold <- 7
for(y in years){
  print(paste0("Working on LAN stats for year ",y,"..."))
  ##  Resample the data because they don't truely have 0.008333333 resolution, 
  ##  in fact the x and y resolutions don't even match:
  if(!file.exists(paste0(lan_raster_dir,"Derived/","Harmonized_DN_NTL_",y,
                         "_resampled.tif"))){
    ##  Load the LAN raster:
    lan_raster_path <-  Sys.glob(paste0(lan_raster_dir,"Raw/","Harmonized_DN_NTL_",
                                        y,"*.tif"))[1]
    lan_ras <- raster(lan_raster_path)
    t_start <- Sys.time()
    print(paste0("Resampling the night-time lights to uniform grid..."))
    print(paste0("    Start: ", t_start))
    resample(lan_ras,
             mask_ras,
             method="ngb",
             filename = paste0(lan_raster_dir,"Derived/","Harmonized_DN_NTL_",y,
                               "_resampled.tif"),
             format="GTiff",
             datatype=dataType(lan_ras),
             overwrite=T,
             options=c("COMPRESS=LZW"))
    print(paste0("    End: ", Sys.time()))
  }
  lan_raster_path <- paste0(lan_raster_dir,"Derived/","Harmonized_DN_NTL_",y,
                            "_resampled.tif")
  lan_ras <- raster(paste0(lan_raster_dir,"Derived/","Harmonized_DN_NTL_",y,
                           "_resampled.tif"))
  
  ##  If the number of cells is smaller than the mask raster, extend the extents 
  ##  with new cells filled with NA values:
  ##  NOTE:  This should also rectify the extents but we'll do an additional 
  ##  check below as well.
  if(ncell(lan_ras) < ncell(mask_ras)){
    t_start <- Sys.time()
    print(paste0("Extending the night-time lights to match masking extents..."))
    print(paste0("    Start: ", t_start))
    lan_ras <- extend(lan_ras,mask_ras, value = NA)
    print(paste0("    End: ", Sys.time()))
  }
  
  ##  Rectify extents if necessary:
  if(extent(lan_ras)!= extent(mask_ras)){
    ymin(lan_ras) <- ymin(mask_ras)
    ymax(lan_ras) <- ymax(mask_ras)
    xmin(lan_ras) <- xmin(mask_ras)
    xmax(lan_ras) <- xmax(mask_ras)
  }
  
  ##  Mask the plan using the threshold mask:
  s_time <- Sys.time()
  print(paste0("Masking LAN ", y,"..."))
  lan_ras <- lan_ras * mask_ras
  print(paste0("     Start: ", s_time,"     End: ",Sys.time()))
  
  
  ##  According to the datset providers, values of DNs less than 7 should not be
  ##  used due to stability issues so we will set all values less than 7 equal 
  ##  to zero:
  t_start <- Sys.time()
  print(paste0("Replacing low level lights with values of zero..."))
  print(paste0("    Start: ", t_start))
  ##  Retrieve the cell indices of cells within our threshold 
  ##  (should take one minute):
  foo_ind <- which(values(lan_ras)%in%seq(1, light_threshold,1))
  lan_ras[foo_ind] <- 0
  print(paste0("    End: ", Sys.time()))
  
  # ##  Log transofrm the lan prior to getting the descriptive stats:
  if(use_log){
    s_time <- Sys.time()
    print(paste0("Log transforming ", y,"..."))
    lan_ras <- log({lan_ras+1})
    print(paste0("     Start: ", s_time,"     End: ",Sys.time()))
  }
  ##  Write the log trans and masked lan ras to file:
  s_time <- Sys.time()
  print(paste0("Writing log transformed and masked LAN ",y,
               " to file..."))
  writeRaster(lan_ras,
              filename =paste0(lan_raster_dir,"Derived/lan_",y,
                               "_thresh_",threshold_val,
                               ifelse(use_log,"_LOG",""),
                               ".tif"),
              overwrite = T,
              datatype = "FLT8S",
              format = "GTiff",
              options=c("COMPRESS = LZW"))
  
  print(paste0("     Start: ", s_time,"     End: ",Sys.time()))
  
  ##  Calc and add the value statistics to the vector:
  max_vec <- c(max_vec, cellStats(lan_ras,stat="max",na.rm=T))
  #sd_vec <- c(sd_vec, cellStats(lan_ras,stat="sd",na.rm=T))
  min_vec <- c(min_vec, cellStats(lan_ras,stat="min",na.rm=T))
  
  rm(lan_ras)
}
sink(file=paste0(lan_raster_dir,"Derived/LAN_Min_Max.txt"))
print(data.frame(YEAR = years,
                 LAN_MAX = max_vec,
                 #LAN_SD = sd_vec,
                 LAN_MIN = min_vec))
sink()



##  Take the max of the max and the min of the mins to capture the largest 
##  range:
lan_max <- max(max_vec, na.rm = T)
#lan_sd <- max(sd_vec, na.rm = T)
lan_min <- min(min_vec,na.rm=T)
#if(is.infinite(lan_min) ){lan_min <- 0.0001}

##  Store the range of values so we aren't recalculating it many times below.
lan_range <- lan_max - lan_min

for(year in years){
  ##  Load the LAN raster:
  lan_raster_path <-  paste0(lan_raster_dir,"Derived/lan_",year,
                             "_thresh_",threshold_val,
                             ifelse(use_log,"_LOG",""),
                             ".tif")
  lan_ras <- raster(lan_raster_path)
  
  # if(extent(lan_ras)!= extent(mask_ras)){
  #   ymin(lan_ras) <- ymin(mask_ras)
  #   ymax(lan_ras) <- ymax(mask_ras)
  #   xmin(lan_ras) <- xmin(mask_ras)
  #   xmax(lan_ras) <- xmax(mask_ras)
  # }
  # 
  # s_time <- Sys.time()
  # print(paste0("Masking LAN ", year,"..."))
  # ##  Multiply by the mask to retain only the original values of interest:
  # lan_ras <- lan_ras * mask_ras
  # print(paste0("     Start: ", s_time,"     End: ",Sys.time()))
  
  # if(use_log){
  #   s_time <- Sys.time()
  #   print(paste0("Log transforming ", year,"..."))
  #   lan_ras <- log(lan_ras)
  #   print(paste0("     Start: ", s_time,"     End: ",Sys.time()))
  #   }
  
  s_time <- Sys.time()
  print(paste0("Rescale LAN ", year,"..."))
  ##  Rescale the values: 
  
  lan_rescale_ras <- {lan_ras-lan_min}/lan_range
  print(paste0("     Start: ", s_time,"     End: ", Sys.time()))
  
  s_time <- Sys.time()
  print(paste0("Writing rescaled LAN ",year," to file..."))
  ##  Write to file:
  writeRaster(lan_rescale_ras,
              filename = paste0(lan_raster_dir,"Derived/lan_",year,
                                "_rescaled_thresh_",threshold_val,
                                ifelse(use_log,"_LOG",""),
                                ".tif"),
              overwrite = T,
              datatype = "FLT8S",
              format = "GTiff",
              options=c("COMPRESS = LZW"))
  print(paste0("     Start: ", s_time,"     End: ", Sys.time()))
  rm(lan_ras, lan_rescale_ras)
  gc()
}

##  Option: Check of values of those rasters:
for(y in years){
  print(raster(paste0(lan_raster_dir,"Derived/lan_",y,
                      "_rescaled_thresh_",threshold_val,
                      ifelse(use_log,"_LOG",""),
                      ".tif")))
}

##  END:  DATA IMPORT RESCALE AND TRANSFORM
####




####  STACKING AND BRICKING TO DISK
##    ----
for(year in years){
  s_time <- Sys.time()
  print(paste0("Loading rasters for ", year,"..."))
  pop_rescale_lyr <- paste0(pop_raster_dir,"Derived/ppkm_",year,
                            "_rescaled_thresh_",threshold_val,
                            ifelse(use_log,"_LOG",""),
                            ".tif")
  pop_rescale <- raster(pop_rescale_lyr)
  urbdens_rescale_lyr <- paste0(urbdens_raster_dir,"Derived/urbdens_",year,
                                "_rescaled_thresh_",threshold_val,
                                ifelse(use_log,"_LOG",""),
                                ".tif")
  urbdens_rescale <- raster(urbdens_rescale_lyr)
  lan_rescale_lyr <- paste0(lan_raster_dir,"Derived/lan_",year,
                            "_rescaled_thresh_",threshold_val,
                            ifelse(use_log,"_LOG",""),
                            ".tif")
  lan_rescale <- raster(lan_rescale_lyr)
  
  print(paste0("     Start: ", s_time,"     End: ", Sys.time()))
  
  
  s_time <- Sys.time()
  print(paste0("     Aligning and Cropping Extents..."))
  my_extent <- extent(pop_rescale)
  
  for (var_name in list(pop_rescale, urbdens_rescale, lan_rescale)) {
    assign("tmp_raster", var_name)
    if (xmin(tmp_raster) > xmin(my_extent)) { my_extent@xmin <- xmin(tmp_raster) }
    if (xmax(tmp_raster) < xmax(my_extent)) { my_extent@xmax <- xmax(tmp_raster) }
    if (ymin(tmp_raster) > ymin(my_extent)) { my_extent@ymin <- ymin(tmp_raster) }
    if (ymax(tmp_raster) < ymax(my_extent)) { my_extent@ymax <- ymax(tmp_raster) }
  }
  
  ##	Re-set the extent based on the sometimes slightly different extent
  ##		post-cropping:
  pop_rescale <- crop(pop_rescale, my_extent)
  urbdens_rescale <- crop(urbdens_rescale, my_extent)
  lan_rescale <- crop(lan_rescale,my_extent)
  
  extent(pop_rescale) <- my_extent
  extent(urbdens_rescale) <- my_extent
  extent(lan_rescale) <- my_extent
  print(paste0("     Start: ", s_time,"     End: ", Sys.time()))
  
  
  s_time <- Sys.time()
  print(paste0("     Stacking rasters..."))
  
  rescale_stack <- stack(list(pop_rescale,urbdens_rescale,lan_rescale),
                         varname = c("ppp","urbdens","lan"))
  print(paste0("     Start: ", s_time,"     End: ", Sys.time()))
  
  
  s_time <- Sys.time()
  print(paste0("     Writing stack to file..."))
  writeRaster(rescale_stack,
              filename = paste0("E:/Research/Global_Relative_Inequalities/Output/",
                                "ppkm_urb_lan_rescale_stack_",year,"_threshold_",
                                threshold_val,
                                ifelse(use_log,"_LOG",""),
                                ".tif"),
              overwrite = T,
              datatype = "FLT8S",
              format = "GTiff",
              options=c("COMPRESS = LZW"))
  print(paste0("     Start: ", s_time,"     End: ", Sys.time()))
  rm(pop_rescale,urbdens_rescale,lan_rescale,rescale_stack)
  gc()
}



##  END:  STACKING AND BRICKING TO DISK
####