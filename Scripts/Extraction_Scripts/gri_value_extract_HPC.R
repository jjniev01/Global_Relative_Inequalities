require(raster)
require(data.table)
require(snow)

# R script to extract the values of the three layered (population - urban - nighttime lights)
# rescaled value raster. Outputs a .RDS file (class of contained object is a data.table) with the extracted
# non-NA pixel values for the given country. The R script called below can be modified to use any zonal raster
# but currently has the level 0 national boundaries (as defined by the worldpop geospatial library)
# hardcoded into the script.

##  Local parameters
# root <- "E:/Research/Global_Relative_Inequalities/"
# outdir <- paste0(root,"Output/Extracted_Values/")
# 
# 
# ##  If the file already exists do we wish to overwrite?
# year <- 2003
# core_number <- 4
# #10
# threshold_val <- 5
# #5
# 
# make_sum <- F
# 
# ##  If the file already exists do we wish to overwrite?
# overwrite <- F
root <- "/mainfs/scratch/jjn1n15/GRI/"
outdir <- paste0(root,"Output/Extracted_Values/")

args <- commandArgs(TRUE)

##  If the file already exists do we wish to overwrite?
year <- as.numeric(eval(parse(text=args[1])))
core_number <- as.numeric(eval(parse(text=args[2])))
#10
threshold_val <- as.numeric(eval(parse(text=args[3])))
#5

make_sum <- as.logical(eval(parse(text=args[4])))

##  If the file already exists do we wish to overwrite?
overwrite <- as.logical(eval(parse(text=args[5])))

iso_exclude <- as.numeric(eval(parse(text=args[6])))

cat(sprintf('YEAR: %d \nCORES: %d\nTHRESHOLD: %d\nMAKESUM: %s\nOVERWRITE: %s\nISO EXCLUDE: %s',
            year, core_number, threshold_val, make_sum, overwrite, iso_exclude))




##  GENERAL FUNCTION DEFINITIONS -----
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





extractPrll <- function(i){
  ##  Get the unique GID:
  g <- iso_codes[i]
  
  
  foo_values <- extract(value_ras,
                        which(values(zonal_ras)==g))
  foo_dt <- data.table("VALUE_1" = foo_values[,1],
                       "VALUE_2" = foo_values[,2],
                       "VALUE_3" = foo_values[,3])
  foo_dt[,"VALUE_TOT" := {VALUE_1+VALUE_2+VALUE_3}]
  foo_dt[,NA_Flag := {is.na(VALUE_1)&is.na(VALUE_2)&
      is.na(VALUE_3)&is.na(VALUE_TOT)}]
  foo_dt <- copy(foo_dt[NA_Flag == FALSE])
  
  return(foo_dt)
}





##  DATA IMPORTATION  ----
##  Declare where our 1km L0 (country level) zonal raster is:
zonal_ras_path <- paste0(root, "Data/L0_Zonal_1km/",
                         "RasterMask_L0_1km.tif")
zonal_ras <- raster(zonal_ras_path)

##  Retrieve our unique country codes:
iso_df <- read.csv(paste0(root,
                          "Data/L0_Zonal_1km/",
                          "Mastergrid countrycodeID with UN continents_BETA.csv"),
                   stringsAsFactors = F)

iso_codes <- unique(iso_df$ISO_number)

##  Remove some of the larger countries so we are not trying to run them in 
##  parallel across shared memory and save them for last, i.e. USA, Russia, 
##  Canada, Brazil, Argentina, Chile:
non_shared_isos <- c(124, 643, 840, 76, 32, 152)
iso_codes <- iso_codes[!{iso_codes %in% non_shared_isos}]
##  Remove some countries that are not relevant (i.e. Antarctica):
iso_codes <- iso_codes[!{iso_codes %in% c(10, 900, 901,
                                          74, 86, 260, 
                                          239, 334, 612,
                                          744, 581)}]
if(!is.null(iso_exclude)){
  iso_codes <- iso_codes[!{iso_codes %in% iso_exclude}]
}
##  Repaste the bigs ones on the end:
iso_codes <- c(iso_codes, non_shared_isos)
iso_codes <- iso_codes[!is.na(iso_codes)]

##  Filter our isocode list to only those we wish to process or overwrite:
iso_logic <- logical(length=length(iso_codes))
for(t in 1:length(iso_codes)){
  g <- iso_codes[t]
  if(!file.exists(paste0(outdir,"GRI_",g,"_",year,
                         "_threshold_",threshold_val,".RDS")) | overwrite){
    iso_logic[t] <- TRUE 
  }else{
    iso_logic[t] <- FALSE
  }
}
iso_codes <- iso_codes[iso_logic]




##  TASK FARM CREATION  ----
clusterExtract <- function(zonal_ras,
                           value_ras,
                           ...){
  ##  Description:
  ##
  ##  Parameters
  ##  -  gini_dt: data.table holding both the ISO codes of all countries we are
  ##              examining as well as the calculated values
  ##  -  zonal_ras: Raster representing the zones that we wish to calculate Gini
  ##                indices for
  ##  -  value_ras: three layered raster for which we want to calculated Gini 
  ##                coefficients and metrics for
  ##  Values
  ##
  ##
  ##  ---------------------------------------------------------------------  ##
  ##	Start the timer:
  tStart <- Sys.time()
  
  ##	Pull the cluster:
  cl <- getCluster()
  on.exit( returnCluster() )
  
  ##	Determine the number of cores we're working with:
  if(length(cl)>length(iso_codes)){
    nodes <- length(iso_codes)}else{
      nodes <- length(cl)}
  
  ##	Pass off required libraries and data to the cluster workers:
  clusterEvalQ(cl, {
    require(raster)
    require(data.table)
  })
  ##  Pass off the required data and functions to the nodes in the cluster
  ##   - this includes the list of lists used for informing predictions, and the
  ##     task function which creates the predictions for each admin unit:
  
  clusterExport(cl, c("zonal_ras",
                      "value_ras",
                      "extractPrll",
                      "year",
                      "threshold_val",
                      "outdir",
                      "iso_codes",
                      "root"))
  
  
  
  ##	Start all nodes on a prediction:
  for (i in 1:nodes) {
    ##  Send the taskMaker function call to the ith node with ith task from
    ##  the prediction_list as an argument and tag it with the value i
    sendCall(cl[[i]], extractPrll, i, tag=i)
  }
  
  
  ##	Create our primary cluster processing loop, recalling that we already
  ##		have clusters running:
  cat("Total tasks to process: ", length(iso_codes), "\n")
  for (i in 1:length(iso_codes)) {
    ##	Receive results from a node:
    predictions <- recvOneData(cl)
    
    ##	Check if there was an error:
    if (!predictions$value$success) {
      stop("ERROR: Cluster barfed...\n\n", predictions)
    }
    
    ##	Which block are we processing:
    block <- predictions$value$tag
    #cat("Received block: ", block, "\n")
    #flush.console()
    
    saveRDS(predictions$value$value,
            file = paste0(outdir,"GRI_",iso_codes[predictions$value$tag],"_",year,
                          "_threshold_",threshold_val,".RDS"))
    
    
    
    ##	Check to see if we are at the end of our tasklist:
    ni <- nodes + i
    if (ni <= length(iso_codes)) {
      ##	And if not, send it to the cluster node that just gave us
      ##		our last result...
      sendCall(cl[[predictions$node]], extractPrll, ni, tag=ni)
    }
    tEnd <- Sys.time()
    wpProgressMessage(i,
                      max = length(iso_codes),
                      label = paste0("Received chunk ", ni,
                                     " Processing Time: ",
                                     wpTimeDiff(tStart,tEnd))
    )
  }
}




value_ras <- brick(paste0(root,"Output/",
                           "ppkm_urb_lan_rescale_stack_",year,"_threshold_",
                           threshold_val,".tif"))




s_time <- Sys.time()
print(paste0("Start time: ", s_time))
beginCluster(n = core_number)
clusterExtract(zonal_ras,value_ras)
endCluster()