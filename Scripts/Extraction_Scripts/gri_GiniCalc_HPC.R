require(raster)
require(sf)
require(data.table)
require(reldist)
require(snow)

# Submission script to extract the values of the three layered (population - urban - nighttime lights)
# rescaled value raster. Outputs a .RDS file (class of contained object is a data.table) with the extracted
# non-NA pixel values for the given country. The R script called below can be modified to use any zonal raster
# but currently has the level 0 national boundaries (as defined by the worldpop geospatial library)
# hardcoded into the script.

args <- commandArgs(TRUE)

#root <- "E:/Research/Global_Relative_Inequalities/"
root <- "/mainfs/scratch/jjn1n15/GRI/"
outdir <- paste0(root, "Output/")
reprocess <- F
backtrans <- F
year <- as.numeric(eval(parse(text=args[1])))

core_number <- as.numeric(eval(parse(text=args[2])))
#12
threshold_val <- as.numeric(eval(parse(text=args[3])))
#5
make_sum <- as.logical(eval(parse(text=args[4])))
#F

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




giniCalc <- function(i){
  ##  Description:  
  ##
  ##  Parameters:
  ##  -  Index between 1 and the number of countries whose values we want to 
  ##     extract and calculate metrics for.
  ##
  ##  Values:
  ##  -  List of values to return to the main handler for merging with the 
  ##     main data.table of values
  ##
  ##  --------------------------------------------------------------------- ##
  ##  Get the geographic ID (ISOCODE) of the country we are working with:
  g <- gini_dt[i]$ISO_number
  
    ##  Extract the values from the value raster:
  foo_values <- extract(value_ras,
                        which(values(zonal_ras)==g))
  ##  IF the extraction didn't come up null:
  if(!is.null(foo_values)){
    ##  For each of the layer-columns:
    for(l in 1:3){
      ## Remove any NA values:
      foo_slice <- foo_values[!is.na(foo_values[,l]),l]
      if(!is.null(foo_slice)){
        ##  Sort in ascending order:
        foo_slice <- foo_slice[order(foo_slice)]
        
        foo_gini <- gini(foo_slice)
        foo_quant <- quantile(foo_slice,
                              probs = c(0.25,0.5,0.75))
        foo_25 <- as.numeric(foo_quant[1])
        foo_50 <- as.numeric(foo_quant[2])
        foo_75 <- as.numeric(foo_quant[3])
      }else{
        foo_gini <- NA
        foo_quant <- NA
        foo_25 <- NA
        foo_50 <- NA
        foo_75 <- NA
      }
      ##  Store desired metrics:
      if(l==1){
        pop_gini <- foo_gini
        pop_25 <- foo_25
        pop_50 <- foo_50
        pop_75 <- foo_75
      }
      if(l==2){
        urb_gini <- foo_gini
        urb_25 <- foo_25
        urb_50 <- foo_50
        urb_75 <- foo_75
      }
      if(l==3){
        lan_gini <- foo_gini
        lan_25 <- foo_25
        lan_50 <- foo_50
        lan_75 <- foo_75
      }
    }
    rm(foo_slice)
    gc()
    
    ##  Calculate our sums across all the layers:
    foo_tot_values <- rowSums(foo_values)
    ##  As long as the total values didn't come up null:
    if(!is.null(foo_tot_values)){  
      ##  Remove NA values:
      foo_tot_values <- foo_tot_values[!is.na(foo_tot_values)]
      
      ##  Sort in ascending order:
      foo_tot_values <- foo_tot_values[order(foo_tot_values)]
      
      foo_tot_gini <- gini(foo_tot_values)
      foo_tot_quant <- quantile(foo_tot_values,
                                probs = c(0.25,0.5,0.75))
      foo_tot_25 <- as.numeric(foo_tot_quant[1])
      foo_tot_50 <- as.numeric(foo_tot_quant[2])
      foo_tot_75 <- as.numeric(foo_tot_quant[3])
    }
  }else{
      ##  IF foo_values is completely null then we set everything to NA
      pop_gini <- NA
      pop_25 <- NA
      pop_50 <- NA
      pop_75 <- NA
      
      urb_gini <- NA
      urb_25 <- NA
      urb_50 <- NA
      urb_75 <- NA
      
      
      lan_gini <- NA
      lan_25 <- NA
      lan_50 <- NA
      lan_75 <- NA
      
      
      foo_tot_gini <- NA
      foo_tot_quant <- NA
      foo_tot_25 <- NA
      foo_tot_50 <- NA
      foo_tot_75 <- NA
      
    }
    
  rm(foo_tot_values)
  gc()
  

  ##  Store the admin_ind and the corresponding probabilities in a list
  ##  within a listthe list under the character representation of the admin
  ##  id so we can retrieve them in our chunking of tasks:
  val_list <- list(g,
                   year,
                   pop_gini,pop_25,pop_50,pop_75,
                   urb_gini,urb_25,urb_50,urb_75,
                   lan_gini,lan_25,lan_50,lan_75,
                   tot_gini,tot_25,tot_50,tot_75)
  
  
  return(val_list)
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

##  Remove some countries that are not relevant (i.e. Antarctica):
iso_codes <- iso_codes[!{iso_codes %in% c(10, 900, 901,
                                          74, 86, 260, 
                                          239, 334, 612,
                                          744, 581)}]

  
  
  
##  If we need to back transform the rasters first:
if(backtrans & 
   !file.exists(paste0("E:/Research/Global_Relative_Inequalities/Output/",
                       "ppkm_urb_lan_rescale_stack_",year,"_threshold_",
                       threshold_val,".tif"))){
  s_time <- Sys.time()
  print(paste0("Back transforming population..."))
  pop_ras <- raster(paste0(root, 
                           "Output/",
                           "ppkm_urb_lan_rescale_stack_2012_threshold_5_LOG.tif"),
                    band = 1)
  pop_ras <- exp(pop_ras)-1
  print(pop_ras)
  print(paste0("     Start: ", s_time,"     End: ", Sys.time()))
  
  
  s_time <- Sys.time()
  print(paste0("Back transforming urban proportion..."))
  urb_ras <- raster(paste0(root, 
                           "Output/",
                           "ppkm_urb_lan_rescale_stack_2012_threshold_5_LOG.tif"),
                    band = 2)
  urb_ras <- exp(urb_ras)-0.01
  print(urb_ras)
  print(paste0("     Start: ", s_time,"     End: ", Sys.time()))
  
  
  s_time <- Sys.time()
  print(paste0("Back transforming lights-at-night..."))
  lan_ras <- raster(paste0(root, 
                           "Output/",
                           "ppkm_urb_lan_rescale_stack_2012_threshold_5_LOG.tif"),
                    band = 3)
  lan_ras <- exp(lan_ras)-1
  print(lan_ras)
  print(paste0("     Start: ", s_time,"     End: ", Sys.time()))
  
  
  s_time <- Sys.time()
  print(paste0("Restacking back-transformed rasters..."))
  rescale_stack <- stack(list(pop_ras,urb_ras,lan_ras),
                         varname = c("ppp","urbdens","lan"))
  print(paste0("     Start: ", s_time,"     End: ", Sys.time()))
  
  
  tot_ras <- pop_ras + lan_ras + urb_ras
  
  s_time <- Sys.time()
  print(paste0("     Writing stack to file..."))
  writeRaster(rescale_stack,
              filename = paste0("E:/Research/Global_Relative_Inequalities/Output/",
                                "ppkm_urb_lan_rescale_stack_",year,"_threshold_",
                                threshold_val,".tif"),
              overwrite = T,
              datatype = "FLT8S",
              format = "GTiff",
              options=c("COMPRESS = LZW"))
  writeRaster(tot_ras,
              filename = paste0("E:/Research/Global_Relative_Inequalities/Output/",
                                "ppkm_urb_lan_rescale_stack_",year,"_threshold_",
                                threshold_val,"_SUM",".tif"),
              overwrite = T,
              datatype = "FLT8S",
              format = "GTiff",
              options=c("COMPRESS = LZW"))
  rm(rescale_stack,
     pop_ras,
     urb_ras,
     lan_ras,
     tot_ras)
}




if(make_sum & !file.exists(paste0(root,"Output/",
                                  "ppkm_urb_lan_rescale_stack_",year,
                                  "_threshold_",
                                  threshold_val,"_SUM",".tif"))){
  foo_ras_1 <- raster(paste0(root,"Output/",
                             "ppkm_urb_lan_rescale_stack_",year,"_threshold_",
                             threshold_val,".tif"),
                      band = 1)
  foo_ras_2 <- raster(paste0(root,"Output/",
                             "ppkm_urb_lan_rescale_stack_",year,"_threshold_",
                             threshold_val,".tif"),
                      band = 2)
  foo_ras_3 <- raster(paste0(root,"Output/",
                             "ppkm_urb_lan_rescale_stack_",year,"_threshold_",
                             threshold_val,".tif"),
                      band = 3)
  tot_ras <- foo_ras_1 + foo_ras_2 + foo_ras_3
  writeRaster(tot_ras,
              filename = paste0(root,"Output/",
                                "ppkm_urb_lan_rescale_stack_",
                                year,"_threshold_",
                                threshold_val,"_SUM",".tif"),
              format = "GTiff",
              datatype = "FLT8S",
              overwrite = T,
              options = c("COMPRESS = LZW"))
  # stackApply(value_ras,
  #                     indices = c(1,1,1),
  #                     fun = "sum",
  #                     na.rm = F,
  # filename = paste0(root,"Output/",
  #                   "ppkm_urb_lan_rescale_stack_",
  #                   year,"_threshold_",
  #                   threshold_val,"_SUM",".tif"),
  # format = "GTiff",
  # datatype = dataType(value_ras),
  # overwrite = T,
  # options = c("COMPRESS = LZW"))
  rm(foo_ras_1, foo_ras_2, foo_ras_3, tot_ras)
}
gc()




##  TASK FARM CREATION  ----
##  TASK FARM CREATION  ----
clusterGini <- function(gini_dt,
                        zonal_ras,
                        value_ras,
                        # value_tot_ras,
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
  nodes <- length(cl)
  
  ##	Pass off required libraries and data to the cluster workers:
  clusterEvalQ(cl, {
    require(raster)
    require(reldist)
    require(data.table)
  })
  ##  Pass off the required data and functions to the nodes in the cluster
  ##   - this includes the list of lists used for informing predictions, and the
  ##     task function which creates the predictions for each admin unit:
  
  clusterExport(cl, c("gini_dt",
                      "zonal_ras",
                      "value_ras",
                      # "value_tot_ras",
                      "giniCalc"))
  
  
  
  ##	Start all nodes on a prediction:
  for (i in 1:nodes) {
    ##  Send the taskMaker function call to the ith node with ith task from
    ##  the prediction_list as an argument and tag it with the value i
    sendCall(cl[[i]], giniCalc, i, tag=i)
  }
  
  
  ##	Create our primary cluster processing loop, recalling that we already
  ##		have clusters running:
  cat("Total tasks to process: ", nrow(gini_dt), "\n")
  for (i in 1:nrow(gini_dt)) {
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
    
    ##	Now store our task item (list of three objects) in our task list:
    gini_dt[ISO_number == predictions$value$value[[1]],
            names(gini_dt):= predictions$value$value]
    
    
    ##	Check to see if we are at the end of our tasklist:
    ni <- nodes + i
    if (ni <= nrow(gini_dt)) {
      ##	And if not, send it to the cluster node that just gave us
      ##		our last result...
      sendCall(cl[[predictions$node]], giniCalc, ni, tag=ni)
    }
    tEnd <- Sys.time()
    wpProgressMessage(i,
                      max = nrow(gini_dt),
                      label = paste0("Received chunk ", ni,
                                     " Processing Time: ",
                                     wpTimeDiff(tStart,tEnd))
    )
  }
  
  ##  Return the cluster transition vector so we can change our raster map:
  return(gini_dt)
}



##  Preallocate a data table to hold our Gini coefficient data and such:
gini_dt <- data.table("ISO_number" = iso_codes,
                      "YEAR" = numeric(length = length(iso_codes)),
                      ##  Gini coefficient
                      "Gini_pop" = numeric(length = length(iso_codes)),
                      ##  25th percentile
                      "P25_pop" = numeric(length = length(iso_codes)),
                      ##  50th percentile
                      "P50_pop" = numeric(length = length(iso_codes)),
                      ##  75th percentile
                      "P75_pop" = numeric(length = length(iso_codes)),
                      ##  Gini coefficient
                      "Gini_urb" = numeric(length = length(iso_codes)),
                      ##  25th percentile
                      "P25_urb" = numeric(length = length(iso_codes)),
                      ##  50th percentile
                      "P50_urb" = numeric(length = length(iso_codes)),
                      ##  75th percentile
                      "P75_urb" = numeric(length = length(iso_codes)),
                      ##  Gini coefficient
                      "Gini_lan" = numeric(length = length(iso_codes)),
                      ##  25th percentile
                      "P25_lan" = numeric(length = length(iso_codes)),
                      ##  50th percentile
                      "P50_lan" = numeric(length = length(iso_codes)),
                      ##  75th percentile
                      "P75_lan" = numeric(length = length(iso_codes)),
                      ##  Gini coefficient
                      "Gini_tot" = numeric(length = length(iso_codes)),
                      ##  25th percentile
                      "P25_tot" = numeric(length = length(iso_codes)),
                      ##  50th percentile
                      "P50_tot" = numeric(length = length(iso_codes)),
                      ##  75th percentile
                      "P75_tot" = numeric(length = length(iso_codes)))

value_ras <- brick(paste0(root,"Output/",
                           "ppkm_urb_lan_rescale_stack_",year,"_threshold_",
                           threshold_val,".tif"))

#value_tot_ras <- raster(paste0(root,"Output/",
#                               "ppkm_urb_lan_rescale_stack_",
#                               year,"_threshold_",
#                               threshold_val,"_SUM",".tif"))
# value_ras_2 <- raster(paste0(root,"Output/",
#                              "ppkm_urb_lan_rescale_stack_",year,"_threshold_",
#                              threshold_val,".tif"),
#                       layer = 2)
# value_ras_3 <- raster(paste0(root,"Output/",
#                              "ppkm_urb_lan_rescale_stack_",year,"_threshold_",
#                              threshold_val,".tif"),
#                       layer = 3)
if(core_number >1){
  s_time <- Sys.time()
  print(paste0("Start time: ", s_time))
  beginCluster(n = core_number)
  gini_dt_filled <- clusterGini(gini_dt,zonal_ras,value_ras)
  endCluster()
}else{
  for(i in 1:nrow(gini_dt)){
    s_time <- Sys.time()
    ##	Now store our task item (list of three objects) in our task list:
    gini_dt[ISO_number==gini_dt[i]$ISO_number,
            names(gini_dt):= giniCalc(i)]
    tEnd <- Sys.time()
    wpProgressMessage(i,
                      max = nrow(gini_dt),
                      label = paste0("Received country ", i,
                                     " Processing Time: ",
                                     wpTimeDiff(s_time,tEnd)))
    wpTimeDiff(s_time,Sys.time())
    saveRDS(gini_dt,
            file = paste0(root,"Output/GRI_Gini_data_",year,"_",threshold_val,".RDS"))
  }
}
wpTimeDiff(s_time,Sys.time())
saveRDS(gini_dt_filled,
        file = paste0(root,"Output/GRI_Gini_data_",year,"_",threshold_val,".RDS"))