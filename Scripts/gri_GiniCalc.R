require(raster)
require(sf)
require(data.table)
require(reldist)
require(snow)


root <- "E:/Research/Global_Relative_Inequalities/"
backtrans <- T
year <- 2012
threshold_val <- 5

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
iso_codes <- iso_codes[!{iso_codes %in% c(10)}]

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
  print(paste0("     Start: ", s_time,"     End: ", Sys.time()))
  
  
  s_time <- Sys.time()
  print(paste0("Back transforming urban proportion..."))
  urb_ras <- raster(paste0(root, 
                           "Output/",
                           "ppkm_urb_lan_rescale_stack_2012_threshold_5_LOG.tif"),
                    band = 2)
  urb_ras <- exp(urb_ras)-0.01
  print(paste0("     Start: ", s_time,"     End: ", Sys.time()))
  
  
  s_time <- Sys.time()
  print(paste0("Back transforming lights-at-night..."))
  lan_ras <- raster(paste0(root, 
                           "Output/",
                           "ppkm_urb_lan_rescale_stack_2012_threshold_5_LOG.tif"),
                    band = 3)
  lan_ras <- exp(lan_ras)-1
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
  rm(rescale_stack,
     pop_ras,
     urb_ras,
     lan_ras,
     tot_ras)
}




##  Preallocate a data table to hold our Gini coefficient data and such:
gini_dt <- data.table("ISO_number" = iso_codes,
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
                      "P75_lan" = numeric(length = length(iso_codes)))


value_ras <- raster(paste0(root,"Output/",
                      "ppkm_urb_lan_rescale_stack_",year,"_threshold_",
                      threshold_val,".tif"))

if(!file.exists(paste0(root,"Output/",
                       "ppkm_urb_lan_rescale_stack_",year,"_threshold_",
                       threshold_val,"_SUM",".tif"))){
  tot_ras <- stackApply(value_ras,
                        indices = c(1,1,1),
                        fun = "sum",
                        na.rm = F,
                        filename = paste0(root,"Output/",
                                          "ppkm_urb_lan_rescale_stack_",
                                          year,"_threshold_",
                                          threshold_val,"_SUM",".tif"),
                        format = "GTiff",
                        datatype = dataType(value_ras),
                        overwrite = T,
                        options = c("COMPRESS = LZW"))
  rm(tot_ras)
}
gc()
value_tot_ras <- raster(paste0(root,"Output/",
                               "ppkm_urb_lan_rescale_stack_",
                               year,"_threshold_",
                               threshold_val,"_SUM",".tif"))
# value_ras_2 <- raster(paste0(root,"Output/",
#                              "ppkm_urb_lan_rescale_stack_",year,"_threshold_",
#                              threshold_val,".tif"),
#                       layer = 2)
# value_ras_3 <- raster(paste0(root,"Output/",
#                              "ppkm_urb_lan_rescale_stack_",year,"_threshold_",
#                              threshold_val,".tif"),
#                       layer = 3)

##  TASK FARM CREATION  ----
clusterGini <- function(gini_dt,
                        zonal_ras,
                        value_ras,
                        value_tot_ras,
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
  })
  ##  Pass off the required data and functions to the nodes in the cluster
  ##   - this includes the list of lists used for informing predictions, and the
  ##     task function which creates the predictions for each admin unit:
  
  clusterExport(cl, c("gini_dt",
                      "zonal_ras",
                      "value_ras",
                      "value_tot_ras"))
  
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
    
    #print(paste0("     Unit ", g))
    for(l in 1:3){
      
      ##  Extract the values from the value raster:
      foo_values <- extract(value_ras,
                            which(values(zonal_ras)==g),
                            layer = l)
      
      ## Remove any NA values:
      foo_values <- foo_values[!is.na(foo_values)]
      
      ##  Sort in ascending order:
      foo_values <- foo_values[order(foo_values)]
      
      foo_gini <- gini(foo_values)
      foo_quant <- quantile(foo_values,
                            probs = c(0.25,0.5,0.75))
      foo_25 <- foo_quant[,1]
      foo_50 <- foo_quant[,2]
      foo_75 <- foo_quant[,3]
      
      ##  Extract the values from the summed value raster:
      foo_tot_values <- extract(value_tot_ras,
                            which(values(zonal_ras)==g))
      
      ## Remove any NA values:
      foo_tot_values <- foo_tot_values[!is.na(foo_tot_values)]
      
      ##  Sort in ascending order:
      foo_tot_values <- foo_tot_values[order(foo_tot_values)]
      
      foo_tot_gini <- gini(foo_tot_values)
      foo_tot_quant <- quantile(foo_tot_values,
                            probs = c(0.25,0.5,0.75))
      foo_tot_25 <- foo_tot_quant[,1]
      foo_tot_50 <- foo_tot_quant[,2]
      foo_tot_75 <- foo_tot_quant[,3]
      
      ##  Calculate desired metrics:
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
      ##  Store the admin_ind and the corresponding probabilities in a list
      ##  within a listthe list under the character representation of the admin
      ##  id so we can retrieve them in our chunking of tasks:
    val_list <- list(g,
                   pop_gini,pop_25,pop_50,pop_75,
                   urb_gini,urb_25,urb_50,urb_75,
                   lan_gini,lan_25,lan_50,lan_75,
                   foo_tot_gini, foo_tot_25, foo_tot_50, foo_tot_75)
    
    
    return(val_list)
  }
  
  
  
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
    gini_dt[gini_dt[i]$ISO_number,
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
