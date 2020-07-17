require(raster)
require(data.table)

args <- commandArgs(TRUE)

root <- "/mainfs/scratch/jjn1n15/GRI/"
outdir <- paste0(root,"Data/Extracted_Values/")
##  If the file already exists do we wish to overwrite?
overwrite <- F
year <- as.numeric(eval(parse(text=args[1])))

threshold_val <- as.numeric(eval(parse(text=args[2])))
#5

make_sum <- as.logical(eval(parse(text=args[3])))




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
iso_codes <- iso_codes[!{iso_codes %in% c(10, 900, 901)}]

##  Repaste the bigs ones on the end:
iso_codes <- c(iso_codes, non_shared_isos)
iso_codes <- iso_codes[!is.na(iso_codes)]



# if(make_sum & !file.exists(paste0(root,"Output/",
#                                   "ppkm_urb_lan_rescale_stack_",year,
#                                   "_threshold_",
#                                   threshold_val,"_SUM",".tif"))){
#   foo_ras_1 <- raster(paste0(root,"Output/",
#                              "ppkm_urb_lan_rescale_stack_",year,"_threshold_",
#                              threshold_val,".tif"),
#                       band = 1)
#   foo_ras_2 <- raster(paste0(root,"Output/",
#                              "ppkm_urb_lan_rescale_stack_",year,"_threshold_",
#                              threshold_val,".tif"),
#                       band = 2)
#   foo_ras_3 <- raster(paste0(root,"Output/",
#                              "ppkm_urb_lan_rescale_stack_",year,"_threshold_",
#                              threshold_val,".tif"),
#                       band = 3)
#   tot_ras <- foo_ras_1 + foo_ras_2 + foo_ras_3
#   writeRaster(tot_ras,
#               filename = paste0(root,"Output/",
#                                 "ppkm_urb_lan_rescale_stack_",
#                                 year,"_threshold_",
#                                 threshold_val,"_SUM",".tif"),
#               format = "GTiff",
#               datatype = "FLT8S",
#               overwrite = T,
#               options = c("COMPRESS = LZW"))
#   # stackApply(value_ras,
#   #                     indices = c(1,1,1),
#   #                     fun = "sum",
#   #                     na.rm = F,
#   # filename = paste0(root,"Output/",
#   #                   "ppkm_urb_lan_rescale_stack_",
#   #                   year,"_threshold_",
#   #                   threshold_val,"_SUM",".tif"),
#   # format = "GTiff",
#   # datatype = dataType(value_ras),
#   # overwrite = T,
#   # options = c("COMPRESS = LZW"))
#   rm(foo_ras_1, foo_ras_2, foo_ras_3, tot_ras)
# }
# gc()




value_ras <- raster(paste0(root,"Output/",
                           "ppkm_urb_lan_rescale_stack_",year,"_threshold_",
                           threshold_val,".tif"))

# value_tot_ras <- raster(paste0(root,"Output/",
#                                "ppkm_urb_lan_rescale_stack_",
#                                year,"_threshold_",
#                                threshold_val,"_SUM",".tif"))



##  DATA PROCESSING  ----
for(i in 1:length(iso_codes)){
  ##  Get the unique GID:
  g <- iso_codes[i]
  
  tStart <- Sys.time()
  print("")
  print(paste0("Working on ",g, "... ", tStart))
  if(!file.exists(paste0(outdir,"GRI_",g,"_",year,
                         "_threshold_",threshold_val,".RDS")) | overwrite){
    for(l in 1:3){
      foo_values <- extract(value_ras,
                            which(values(zonal_ras)==g))
      if(l == 1){
        foo_dt <- data.table("VALUE_1" = foo_values)
      }
      if(l == 2){
        foo_dt[,"VALUE_2" := foo_values]
      }
      if(l == 3){
        foo_dt[,"VALUE_3" := foo_values]
      }
    }
    foo_dt[,"VALUE_TOT" := {VALUE_1+VALUE_2+VALUE_3}]
    foo_dt[,NA_Flag := {is.na(VALUE_1)&is.na(VALUE_2)&
        is.na(VALUE_3)&is.na(VALUE_TOT)}]
    foo_dt <- copy(foo_dt[NA_Flag == FALSE])
    saveRDS(foo_dt,
            file = paste0(outdir,"GRI_",g,"_",year,
                          "_threshold_",threshold_val,".RDS"))
  }
  tEnd <- Sys.time()
  wpProgressMessage(i,
                    max = length(iso_codes),
                    label = paste0("Received country ", g,
                                   " Processing Time: ",
                                   wpTimeDiff(tStart,tEnd)))
  
}