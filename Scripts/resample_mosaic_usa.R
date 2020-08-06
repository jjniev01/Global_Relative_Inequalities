##  RESAMPLE MOSAIC USA
require(gdalUtils)
require(rgdal)
require(raster)

args <- commandArgs(TRUE)

y <- as.numeric(eval(parse(text=args[1])))


root <- "/mainfs/scratch/jjn1n15/GRI/Data/Built_Settlement/"

if(!file.exists(paste0(root,"USA_CutData/","USA_",
                      ifelse(y==2000,
                             "GHSL_ESA_2000_clipped",
                             "GUF_GHSL_2012_clipped"),".tif"))){
  
  state_ras_list <- grep("(?!USA).*",
                         Sys.glob(paste0(root, "USA_CutData/*_",
                                    ifelse(y==2000,
                                           "GHSL_ESA_2000_USA",
                                           "GUF_GHSL_2012_USA"),
                                    ".tif")),
                         perl=T,
                         value=T)
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
}

##   Resample the entire USA for the given year:
outname <- paste0("USA_",ifelse(y==2000,
                                "GHSL_ESA_2000_clipped",
                                "GUF_GHSL_2012_clipped"),
                  "_1km.tif")
usa_in_ras <- raster(paste0(root,"USA_CutData/","USA_",
                            ifelse(y==2000,
                                   "GHSL_ESA_2000_clipped",
                                   "GUF_GHSL_2012_clipped"),".tif"))
print(paste0("Aggregating to 0.008333 arc sec..."))
##  Note: the below takes about 6 hours to aggregate.
start_t <- Sys.time()
print(paste0("     Start: ", start_t))

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

print(paste0("     End: ", Sys.time()))
print("Complete!")
rm(usa_in_ras)
gc()
