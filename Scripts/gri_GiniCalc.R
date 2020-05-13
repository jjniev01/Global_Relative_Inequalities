require(raster)
require(sf)
require(data.table)
require(reldist)



root <- "E:/Research/Global_Relative_Inequalities/"


##
##  Declare where our zonal raster is:
zonal_ras_path <- paste0(root, "Data/L1_Zonal_100m/",
                         "l1mosaicpixel.tif")

####  ZONAL 100m TO 1KM RESAMPLE  --- ####
zonal_ras <- raster(zonal_ras_path)

