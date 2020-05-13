require(raster)
require(gdalUtils)



root <- "E:/Research/Global_Relative_Inequalities/Data/LAN/"

## Input folder:
inpath <- paste0(root,"Raw/Intercalibrated_Zhang/")
outpath <- paste0(root,"Derived/")

##  Retrieve the semi processed file in the folder:
raw_lan_list <- Sys.glob(paste0(inpath,"F*B.tif"))

for(lan in raw_lan_list){
  lan <- raster(lan)
  ##  Convert value of -50000 to 0 to get rid of the no data stripe and then set
  ##  the nodata vlaue in the output to -9999:
  Sys.time()
  lan_zeros <- lan * (lan > -40000)
  Sys.time()
}