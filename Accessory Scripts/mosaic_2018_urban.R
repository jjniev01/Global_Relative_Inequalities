require(raster)
require(rgdal)
require(gdalUtils)


## Get list of rasters to merge:
ras_list <- list.files(path = "E:/Research/Global_Relative_Inequalities/Data/Built_Settlement/Binary/GP/2018/",full.names = T)

## Set output dir and path
outdir <- "E:/Research/Global_Relative_Inequalities/Data/Built_Settlement/Binary/GP/"
outfile <- "BSGM_BinaryMosaic_1_0_NoData_2018.tif"

##  Make template raster:
e <- extent(-180.0012,180.0012,-72.00042,84.00208)
template <- raster(e)
projection(template) <- "+proj=longlat +datum=WGS84 +no_defs +ellps=WGS84 +towgs84=0,0,0"
writeRaster(template,
            file=paste0(outdir, outfile),
            format="GTiff",
            datatype="INT1U",
            overwrite=T,
            options=c("COMPRESS=LZW"))
rm(template,e)
gc()

print(paste0("Start: ",Sys.time()))
mosaic_rasters(gdalfile=ras_list,
               dst_dataset=paste0(outdir,outfile),
               of = "GTiff", 
               ot = "Byte",
               co = "COMPRESS=LZW")
print(paste0("End: ",Sys.time()))
