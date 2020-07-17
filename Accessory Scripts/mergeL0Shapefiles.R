##  FILE:  mergeL0Shapefiles.R
##  TITLE: L0 Merging of Shapefiles
##  AUTHOR: JEREMIAH J. NIEVES
##  DATE:  2020-06-11
##  SUMMARY: Reads in all of the polygonized shapefiles of the L0 boundaries 
##           based upon the worldpop global project mastergrid. Those shapefiles
##           are country specific and have multiple features for each country 
##           even though there should be one boundary. This script loops through
##           the files, dissolves them into singular features if needed, and 
##           places the sf objects into a list. The list of sf objects are then 
##           merged into a single shapefile and written to file.
##  ----------------------------------------------------------------------------

require(sf)



root <- "E:/Research/Global_Relative_Inequalities/"
indir <- "E:/Datasets/Global L0 Shapefiles/"
outdir <- paste0(root,"Data/L0_Zonal_1km/")


##  Retrieve all the shapefiles:
shp_list <- list.files(indir, 
                       pattern = ".*[.]shp",
                       full.names = T,
                       recursive = T)

##  Read them all into a list as sf objects:
shps <- vector(length = length(shp_list), mode= "list")
for(s in 1:length(shp_list)){
  lyr <- strsplit(basename(shp_list[s]), ".shp")[[1]]
  print(paste0("Working on ", lyr, "    ", Sys.time()))
  foo_shp <- st_read(dsn = shp_list[s],
                     layer = lyr,
                     stringsAsFactors = F, 
                     quiet = T)
  ##  If there is more than one feature per country, we need to dissolve by the 
  ##  geometry into a single multipart feature
  if(nrow(foo_shp)>1){
    print(paste0("     Dissolving for ", lyr," ..."))
    foo_dissolve <- st_union(foo_shp)
    foo_shp <- st_sf(foo_dissolve)
    st_write(foo_shp,
             shp_list[s],
             lyr,
             driver = "ESRI Shapefile",
             overwrite = T,
             append= F)
    foo_shp <- st_read(dsn = shp_list[s],
                       layer = lyr,
                       stringsAsFactors = F, 
                       quiet = T)
    rm(foo_dissolve)
  }
  if(!("DN" %in% names(foo_shp))){
    names(foo_shp)[1] <- "DN"
    foo_dn <- sub("({[0-9]{1,3})_.*","\\1",lyr,perl=T)
    foo_shp$DN <- foo_dn
  }
  ##  Put the sf in the list:
  shps[[s]] <- foo_shp
}

for(s in shps){
  if(any(!(names(s) %in% c("DN","geometry")))){
    stop(paste0(str(s)))
  }
}
##  Merge all the shapes together:
merged_shps <- st_sf(do.call(rbind,shps))

##  Write the new merged shapefile:
st_write(merged_shps,
         dsn = paste0(outdir,"L0_Zonal_1km.shp"),
         layer = "L0_Zonal_1km",
         driver = "ESRI Shapefile",
         overwrite = T)