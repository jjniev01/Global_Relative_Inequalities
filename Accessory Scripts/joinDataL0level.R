require(sf)
root <- "E:/Research/Global_Relative_Inequalities/"
inshp <- st_read(paste0(root,"Data/L0_Zonal_1km/L0_Zonal_1km.shp"),
                 "L0_Zonal_1km",
                 stringsAsFactors = F)
names(inshp)[1] <- "GID"
for(i in 1:nrow(inshp)){
  foo <- sub("([0-9]{1,3})_.*","\\1",inshp[i,]$GID,perl=T)
  inshp[i,]$GID <- foo
}

for(y in c(2000,2003,2006,2009,2012)){
  ##  Load the table data:
  foo_dt <- readRDS(paste0(root,
                           "Output/GRI_Gini_data_",y,
                           "_5.RDS"))
  names(foo_dt)[1] <- "GID"
  names(foo_dt)[2:length(foo_dt)] <- paste0(sub("(.{3}).*_.*","\\1",
                                                names(foo_dt)[2:length(foo_dt)],
                                                perl=T),
                                            "_",
                                            toupper(sub(".*_(.{1}).*","\\1",
                                                names(foo_dt)[2:length(foo_dt)],
                                                perl=T)),
                                            "_",
                                            sub("[0-9]{2}([0-9]{2})","\\1",
                                                y, perl=T))
  inshp <- st_sf(merge(inshp,foo_dt,by="GID"))
}
inshp$Gin_T_Diff <- inshp$Gin_T_12-inshp$Gin_T_00
inshp$GTD_03_00 <- inshp$Gin_T_03-inshp$Gin_T_00
inshp$GTD_06_03 <- inshp$Gin_T_06-inshp$Gin_T_03
inshp$GTD_09_06 <- inshp$Gin_T_09-inshp$Gin_T_06
inshp$GTD_12_09 <- inshp$Gin_T_12-inshp$Gin_T_09

st_write(inshp,
         paste0(root,"Output/",
                "GRI_Gini_Data_2000-2012_thresh_5.shp"),
         "GRI_Gini_Data_2000-2012_thresh_5",
         overwrite=T,
         append=F,
         delete_dsn = T)



