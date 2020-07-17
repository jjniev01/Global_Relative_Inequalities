require(sf)
require(data.table)
require(ggplot2)


root <- "E:/Research/Global_Relative_Inequalities/"

inshp <- st_read(paste0(root,"Output/",
                        "GRI_Gini_Data_2000-2012_thresh_5.shp"),
                 "GRI_Gini_Data_2000-2012_thresh_5",
                 stringsAsFactors = F)

if(!file.exists(paste0(root,"Output/",
                       "GRI_Gini_Data_2000-2012_thresh_5.RDS"))){
  years <- c(2000,2003,2006,2009,2012)
  for(y in 1:length(years)){
    ##  Load the table data:
    foo_dt <- readRDS(paste0(root,
                             "Output/GRI_Gini_data_",years[y],
                             "_5.RDS"))
    names(foo_dt)[1] <- "GID"
    foo_dt$YEAR <- years[y]
    if(y ==1){
      indt <- copy(foo_dt)
    }else{
      indt <- copy(rbind(indt,foo_dt))
    }
    rm(foo_dt)
  }
  
  
  ##  Retrieve our unique country codes:
  iso_df <- as.data.table(read.csv(paste0(root,
                                          "Data/L0_Zonal_1km/",
                                          "Mastergrid countrycodeID with UN continents_BETA.csv"),
                                   stringsAsFactors = F))
  names(iso_df)[2] <- "GID"
  iso_df <- copy(iso_df[!is.na(GID),])
  
  
  ## Subset and merge with the gini data:
  indt <- copy(merge(indt,
                     iso_df[,c("GID","contNAME","subContNAME","ISO3")],
                     by="GID"))
  rm(iso_df)
  saveRDS(indt,
          paste0(root,"Output/","GRI_Gini_Data_2000-2012_thresh_5.RDS"))
}
indt <- readRDS(paste0(root,"Output/","GRI_Gini_Data_2000-2012_thresh_5.RDS"))



##  Removal of outliers:
##  Remove any record missing a subcontinent:
indt <- copy(indt[!{subContNAME==""}])



####  GINI POINT AND LINE PLOTS
##  Gini Total Curves by countries and faceted by region
ggplot(data = indt,aes(x=as.factor(YEAR),y=Gini_tot,group=ISO3))+#,color = ISO3))+
  geom_point()+
  geom_line()+
  theme_bw()+
  facet_wrap(subContNAME~.,ncol=3)



ggplot(data = indt,aes(x=as.factor(YEAR),y=Gini_pop,group=ISO3))+#,color = ISO3))+
  geom_point()+
  geom_line()+
  theme_bw()+
  facet_wrap(subContNAME~.,ncol=3)



ggplot(data = indt,aes(x=as.factor(YEAR),y=Gini_lan,group=ISO3))+#,color = ISO3))+
  geom_point()+
  geom_line()+
  theme_bw()+
  facet_wrap(subContNAME~.,ncol=3)



ggplot(data = indt[ISO3 %in% c("USA","CHN","IRQ","UK","RUS","AFG","COL"),],
       aes(x=as.integer(YEAR),y=Gini_urb,group=ISO3,color = ISO3))+
  geom_point()+
  geom_smooth(method = "loess")+
  theme_bw()#+
  #facet_wrap(subContNAME~.,ncol=3)


ggplot(data = indt[ISO3 %in% c("USA","CHN","IRQ","UK","RUS","AFG","COL"),],
       aes(x=as.integer(YEAR),y=Gini_tot,group=ISO3,color = ISO3))+
  geom_point()+
  geom_smooth(method = "loess")+
  theme_bw()



ggplot(data = indt[ISO3 %in% c("USA","CHN","IRQ","UK","RUS","AFG","COL"),],
       aes(x=as.integer(YEAR),y=Gini_lan,group=ISO3,color = ISO3))+
  geom_point()+
  geom_smooth(method = "loess")+
  theme_bw()


ggplot(data = indt[ISO3 %in% c("USA","CHN","IRQ","UK","RUS","AFG","COL"),],
       aes(x=as.integer(YEAR),y=Gini_pop,group=ISO3,color = ISO3))+
  geom_point()+
  geom_smooth(method = "loess")+
  theme_bw()
