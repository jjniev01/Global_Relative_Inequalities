require(data.table)
require(reldist)


root <- "E:/Research/Global_Relative_Inequalities/"
year <- 2012
threshold_val <- 5

##  Declare the input directory:
indir <- paste0(root,"Output/Extracted_Values/")


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


##  DATA IMPORTATION  ---
##  List the files that have been extracted for the given year of interest:
infiles <- list.files(indir, 
                      pattern=paste0("GRI_.*_(?:",year,").*[.]RDS"),
                      full.names=T, 
                      recursive=F)

##  Preallocate our data.table:
##  Preallocate a data table to hold our Gini coefficient data and such:
gini_dt <- data.table("ISO_number" = numeric(length = length(infiles)),
                      "YEAR" = numeric(length = length(infiles)),
                      ##  Gini coefficient
                      "Gini_pop" = numeric(length = length(infiles)),
                      ##  25th percentile
                      "P25_pop" = numeric(length = length(infiles)),
                      ##  50th percentile
                      "P50_pop" = numeric(length = length(infiles)),
                      ##  75th percentile
                      "P75_pop" = numeric(length = length(infiles)),
                      ##  Gini coefficient
                      "Gini_urb" = numeric(length = length(infiles)),
                      ##  25th percentile
                      "P25_urb" = numeric(length = length(infiles)),
                      ##  50th percentile
                      "P50_urb" = numeric(length = length(infiles)),
                      ##  75th percentile
                      "P75_urb" = numeric(length = length(infiles)),
                      ##  Gini coefficient
                      "Gini_lan" = numeric(length = length(infiles)),
                      ##  25th percentile
                      "P25_lan" = numeric(length = length(infiles)),
                      ##  50th percentile
                      "P50_lan" = numeric(length = length(infiles)),
                      ##  75th percentile
                      "P75_lan" = numeric(length = length(infiles)),
                      ##  Gini coefficient
                      "Gini_tot" = numeric(length = length(infiles)),
                      ##  25th percentile
                      "P25_tot" = numeric(length = length(infiles)),
                      ##  50th percentile
                      "P50_tot" = numeric(length = length(infiles)),
                      ##  75th percentile
                      "P75_tot" = numeric(length = length(infiles))) 


s_time <- Sys.time()
print(paste0("Start time: ", s_time))


for(f in 1:length(infiles)){
  ##  Extract the ISOCODE:
  g <- sub(".*/GRI_(.*)_.*_threshold.*","\\1",infiles[f],perl=T)
  
  ##  Read in the data:
  foo_dat <- readRDS(infiles[f])
  
  for(l in 1:3){
    foo_col_name <-paste0("VALUE_",l)
    ## Remove any NA values:
    foo_slice <- foo_dat[!is.na(foo_dat[,get(foo_col_name)]), get(foo_col_name)]
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
  ##  Calculate the gini and other values for the total column:
  foo_col_name <- "VALUE_TOT"
  foo_tot_values <- foo_dat[!is.na(foo_dat[,get(foo_col_name)]), get(foo_col_name)]
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
  
  }else{
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
  val_list <- list(as.numeric(g),
                   year,
                   pop_gini,pop_25,pop_50,pop_75,
                   urb_gini,urb_25,urb_50,urb_75,
                   lan_gini,lan_25,lan_50,lan_75,
                   foo_tot_gini,foo_tot_25,foo_tot_50,foo_tot_75)
  
  ##	Now store our task item (list of three objects) in our task list:
  gini_dt[f,
          names(gini_dt):= val_list]
  
  wpProgressMessage(f,
                    max=length(infiles),
                    label = paste0(" ISO ",g," complete."))
}

saveRDS(gini_dt,
        file = paste0(root,"Output/Gini_Tables/GRI_Gini_data",year,
                      "_threshold_",threshold_val,".RDS"))

 


# for(i in 1:nrow(gini_dt)){
#   tStart <- Sys.time()
#   ##	Now store our task item (list of three objects) in our task list:
#   gini_dt[gini_dt[i]$ISO_number,
#           names(gini_dt):= giniCalc(i)]
#   tEnd <- Sys.time()
#   wpProgressMessage(i,
#                     max = nrow(gini_dt),
#                     label = paste0("Received country ", i,
#                                    " Processing Time: ",
#                                    wpTimeDiff(tStart,tEnd)))
# }
wpTimeDiff(s_time,Sys.time())