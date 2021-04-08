require(raster)
require(glcm)
require(data.table)
require(microbenchmark)

##  Row number to start our benchmarking on in case we need to restart because 
##  of a crash or an interrupt:
inter_start <- NULL
##  Number of repetitions per benchmark:
repetitions <- 10
##  Raster dimensions in numbers of pixels:
ras_dims <- c(100,500,seq(1000,3000,by=1000))#5000,by=1000),10000,50000,100000)
## Numebr of grey levels to use in quantizization:
grey_levels <- c(8,16,48,124,256)
##  Size of rectangular search window in pixels (must be odd to be able to have a "center":
search_window <- c(3,5,11,21)

##  Get the original unique lengths of each parameterL
n_ras_dims <- length(ras_dims)
n_grey_levels <- length(grey_levels)
n_search_window <- length(search_window)

##  Make unique combination out of the three specified levels:
unique_combos <- CJ(ras_dims, grey_levels, search_window, unique=TRUE)

##  Table for holding the stats on the time runs
process_stats_dt <- data.table("RASTER.DIMS"=unique_combos$ras_dims,
                               "RAS.INDEX"=rep(seq(1:n_ras_dims),each=20),
                               "GREY.LEVELS"=unique_combos$grey_levels,
                               "SEARCH.WINDOW"=unique_combos$search_window,
                               "MED.TIME"=numeric(),
                               "AVG.TIME"=numeric(),
                               "LQ.TIME"=numeric(),
                               "UQ.TIME"=numeric(),
                               "N.REP"=repetitions
                               )

##  List to hold rasters:
test_ras_list<-vector(mode="list",length=n_ras_dims)

## Create the test rasters:
for(i in 1:length(test_ras_list)){
  set.seed(245634)
  ##  Create a raster with values between 0 and 1 whose probability density 
  ##  function is given by a beta(1,7) distribution:
  tmp_ras <- raster(nrow = unique(ras_dims)[i])
  tmp_ras[]<-rbeta(ncell(tmp_ras),1,7)
  ## Place in the list:
  test_ras_list[[i]]<-tmp_ras
  rm(tmp_ras)
}



##  Run the benchmarks:
for(i in 1:ifelse(!is.null(inter_start),interstart,nrow(process_stats_dt))){
  print(paste0("Processing combination: ", i, " of ",nrow(process_stats_dt),"..."))
  ## Run the microbenchmark:
  bench_results <- summary(microbenchmark(glcm(test_ras_list[[process_stats_dt[i,RAS.INDEX]]],
         ##  Retrieve our grey levels we are using to quantize things
         n_grey = process_stats_dt[i,GREY.LEVELS],
         ##  Declare our moving window size:
         window = c(process_stats_dt[i,SEARCH.WINDOW],
                    process_stats_dt[i,SEARCH.WINDOW]),
         ##  Calculate over directions of 0, 45, 90, & 135 degrees
         shift=list(c(0,1), c(1,1), c(1,0), c(1,-1)),
         ##Ignore NA values in a window
         na_opt = "ignore"),
    setup=set.seed(34645723),
    times=repetitions))
  
  ##  Put the data in the data.table by converting the results into an list 
  ##  with a specified order:
  process_stats_dt[i,c("MED.TIME",
                       "AVG.TIME",
                       "LQ.TIME",
                       "UQ.TIME"):=list(bench_results$median,
                                        bench_results$mean,
                                        bench_results$lq,
                                        bench_results$uq)]
  
  ##  Write to file in case things crash or we need to exit early:
  saveRDS(process_stats_dt,file="E:/Research/Global_Relative_Inequalities/GLCM_benchmark_tests.RDS")
}

