:: dmspNL_processing.bat - script number 2, to be run after (1) dmspNL_preprocessing.bat.	Script by Heather Chamberlain May 2017	Modified JJNieves Nov 2019


@ECHO on
SETLOCAL EnableDelayedExpansion


SET mypath_calib=E:\Research\Global_Relative_Inequalities\Data\LAN\Raw\Intercalibrated_Zhang\
:: create new folder for output files - the following line should be commented out if the folder \processing\ already exists
2>nul MD ::F:\WorldPop\Project02_WorldPop_Global\Pr02_WPG_processing\DMSPnightlightsv4_processing\processing\

SET mypath_output=E:\Research\Global_Relative_Inequalities\Data\LAN\Derived\

SET mypath_ccid= E:\Research\Global_Relative_Inequalities\Data\CCID\


:: processing - apply to all annual DMSP rasters (2000-2011)

:: Note: In order to clip to ciesin coastal boundaries, and in order to tile the data, you will need to place a copy of ccid100mr0.tif into your ccid directory

:: removing projection so that no pixel shift or clipping at extreme longitudes occurs when asserting dataset extents throughout the workflow
FOR %%i IN (%mypath_calib%F*.tif) DO ( 
	
	SET infile=%%i
	
	call python "C:\Program Files (x86)\GDAL\gdal_edit.py" -a_srs None !infile!
)

:: change resolution using nearest neighbour technique and standardise grid extent. New cells created by the expansion of spatial extent will be automatically be assigned 0 values, however there seems to be an issue with nodata values at the edges of the viirs spatial extent - the next few steps will deal with these nodata values and change them to zeros. Change nodata value to -5000 so common with ccid grid for next step
FOR %%i IN (%mypath_calib%F*.tif) DO (
	
	SET infile=%%i
	SET outfile=!infile:.tif=A.tif!
	
	gdalwarp -wo NUM_THREADS=8 -ot Int16 -te -180.001249265 -71.999582844 180.001249295 84.0020831987 -tr 0.00083333333 0.00083333333 -srcnodata 65535 -dstnodata -5000 -co COMPRESS=LZW -co PREDICTOR=2 -co BIGTIFF=YES -wo SKIP_NOSOURCE=YES -wo OPTIMIZE_SIZE=true -of GTiff !infile! !outfile!
)

:: change nodata value in input dataset (nodata=-5000) to an actual value (-5000)
FOR %%i IN (%mypath_calib%F*A.tif) DO (
	
	SET infile=%%i
	SET outfile=!infile:A.tif=B.tif!
	
	gdal_translate -a_nodata none !infile! !outfile! -co COMPRESS=LZW -co BIGTIFF=YES
)

:: Convert value of -50000 to 0 --> get rid of nodata stripe. Set nodata value in output to -9999
FOR %%i IN (%mypath_calib%F*B.tif) DO (
	
	SET infile=%%i
	SET outfile=!infile:B.tif=C.tif!
	
	call python "C:\Program Files (x86)\GDAL\gdal_calc.py" -A !infile! --outfile=!outfile! --calc="A*(A > -4000)" --NoDataValue=-9999 --co BIGTIFF=YES --co COMPRESS=LZW --overwrite --debug
)




:: Tweak the extent of ccid100mr0 (i.e. 1 for land, 0 elsewhere) reclassified ccid grid so common to other datasets we are working with (we are going to use the ccid grid to clip the ViiRS data to the ccid coastlines). Also change the nodata value to be -9999 and convert data type to be int16
gdalwarp -wo NUM_THREADS=8 -te -180.001249265 -71.999582844 180.001249295 84.0020831987 -ot Int16 -tr 0.00083333333 0.00083333333 -dstnodata -9999 -wo SKIP_NOSOURCE=YES -co PREDICTOR=2 -co BIGTIFF=YES -co COMPRESS=LZW -wo OPTIMIZE_SIZE=true %mypath_ccid%ccid100mr0.tif %mypath_ccid%ccid100mr0_int16.tif

PAUSE

:: create inverse land mask from ccid grid where sea has a value of -1000 and land is 0
call pyhton "C:\Program Files (x86)\GDAL\gdal_calc.py" -A %mypath_ccid%ccid100mr0_int16.tif --outfile=%mypath_ccid%ccid100mr0_int16_inv.tif --calc="(A-1)*1000" --co BIGTIFF=YES --co COMPRESS=LZW --NoDataValue=-9999

PAUSE

:: clip each of the grids to ccid coastlines (multiply together) and set nodata value as -5000
FOR %%i IN (%mypath_calib%F*C.tif) DO (
	
	SET infile=%%i
	SET outfile=!infile:C.tif=D.tif!
	
	call python "C:\Program Files (x86)\GDAL\gdal_calc.py" -A !infile! -B %mypath_ccid%ccid100mr0_int16.tif --outfile=!outfile! --calc="A*B" --co BIGTIFF=YES --co COMPRESS=LZW --co PREDICTOR=2 --NoDataValue=-5000 --overwrite --debug
)

:: Add the inverse land mask to each of the viirs rasters. Set the output nodata value to be -1000
FOR %%i IN (%mypath_calib%F*D.tif) DO (
	
	SET infile=%%i
	SET outfile=!infile:D.tif=_E.tif!
	
	call python "C:\Program Files (x86)\GDAL\gdal_calc.py" -A !infile! -B %mypath_ccid%ccid100mr0_int16_inv.tif --outfile=!outfile! --calc="A+B" --co BIGTIFF=YES --co COMPRESS=LZW --NoDataValue=-1000 --overwrite --debug
)

:: Keep the data type as int16, and then change the nodata value to be the max value for uint16 data type i.e. 32767
FOR %%i IN (%mypath_calib%F*_E.tif) DO (
	
	SET infile=%%i
	SET outfile=!infile:_E.tif=_F.tif!
	ECHO %outfile%
	
	gdalwarp -wo NUM_THREADS=8 -ot Int16 -te -180.001249265 -71.999582844 180.001249295 84.0020831987 -tr 0.00083333333 0.00083333333 -srcnodata -1000 -dstnodata 32767 -co COMPRESS=LZW -co PREDICTOR=2 -co BIGTIFF=YES -wo SKIP_NOSOURCE=YES -wo OPTIMIZE_SIZE=true -of GTiff !infile! !outfile!
)



:: Rename the final output rasters so that the average and single satellite rasters are consistently named
:: Firstly rename the averaged rasters (years for which there is imagery from 2 satellites) 
FOR %%i IN (%mypath_calib%G*average_F.tif) DO (
	
	SET infile=%%i
	SET file_name=!infile:~-29!
	SET file_year=!file_name:~3,4!
	REN "%%i" "dmsp_!file_year!_global_100m.tif"
)

:: Then rename the single satellite rasters
FOR %%i IN (%mypath_calib%G*_F.tif) DO (
	
	SET infile=%%i
	SET file_name=!infile:~-13!
	SET file_year=!file_name:~3,4!
	REN "%%i" "dmsp_!file_year!_global_100m.tif"
)


ECHO.
ECHO.
ECHO.
ECHO end of script reached for script dmspNL_processing.bat
ECHO run for all years (2000-2011)
ECHO end of script
