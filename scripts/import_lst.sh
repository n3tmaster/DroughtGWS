#!/bin/bash


# first- check if current date is present into right days
year=$1
doy=$2



gdalwarp -of GTiff -t_srs 'EPSG:4326' 'HDF4_EOS:EOS_GRID:/tmp/tomcat7-tomcat7-tmp/prod0.hdf:MODIS_Grid_8Day_1km_LST:LST_Day_1km' /tmp/tomcat7-tomcat7-tmp/hdfout1.tif
gdalwarp -of GTiff -t_srs 'EPSG:4326' 'HDF4_EOS:EOS_GRID:/tmp/tomcat7-tomcat7-tmp/prod1.hdf:MODIS_Grid_8Day_1km_LST:LST_Day_1km' /tmp/tomcat7-tomcat7-tmp/hdfout2.tif
gdalwarp -of GTiff -t_srs 'EPSG:4326' 'HDF4_EOS:EOS_GRID:/tmp/tomcat7-tomcat7-tmp/prod2.hdf:MODIS_Grid_8Day_1km_LST:LST_Day_1km' /tmp/tomcat7-tomcat7-tmp/hdfout3.tif
gdalwarp -of GTiff -t_srs 'EPSG:4326' 'HDF4_EOS:EOS_GRID:/tmp/tomcat7-tomcat7-tmp/prod3.hdf:MODIS_Grid_8Day_1km_LST:LST_Day_1km' /tmp/tomcat7-tomcat7-tmp/hdfout4.tif

/usr/bin/raster2pgsql  -c -s 4326 -f rast /tmp/tomcat7-tomcat7-tmp/hdfout*.tif postgis.LST_"$year"_"$doy" | /usr/bin/psql -d gisdb -U satserv

rm -f /tmp/tomcat7-tomcat7-tmp/hdfout*.tif

ILCOMANDO="curl -X POST -F 'table_name=lst' -F 'table_temp=LST_$year_$doy' -F 'year=$year' -F 'dayofyear=$doy' http://149.139.16.84:8080/dgws/api/organize/j_raster_save"

eval $ILCOMANDO

echo "done"



