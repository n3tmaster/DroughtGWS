#!/bin/bash


# first- check if current date is present into right days
year=$1
doy=$2
icount=$3

gdalwarp -of GTiff -t_srs 'EPSG:4326' 'HDF4_EOS:EOS_GRID:/tmp/tomcat7-tomcat7-tmp/prodlst'"$icount"'.hdf:MODIS_Grid_8Day_1km_LST:LST_Day_1km' /tmp/tomcat7-tomcat7-tmp/hdfoutlst"$icount".tif
#gdalwarp -of GTiff -t_srs 'EPSG:4326' 'HDF4_EOS:EOS_GRID:/tmp/tomcat7-tomcat7-tmp/prodlst1.hdf:MODIS_Grid_8Day_1km_LST:LST_Day_1km' /tmp/tomcat7-tomcat7-tmp/hdfoutlst2.tif
#gdalwarp -of GTiff -t_srs 'EPSG:4326' 'HDF4_EOS:EOS_GRID:/tmp/tomcat7-tomcat7-tmp/prodlst2.hdf:MODIS_Grid_8Day_1km_LST:LST_Day_1km' /tmp/tomcat7-tomcat7-tmp/hdfoutlst3.tif
#gdalwarp -of GTiff -t_srs 'EPSG:4326' 'HDF4_EOS:EOS_GRID:/tmp/tomcat7-tomcat7-tmp/prodlst3.hdf:MODIS_Grid_8Day_1km_LST:LST_Day_1km' /tmp/tomcat7-tomcat7-tmp/hdfoutlst4.tif

#/usr/bin/raster2pgsql  -c -s 4326 -f rast /tmp/tomcat7-tomcat7-tmp/hdfoutlst*.tif postgis.LST_"$year"_"$doy" | /usr/bin/psql -d gisdb -U satserv

#rm -f /tmp/tomcat7-tomcat7-tmp/hdfoutlst*.tif


#ILCOMANDO="curl -X POST  https://droughtsdi.fi.ibimet.cnr.it/dgws/api/organize/j_organize_raster/lst/$year/$doy"

#eval $ILCOMANDO

echo "done"

