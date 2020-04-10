#!/bin/bash

year=$1
doy=$2
tile_Ref=$3



/usr/bin/raster2pgsql  -M -I  -c -s 4326 -f rast /tmp/tomcat7-tomcat7-tmp/hdfoutevi*.tif postgis.EVI_"$year"_"$doy" | /usr/bin/psql -d gisdb -U satserv
/usr/bin/raster2pgsql  -M -I  -c -s 4326 -f rast /tmp/tomcat7-tomcat7-tmp/hdfoutndvi*.tif postgis.NDVI_"$year"_"$doy" | /usr/bin/psql -d gisdb -U satserv

rm -f /tmp/tomcat7-tomcat7-tmp/hdfout*.tif
rm -f /tmp/tomcat7-tomcat7-tmp/prod*.hdf

ILCOMANDO="curl -X POST  https://droughtsdi.fi.ibimet.cnr.it/dgws/api/organize/j_organize_evi/evi/$year/$doy/$tile_ref"

eval $ILCOMANDO

ILCOMANDO="curl -X POST  https://droughtsdi.fi.ibimet.cnr.it/dgws/api/organize/j_organize_ndvi/ndvi/$year/$doy/$tile_ref"

eval $ILCOMANDO

echo "done"