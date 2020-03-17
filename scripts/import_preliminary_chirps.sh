#!/bin/bash


# first- check if current date is present into right days
#doy=$1


#year=$(date +%Y)

gdal_translate -of VRT -a_ullr -180 50 180 -50 -a_srs EPSG:4326 -a_nodata -9999 -b $1 /tmp/tomcat7-tomcat7-tmp/chirps_preliminary.nc /tmp/tomcat7-tomcat7-tmp/pchirps_flipped.vrt

gdal_translate -of VRT -srcwin 3400 0 1000 400 -a_srs EPSG:4326 -a_nodata -9999 -b 1 /tmp/tomcat7-tomcat7-tmp/pchirps_flipped.vrt /tmp/tomcat7-tomcat7-tmp/pchirps_flipped_tiled.vrt

gdalwarp -of netcdf -t_srs 'EPSG:4326' /tmp/tomcat7-tomcat7-tmp/pchirps_flipped_tiled.vrt /tmp/tomcat7-tomcat7-tmp/pchirps_flipped_warped.nc

/usr/bin/raster2pgsql -c -s 4326 -f rast /tmp/tomcat7-tomcat7-tmp/pchirps_flipped_warped.nc postgis.prel_rain_flipped_warped | /usr/bin/psql -d gisdb -U satserv


rm -f /tmp/tomcat7-tomcat7-tmp/pchirps_flipped.vrt
rm -f /tmp/tomcat7-tomcat7-tmp/pchirps_flipped_tiled.vrt
rm -f /tmp/tomcat7-tomcat7-tmp/pchirps_flipped_warped.nc

echo "done"

