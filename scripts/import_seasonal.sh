#!/bin/bash

# Import seasonal images into DO.
# $1 - name of nc file

gdalwarp -of GTiff -t_srs 'EPSG:4326' NETCDF:"$1":spi3  /tmp/tomcat7-tomcat7-tmp/spi3.tif
/usr/bin/raster2pgsql -b 1 -c -s 4326 -f rast /tmp/tomcat7-tomcat7-tmp/spi3.tif postgis.spi3_temp | /usr/bin/psql -d gisdb -U satserv

gdalwarp -of GTiff -t_srs 'EPSG:4326' NETCDF:"$1":perc_below /tmp/tomcat7-tomcat7-tmp/perc_below.tif
/usr/bin/raster2pgsql -b 1 -c -s 4326 -f rast /tmp/tomcat7-tomcat7-tmp/perc_below.tif postgis.spi3_perc_below_temp | /usr/bin/psql -d gisdb -U satserv

gdalwarp -of GTiff -t_srs 'EPSG:4326' NETCDF:"$1":perc_norm /tmp/tomcat7-tomcat7-tmp/perc_norm.tif
/usr/bin/raster2pgsql -b 1 -c -s 4326 -f rast /tmp/tomcat7-tomcat7-tmp/perc_norm.tif postgis.spi3_perc_norm_temp | /usr/bin/psql -d gisdb -U satserv

gdalwarp -of GTiff -t_srs 'EPSG:4326' NETCDF:"$1":perc_above /tmp/tomcat7-tomcat7-tmp/perc_above.tif
/usr/bin/raster2pgsql -b 1 -c -s 4326 -f rast /tmp/tomcat7-tomcat7-tmp/perc_above.tif postgis.spi3_perc_above_temp | /usr/bin/psql -d gisdb -U satserv

gdalwarp -of GTiff -t_srs 'EPSG:4326' NETCDF:"$1":perc_bottom /tmp/tomcat7-tomcat7-tmp/perc_bottom.tif
/usr/bin/raster2pgsql -b 1 -c -s 4326 -f rast /tmp/tomcat7-tomcat7-tmp/perc_bottom.tif postgis.spi3_perc_bottom_temp | /usr/bin/psql -d gisdb -U satserv

gdalwarp -of GTiff -t_srs 'EPSG:4326' NETCDF:"$1":perc_top /tmp/tomcat7-tomcat7-tmp/perc_top.tif
/usr/bin/raster2pgsql -b 1 -c -s 4326 -f rast /tmp/tomcat7-tomcat7-tmp/perc_top.tif postgis.spi3_perc_top_temp | /usr/bin/psql -d gisdb -U satserv

rm /tmp/tomcat7-tomcat7-tmp/spi3.tif
rm /tmp/tomcat7-tomcat7-tmp/perc_below.tif
rm /tmp/tomcat7-tomcat7-tmp/perc_norm.tif
rm /tmp/tomcat7-tomcat7-tmp/perc_above.tif
rm /tmp/tomcat7-tomcat7-tmp/perc_bottom.tif
rm /tmp/tomcat7-tomcat7-tmp/perc_top.tif