#!/bin/sh

#Import generic raster into temporary table
#This script will be used with specific API that perform the uploading of the raster

/usr/bin/raster2pgsql -b 1 -c -s 4326 -f rast $1 postgis.$2 | /usr/bin/psql -d gisdb -U satserv