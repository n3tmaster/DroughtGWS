#!/bin/bash

year=$1
doy=$2


gdalwarp -of GTiff -t_srs 'EPSG:4326' 'HDF4_EOS:EOS_GRID:/tmp/tomcat7-tomcat7-tmp/prod0.hdf:MODIS_Grid_16DAY_250m_500m_VI:250m 16 days EVI' /tmp/tomcat7-tomcat7-tmp/hdfoutevi1.tif
gdalwarp -of GTiff -t_srs 'EPSG:4326' 'HDF4_EOS:EOS_GRID:/tmp/tomcat7-tomcat7-tmp/prod0.hdf:MODIS_Grid_16DAY_250m_500m_VI:250m 16 days NDVI' /tmp/tomcat7-tomcat7-tmp/hdfoutndvi1.tif

gdalwarp -of GTiff -t_srs 'EPSG:4326' 'HDF4_EOS:EOS_GRID:/tmp/tomcat7-tomcat7-tmp/prod1.hdf:MODIS_Grid_16DAY_250m_500m_VI:250m 16 days EVI' /tmp/tomcat7-tomcat7-tmp/hdfoutevi2.tif
gdalwarp -of GTiff -t_srs 'EPSG:4326' 'HDF4_EOS:EOS_GRID:/tmp/tomcat7-tomcat7-tmp/prod1.hdf:MODIS_Grid_16DAY_250m_500m_VI:250m 16 days NDVI' /tmp/tomcat7-tomcat7-tmp/hdfoutndvi2.tif

gdalwarp -of GTiff -t_srs 'EPSG:4326' 'HDF4_EOS:EOS_GRID:/tmp/tomcat7-tomcat7-tmp/prod2.hdf:MODIS_Grid_16DAY_250m_500m_VI:250m 16 days EVI' /tmp/tomcat7-tomcat7-tmp/hdfoutevi3.tif
gdalwarp -of GTiff -t_srs 'EPSG:4326' 'HDF4_EOS:EOS_GRID:/tmp/tomcat7-tomcat7-tmp/prod2.hdf:MODIS_Grid_16DAY_250m_500m_VI:250m 16 days NDVI' /tmp/tomcat7-tomcat7-tmp/hdfoutndvi3.tif

gdalwarp -of GTiff -t_srs 'EPSG:4326' 'HDF4_EOS:EOS_GRID:/tmp/tomcat7-tomcat7-tmp/prod3.hdf:MODIS_Grid_16DAY_250m_500m_VI:250m 16 days EVI' /tmp/tomcat7-tomcat7-tmp/hdfoutevi4.tif
gdalwarp -of GTiff -t_srs 'EPSG:4326' 'HDF4_EOS:EOS_GRID:/tmp/tomcat7-tomcat7-tmp/prod3.hdf:MODIS_Grid_16DAY_250m_500m_VI:250m 16 days NDVI' /tmp/tomcat7-tomcat7-tmp/hdfoutndvi4.tif

/usr/bin/raster2pgsql  -M -I  -c -s 4326 -f rast /tmp/tomcat7-tomcat7-tmp/hdfoutevi*.tif postgis.EVI_"$year"_"$doy" | /usr/bin/psql -d gisdb -U satserv
/usr/bin/raster2pgsql  -M -I  -c -s 4326 -f rast /tmp/tomcat7-tomcat7-tmp/hdfoutndvi*.tif postgis.NDVI_"$year"_"$doy" | /usr/bin/psql -d gisdb -U satserv

rm -f /tmp/tomcat7-tomcat7-tmp/hdfout*.tif
rm -f /tmp/tomcat7-tomcat7-tmp/prod*.hdf

ILCOMANDO="curl -X POST  http://149.139.16.84:8080/dgws/api/organize/j_organize_raster/evi/$year/$doy"

eval $ILCOMANDO

ILCOMANDO="curl -X POST  http://149.139.16.84:8080/dgws/api/organize/j_organize_raster/ndvi/$year/$doy"

eval $ILCOMANDO

echo "done"

