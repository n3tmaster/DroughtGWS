
#!/bin/bash


# first- check if current date is present into right days
#doy=$1


#year=$(date +%Y)

gdal_translate -of VRT -a_ullr -180 50 180 -50 -a_srs EPSG:4326 -a_nodata -9999 -b $2 $1 chirps_flipped.vrt

gdal_translate -of VRT -srcwin 3420 0 920 400 -a_srs EPSG:4326 -a_nodata -9999 -b 1 chirps_flipped.vrt chirps_flipped_tiled.vrt

gdalwarp -of netcdf -t_srs 'EPSG:4326' chirps_flipped_tiled.vrt chirps_flipped_warped.vrt

/usr/bin/raster2pgsql -c -s 4326 -f rast chirps_flipped_warped.vrt postgis.rain_flipped_warped | /usr/bin/psql -d gisdb -U satserv


rm -f *.vrt

echo "done"

