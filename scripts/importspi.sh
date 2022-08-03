#!/bin/sh

# 1 - band number
# 2 - original nc dataset
# 3 - temp dir
# 4 - step

/usr/bin/gdal_translate -of NETCDF -b $1 NETCDF:"$2" $3/spi$4_o_$1.nc

/usr/bin/gdalwarp -of NETCDF -t_srs 'EPSG:4326' NETCDF:"$3/spi$4_o_$1.nc":spi $3/spi$4_$1.nc

/usr/bin/gdal_translate -of GTiff -unscale -ot Float32 -strict NETCDF:"$3/spi$4_$1.nc" $3/spi$4_$1.tiff

/usr/bin/raster2pgsql  -a -s 4326 -f rast $3/spi$4_$1.tiff postgis.temp_spi$4 | /usr/bin/psql -d gisdb -U satserv