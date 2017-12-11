product=MOD11A2
collection=6


# first- check if current date is present into right days
doy=$1
year=$2

#doy=$(date +%j)
#year=$(date +%Y)
echo "check if LST exists for: "$doy" "$year


if [ $doy == 97 ] || [ $doy == 105 ] || [ $doy == 113 ] || [ $doy == 121 ] || [ $doy == 129 ] || [ $doy == 137 ] || [ $doy == 145 ] || [ $doy == 153 ] || [ $doy == 161 ] || [ $doy == 169 ] || [ $doy == 177 ] || [ $doy == 185 ] || [ $doy == 193 ] || [ $doy == 201 ] || [ $doy == 209 ] || [ $doy == 217 ] || [ $doy == 225 ] || [ $doy == 233 ] || [ $doy == 241 ] || [ $doy == 249 ] || [ $doy == 257 ] || [ $doy == 265 ] || [ $doy == 273 ] || [ $doy == 281 ] || [ $doy == 289 ] || [ $doy == 297 ]

then
        echo "attempt to download it"

        if [ $doy -lt 100 ]
        then
           wget "ftp://ladsweb.modaps.eosdis.nasa.gov/allData/"$collection"/"$product"/"$year"/0"$doy"/"$product".A"$year"0"$doy".h18v04*"

           z=MOD11A2.A$year"0"$doy.*.hdf

           mv $z $year$doy.hdf
        else
           wget "ftp://ladsweb.modaps.eosdis.nasa.gov/allData/"$collection"/"$product"/"$year"/"$doy"/"$product".A"$year$doy".h18v04*"

           z=MOD11A2.A$year""$doy.*.hdf

           mv $z $year$doy.hdf
        fi

        gdalwarp -of GTiff -t_srs 'EPSG:4326' 'HDF4_EOS:EOS_GRID:'$year$doy'.hdf:MODIS_Grid_8Day_1km_LST:LST_Day_1km' hdfout.tif

        /usr/bin/raster2pgsql  -c -s 4326 -f rast hdfout.tif postgis.LST_"$year"_"$doy" | /usr/bin/psql -d gisdb -U satserv

        rm hdfout.tif
        rm $year$doy.hdf

        ILCOMANDO="curl -X POST -F 'table_name=lst' -F 'table_temp=LST_$year_$doy' -F 'year=$year' -F 'dayofyear=$doy' http://149.139.16.84:8080/dgws/api/organize/j_raster_save"

        eval $ILCOMANDO

        echo "done"
else
        echo "this is no right doy, i will retry tomorrow"
fi

