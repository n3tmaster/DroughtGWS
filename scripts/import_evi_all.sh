#!/bin/bash



# first- check if current date is present into right days

#doy=$(date +%j)
year=$1


   for doy in {1..353..16}  
   do

        if [ $doy -lt 10  ]
        then
           z=MOD13Q1.A$year"00"$doy.h18v04*.hdf
         
  	   cp ./$z ./$year$doy.hdf
        else	
	   if [ $doy -lt 100 ] 
	   then
              z=MOD13Q1.A$year"0"$doy.h18v04*.hdf
         
  	      cp ./$z ./$year$doy.hdf
	   else
              z=MOD13Q1.A$year""$doy.h18v04*.hdf
         
  	      cp ./$z ./$year$doy.hdf
 	   fi
        fi
        gdalwarp -of GTiff -t_srs 'EPSG:4326' 'HDF4_EOS:EOS_GRID:'$year$doy'.hdf:MODIS_Grid_16DAY_250m_500m_VI:250m 16 days EVI' hdfoutevi1.tif
        gdalwarp -of GTiff -t_srs 'EPSG:4326' 'HDF4_EOS:EOS_GRID:'$year$doy'.hdf:MODIS_Grid_16DAY_250m_500m_VI:250m 16 days NDVI' hdfoutndvi1.tif

      #  /usr/bin/raster2pgsql -I-C -M -t 240x240 -c -s 4326 -f rast hdfout.tif postgis.EVI_"$year"_"$doy" | /usr/bin/psql -d gisdb -U satserv
      #  /usr/bin/raster2pgsql -I-C -M -t 240x240 -c -s 4326 -f rast hdfout.tif postgis.NDVI_"$year"_"$doy" | /usr/bin/psql -d gisdb -U satserv

       # rm hdfout.tif
       # rm hdfoutndvi.tif
	rm $year$doy.hdf

       # ILCOMANDO="curl -X POST  http://149.139.16.84:8080/dgws/api/organize/j_organize_raster/evi/$year/$doy"

       # eval $ILCOMANDO
         
       # ILCOMANDO="curl -X POST  http://149.139.16.84:8080/dgws/api/organize/j_organize_raster/ndvi/$year/$doy"

       # eval $ILCOMANDO

        if [ $doy -lt 10  ]
        then
           z=MOD13Q1.A$year"00"$doy.h18v05*.hdf
         
  	   cp ./$z ./$year$doy.hdf
        else
	   if [ $doy -lt 100 ] 
	   then
              z=MOD13Q1.A$year"0"$doy.h18v05*.hdf
         
  	      cp ./$z ./$year$doy.hdf
	   else
              z=MOD13Q1.A$year""$doy.h18v05*.hdf
         
  	      cp ./$z ./$year$doy.hdf
	   fi
        fi
        gdalwarp -of GTiff -t_srs 'EPSG:4326' 'HDF4_EOS:EOS_GRID:'$year$doy'.hdf:MODIS_Grid_16DAY_250m_500m_VI:250m 16 days EVI' hdfoutevi2.tif
        gdalwarp -of GTiff -t_srs 'EPSG:4326' 'HDF4_EOS:EOS_GRID:'$year$doy'.hdf:MODIS_Grid_16DAY_250m_500m_VI:250m 16 days NDVI' hdfoutndvi2.tif

     #   /usr/bin/raster2pgsql  -C -M -t 240x240 -a -s 4326 -f rast hdfout.tif postgis.EVI_"$year"_"$doy" | /usr/bin/psql -d gisdb -U satserv
    #    /usr/bin/raster2pgsql  -C -M -t 240x240 -a -s 4326 -f rast hdfout.tif postgis.NDVI_"$year"_"$doy" | /usr/bin/psql -d gisdb -U satserv

    #    rm hdfout.tif
    #    rm hdfoutndvi.tif
	rm $year$doy.hdf

        #ILCOMANDO="curl -X POST  http://149.139.16.84:8080/dgws/api/organize/j_organize_raster/evi/$year/$doy"

        #eval $ILCOMANDO
         
        #ILCOMANDO="curl -X POST  http://149.139.16.84:8080/dgws/api/organize/j_organize_raster/ndvi/$year/$doy"

        #eval $ILCOMANDO

        if [ $doy -lt 10  ]
        then
           z=MOD13Q1.A$year"00"$doy.h19v04*.hdf
         
  	   cp ./$z ./$year$doy.hdf
        else
	   if [ $doy -lt 100 ] 
	   then
              z=MOD13Q1.A$year"0"$doy.h19v04*.hdf
         
  	      cp ./$z ./$year$doy.hdf
	   else
              z=MOD13Q1.A$year""$doy.h19v04*.hdf
         
  	      cp ./$z ./$year$doy.hdf
	   fi
        fi
        gdalwarp -of GTiff -t_srs 'EPSG:4326' 'HDF4_EOS:EOS_GRID:'$year$doy'.hdf:MODIS_Grid_16DAY_250m_500m_VI:250m 16 days EVI' hdfoutevi3.tif
        gdalwarp -of GTiff -t_srs 'EPSG:4326' 'HDF4_EOS:EOS_GRID:'$year$doy'.hdf:MODIS_Grid_16DAY_250m_500m_VI:250m 16 days NDVI' hdfoutndvi3.tif

      #  /usr/bin/raster2pgsql -C -M -t 240x240 -a -s 4326 -f rast hdfout.tif postgis.EVI_"$year"_"$doy" | /usr/bin/psql -d gisdb -U satserv
     #   /usr/bin/raster2pgsql -C -M -t 240x240 -a -s 4326 -f rast hdfout.tif postgis.NDVI_"$year"_"$doy" | /usr/bin/psql -d gisdb -U satserv

    #    rm hdfout.tif
    #    rm hdfoutndvi.tif
	rm $year$doy.hdf

        #ILCOMANDO="curl -X POST  http://149.139.16.84:8080/dgws/api/organize/j_organize_raster/evi/$year/$doy"

        #eval $ILCOMANDO
         
        #ILCOMANDO="curl -X POST  http://149.139.16.84:8080/dgws/api/organize/j_organize_raster/ndvi/$year/$doy"

        #eval $ILCOMANDO

        if [ $doy -lt 10  ]
        then
           z=MOD13Q1.A$year"00"$doy.h19v05*.hdf
         
  	   cp ./$z ./$year$doy.hdf
        else
	   if [ $doy -lt 100 ] 
	   then
              z=MOD13Q1.A$year"0"$doy.h19v05*.hdf
         
  	      cp ./$z ./$year$doy.hdf
	   else
              z=MOD13Q1.A$year""$doy.h19v05*.hdf
         
  	      cp ./$z ./$year$doy.hdf
	   fi
        fi
        gdalwarp -of GTiff -t_srs 'EPSG:4326' 'HDF4_EOS:EOS_GRID:'$year$doy'.hdf:MODIS_Grid_16DAY_250m_500m_VI:250m 16 days EVI' hdfoutevi4.tif
        gdalwarp -of GTiff -t_srs 'EPSG:4326' 'HDF4_EOS:EOS_GRID:'$year$doy'.hdf:MODIS_Grid_16DAY_250m_500m_VI:250m 16 days NDVI' hdfoutndvi4.tif

        /usr/bin/raster2pgsql  -M -I  -c -s 4326 -f rast hdfoutevi*.tif postgis.EVI_"$year"_"$doy" | /usr/bin/psql -d gisdb -U satserv
        /usr/bin/raster2pgsql  -M -I  -c -s 4326 -f rast hdfoutndvi*.tif postgis.NDVI_"$year"_"$doy" | /usr/bin/psql -d gisdb -U satserv

    #    rm hdfout.tif
    #    rm hdfoutndvi.tif
	rm $year$doy.hdf
        
        
        rm hdfout*.tif
       
        ILCOMANDO="curl -X POST  http://149.139.16.84:8080/dgws/api/organize/j_organize_raster/evi/$year/$doy"

        eval $ILCOMANDO
         
        ILCOMANDO="curl -X POST  http://149.139.16.84:8080/dgws/api/organize/j_organize_raster/ndvi/$year/$doy"

        eval $ILCOMANDO

        echo "done"
   done

