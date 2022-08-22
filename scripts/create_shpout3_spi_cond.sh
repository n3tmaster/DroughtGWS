#!/bin/sh

#create shape file for download

pgsql2shp -f /tmp/tomcat7-tomcat7-tmp/$1_m20 -u satserv -P 'ss!2017pwd'  -g spi_geom gisdb "SELECT spi_geom, n_pixels, perc, region_name from postgis.spi_cond_temp where dtime = $2 AND spi_class = -20.0"
pgsql2shp -f /tmp/tomcat7-tomcat7-tmp/$1_m15 -u satserv -P 'ss!2017pwd'  -g spi_geom gisdb "SELECT spi_geom, n_pixels, perc, region_name from postgis.spi_cond_temp where dtime = $2 AND spi_class = -15.0"
pgsql2shp -f /tmp/tomcat7-tomcat7-tmp/$1_m10 -u satserv -P 'ss!2017pwd'  -g spi_geom gisdb "SELECT spi_geom, n_pixels, perc, region_name from postgis.spi_cond_temp where dtime = $2 AND spi_class = -10.0"
pgsql2shp -f /tmp/tomcat7-tomcat7-tmp/$1_0 -u satserv -P 'ss!2017pwd'  -g spi_geom gisdb "SELECT spi_geom, n_pixels, perc, region_name from postgis.spi_cond_temp where dtime = $2 AND spi_class = 0.0"
pgsql2shp -f /tmp/tomcat7-tomcat7-tmp/$1_p10 -u satserv -P 'ss!2017pwd'  -g spi_geom gisdb "SELECT spi_geom, n_pixels, perc, region_name from postgis.spi_cond_temp where dtime = $2 AND spi_class = 10.0"
pgsql2shp -f /tmp/tomcat7-tomcat7-tmp/$1_p15 -u satserv -P 'ss!2017pwd'  -g spi_geom gisdb "SELECT spi_geom, n_pixels, perc, region_name from postgis.spi_cond_temp where dtime = $2 AND spi_class = 15.0"
pgsql2shp -f /tmp/tomcat7-tomcat7-tmp/$1_p20 -u satserv -P 'ss!2017pwd'  -g spi_geom gisdb "SELECT spi_geom, n_pixels, perc, region_name from postgis.spi_cond_temp where dtime = $2 AND spi_class = 20.0"

zip -D -j /tmp/tomcat7-tomcat7-tmp/$1.zip /tmp/tomcat7-tomcat7-tmp/$1_m20.shp /tmp/tomcat7-tomcat7-tmp/$1_m20.shx /tmp/tomcat7-tomcat7-tmp/$1_m20.dbf /tmp/tomcat7-tomcat7-tmp/$1_m15.shp /tmp/tomcat7-tomcat7-tmp/$1_m15.shx /tmp/tomcat7-tomcat7-tmp/$1_m15.dbf /tmp/tomcat7-tomcat7-tmp/$1_m10.shp /tmp/tomcat7-tomcat7-tmp/$1_m10.shx /tmp/tomcat7-tomcat7-tmp/$1_m10.dbf /tmp/tomcat7-tomcat7-tmp/$1_0.shp /tmp/tomcat7-tomcat7-tmp/$1_0.shx /tmp/tomcat7-tomcat7-tmp/$1_0.dbf /tmp/tomcat7-tomcat7-tmp/$1_p10.shp /tmp/tomcat7-tomcat7-tmp/$1_p10.shx /tmp/tomcat7-tomcat7-tmp/$1_p10.dbf /tmp/tomcat7-tomcat7-tmp/$1_p15.shp /tmp/tomcat7-tomcat7-tmp/$1_p15.shx /tmp/tomcat7-tomcat7-tmp/$1_p15.dbf /tmp/tomcat7-tomcat7-tmp/$1_p20.shp /tmp/tomcat7-tomcat7-tmp/$1_p20.shx /tmp/tomcat7-tomcat7-tmp/$1_p20.dbf