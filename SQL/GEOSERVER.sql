create or replace function postgis.organize_geoserver_views(imgtype_in varchar)
RETURNS boolean AS
$$﻿
DECLARE
 --LST rasters
 ret_state BOOLEAN := FALSE;
 record_out RECORD;
  sqlStr VARCHAR;
  geoSerName VARCHAR;
  viewName VARCHAR;
BEGIN
	RAISE NOTICE 'Updating % views',imgtype_in;


    RAISE NOTICE 'cleaning MOSAIC table...';


    DELETE FROM postgis.MOSAIC WHERE name like imgtype_in||'%';

    RAISE NOTICE 'OK';



    FOR record_out IN  select extract(year from dtime) as year_out , extract(doy from dtime) as doy_out, id_acquisizione
                        from postgis.acquisizioni inner join postgis.imgtypes using (id_imgtype)
                        where imgtype = upper(imgtype_in)
                        order by 1,2
    LOOP

         sqlStr := 'CREATE or REPLACE VIEW postgis.'||imgtype_in||'_'||record_out.year_out||'_'||record_out.doy_out||
                    ' AS SELECT rast FROM postgis.'||imgtype_in||' WHERE id_acquisizione = '||record_out.id_acquisizione;

        geoSerName := ''||imgtype_in||'_'||record_out.year_out||'_'||record_out.doy_out||'_out';
        viewName := 'postgis.'||imgtype_in||'_'||record_out.year_out||'_'||record_out.doy_out;

        RAISE NOTICE '%',sqlStr;

        EXECUTE sqlStr;

        RAISE NOTICE '%_%_% updated!',imgtype_in,record_out.year_out,record_out.doy_out;

        EXECUTE 'INSERT INTO MOSAIC(NAME,TileTable) values ($1, $2)' USING geoSerName, viewName;

    END LOOP;

 RETURN ret_state;
 EXCEPTION WHEN OTHERS THEN RETURN false;
END;

$$
language 'plpgsql';
