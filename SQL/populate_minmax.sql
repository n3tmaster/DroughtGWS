﻿--populate max_rasters images
create or replace function postgis.populate_min_max_rasters(imgtype_in integer, year_in integer,  data_type varchar, tile_in integer
    )
RETURNS BOOLEAN AS
$BODY$
DECLARE
 --NDVI rasters
 ndvimax RASTER;
 ndvimin RASTER;

 ndvi_i   RASTER;
 first_time BOOLEAN := true;
 doy_in INT;
 id_acquisizione_in INT;

 acquisizioni_i RECORD;
 doy_i RECORD;

BEGIN
 RAISE NOTICE 'Populating max rasters';

 FOR doy_i IN select extract('doy' from dtime) as d
 			from     postgis.acquisizioni
            where    extract('year' from dtime) <> year_in
            and      id_imgtype = imgtype_in
            group by 1
            order by 1
 LOOP

    RAISE NOTICE 'managing doy: %', doy_i.d;

    FOR acquisizioni_i IN select id_acquisizione as id, extract('year' from dtime) as y
 	    		from     postgis.acquisizioni
                where    extract('year' from dtime) <> year_in
                and      extract('doy' from dtime) = doy_i.d
                and      id_imgtype = imgtype_in
                order by dtime
    LOOP




        IF first_time = true THEN

            RAISE NOTICE 'first time - id ref: %, %',acquisizioni_i.id, acquisizioni_i.y;

            EXECUTE 'select ST_Union(rast)
            from   postgis.'||data_type||'
            where  id_acquisizione = $1' INTO ndvimax USING acquisizioni_i.id;

            ndvimin := ndvimax;

            first_time := false;
        ELSE

            RAISE NOTICE 'id ref: %, %',acquisizioni_i.id, acquisizioni_i.y;

            EXECUTE 'select ST_Union(rast)
            from   postgis.'||data_type||'
            where  id_acquisizione = $1' INTO ndvi_i USING acquisizioni_i.id;

            ndvimax := ST_MapAlgebra(ARRAY[ROW(ndvimax,1), ROW(ndvi_i,1)]::rastbandarg[],
                            'ST_Max4ma(double precision[], int[], text[])'::regprocedure,
                           	'16BSI', 'LAST', null, 0, 0, null);

            ndvimin := ST_MapAlgebra(ARRAY[ROW(ndvimin,1), ROW(ndvi_i,1)]::rastbandarg[],
                            'ST_Min4ma(double precision[], int[], text[])'::regprocedure,
                           	'16BSI', 'LAST', null, 0, 0, null);
        END IF;
        RAISE NOTICE 'ok';
    END LOOP;

    RAISE NOTICE 'Storing MAX';

    insert into max_rasters (id_imgtype, rast, doy)
    values
    (imgtype_in, ST_Tile(ndvimax,tile_in,tile_in,false),doy_i.d);

    RAISE NOTICE 'Storing MIN';

    insert into min_rasters (id_imgtype, rast,doy )
    values
    (imgtype_in, ST_Tile(ndvimin,tile_in,tile_in,false),doy_i.d);

    first_time := true;

    RAISE NOTICE 'done.';

 END LOOP;



 RAISE NOTICE 'finished!';



 RETURN true;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE;