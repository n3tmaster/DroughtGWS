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


-- NEW VERSION - generic type
create or replace function postgis.calculate_minmax(imgtyp varchar, doy_in int, year_in int)
RETURNS TABLE(img_min RASTER, img_max RASTER, lst_zero RASTER, last_img RASTER) AS
$BODY$
DECLARE


 rastin RASTER;


 num_element int;
 imgtype_in int;
 sqlStr varchar;

 perc_threshold int;

 lst_i RECORD;
BEGIN



 select id_imgtype into imgtype_in
 from   postgis.imgtypes
 where  imgtype = upper(imgtyp);


 RAISE NOTICE 'Get Last IMG %', imgtype_in;
 sqlStr := 'SELECT ST_Union(rast)  FROM postgis.'||imgtyp||
 		' INNER JOIN postgis.acquisizioni USING (id_acquisizione) WHERE id_imgtype = $1 AND extract(doy from dtime)= $2 AND extract(year from dtime) = $3';

 RAISE NOTICE '%',sqlStr;

 EXECUTE sqlStr USING imgtype_in,doy_in,year_in INTO last_img;


 RAISE NOTICE 'Pixel Type %',ST_BandPixelType(last_img,1);
 img_max := st_addband(st_makeemptyraster(st_band(last_img,1)),ST_BandPixelType(last_img, 1));
 img_min := st_addband(st_makeemptyraster(st_band(last_img,1)),ST_BandPixelType(last_img, 1));


 RAISE NOTICE 'Calculating check threshold';
 sqlStr := 'select count(*) '||
	'from postgis.acquisizioni '||
	'where extract(doy from dtime) = $1 '||
	'and   extract(year from dtime) <> $2 '||
	'and   id_imgtype = $3';
 EXECUTE sqlStr USING doy_in, year_in, imgtype_in INTO num_element;

 RAISE NOTICE 'Number of elements: %',num_element;
 perc_threshold := (num_element / 100.0) * 25.0;
 RAISE NOTICE 'Perc: %',perc_threshold;

 RAISE NOTICE 'Calculating 0 occurrencies with %',perc_threshold;
	FOR lst_i IN select id_acquisizione as ids
			from postgis.acquisizioni
			where extract(doy from dtime) = doy_in
			and   extract(year from dtime) <> year_in
			and   id_imgtype = imgtype_in
			order by dtime
	LOOP
			RAISE NOTICE 'Processing %',lst_i.ids;
			sqlStr := 'select ST_Union(rast) '||
					'from postgis.'||imgtyp||' '||
					'where id_acquisizione = $1';

			EXECUTE sqlStr USING lst_i.ids INTO rastin;

			lst_zero := ST_MapAlgebra(ARRAY[ROW(lst_zero,1), ROW(rastin,1)]::rastbandarg[],
			'calc_zero_occurrencies(double precision[], int[], text[])'::regprocedure,
			'16BUI', 'LAST', null, 0, 0, null);

	END LOOP;

	RAISE NOTICE 'Calculating max';
 	FOR lst_i IN select id_acquisizione as ids
			from postgis.acquisizioni
			where extract(doy from dtime) = doy_in
			and   extract(year from dtime) <> year_in
			and   id_imgtype = imgtype_in
			order by dtime
	LOOP
			RAISE NOTICE 'Processing %',lst_i.ids;
			sqlStr := 'select ST_Union(rast) '||
					'from postgis.'||imgtyp||' '||
					'where id_acquisizione = $1';

			EXECUTE sqlStr USING lst_i.ids INTO rastin;

			img_max := ST_MapAlgebra(ARRAY[ROW(lst_zero,1), ROW(img_max,1), ROW(rastin,1)]::rastbandarg[],
                'calc_max(double precision[], int[], text[])'::regprocedure,
                ST_BandPixelType(rastin,1), 'LAST', null, 0, 0, VARIADIC ARRAY[perc_threshold]::text[]);
			img_min := ST_MapAlgebra(ARRAY[ROW(lst_zero,1), ROW(img_min,1), ROW(rastin,1)]::rastbandarg[],
                'calc_min(double precision[], int[], text[])'::regprocedure,
                ST_BandPixelType(rastin,1), 'LAST', null, 0, 0, VARIADIC ARRAY[perc_threshold]::text[]);


	END LOOP;



END;
$BODY$
  LANGUAGE plpgsql VOLATILE;

