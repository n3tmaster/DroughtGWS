-- calculate a raster with max value between 2 images

CREATE OR REPLACE FUNCTION postgis.calculate_max_raster(value double precision[][][], pos integer[][], VARIADIC userargs text[])
	RETURNS double precision
	AS $$
	BEGIN

    --    IF value[1][1][1] = 0 THEN
    --        RETURN value[2][1][1];
    --    ELSEIF value[2][1][1] = 0 THEN
    --        RETURN value[1][1][1];
    --    ELSE

        IF value[1][1][1] > value[2][1][1] THEN
            RETURN value[1][1][1];
   		ELSE
		    RETURN value[2][1][1];
		END IF;

	END;
	$$ LANGUAGE 'plpgsql' IMMUTABLE;


-- calculate a raster with min value between 2 images


CREATE OR REPLACE FUNCTION postgis.calculate_min_raster(value double precision[][][], pos integer[][], VARIADIC userargs text[])
	RETURNS double precision
	AS $$
	BEGIN

	 --   IF value[1][1][1] = 0 THEN
     --       RETURN value[2][1][1];
     --   ELSEIF value[2][1][1] = 0 THEN
     --       RETURN value[1][1][1];
     --   ELSE

        IF value[1][1][1] < value[2][1][1] THEN
            RETURN value[1][1][1];
   		ELSE
		    RETURN value[2][1][1];
		END IF;


	END;
	$$ LANGUAGE 'plpgsql' IMMUTABLE;





-- Create TCI image from LST serial
-- Input : dtime_in - timestamp - reference date for TCI calculation
-- Outpu : TCI raster
create or replace function postgis.calculate_tci(
    dtime_in timestamp, store boolean
    )
RETURNS RASTER AS
$BODY$
DECLARE
 --LST rasters
 lstmax RASTER;
 lstmin RASTER;
 lstlast RASTER;

 --TCI raster
 tci RASTER;

 imgtype_in INT;
 tcitype_in INT;
 id_acquisizione_in INT;
 first_time BOOLEAN;
 doy_in INT := extract('doy' from dtime_in);
 year_in INT := extract('year' from dtime_in);

 lst_i RECORD;

BEGIN
 RAISE NOTICE 'Calculating TCI raster for doy: % and year: %',doy_in,year_in;

 first_time := TRUE;

 select id_imgtype into imgtype_in
 from   postgis.imgtypes
 where  imgtype = 'LST';

 select id_imgtype into tcitype_in
 from   postgis.imgtypes
 where  imgtype = 'TCI';

 RAISE NOTICE 'ImgType : %',imgtype_in;

 RAISE NOTICE 'Get Last LST';
 select b.rast into lstlast
 from postgis.acquisizioni a inner join postgis.lst b using (id_acquisizione)
 where  extract('year' from a.dtime) = year_in
 and   extract('doy' from a.dtime) = doy_in
 and   a.id_imgtype = imgtype_in;

 RAISE NOTICE 'Prepare lstmax and lstmin matrix';

 lstmax := st_addband(st_makeemptyraster(st_band(lstlast,1)),'16BUI'::text);
 lstmin := st_addband(st_makeemptyraster(st_band(lstlast,1)),'16BUI'::text);
 tci    := st_addband(st_makeemptyraster(st_band(lstlast,1)),'32BF'::text);



 RAISE NOTICE 'Calculating min and max rasters';
 FOR lst_i IN select b.rast as rastin
 			from postgis.acquisizioni a inner join postgis.lst b using (id_acquisizione)
            where extract('doy' from a.dtime) = doy_in
            and   extract('year' from a.dtime) < year_in
            and   a.id_imgtype = imgtype_in
            order by a.dtime
 LOOP
 	lstmax := ST_MapAlgebra(ARRAY[ROW(lstmax,1), ROW(lst_i.rastin,1)]::rastbandarg[],
                            'ST_Max4ma(double precision[], int[], text[])'::regprocedure,
                           	'16BUI', 'LAST', null, 0, 0, null);

    lstmin := ST_MapAlgebra(ARRAY[ROW(lstmin,1), ROW(lst_i.rastin,1)]::rastbandarg[],
                            'ST_Min4ma(double precision[], int[], text[])'::regprocedure,
                           	'16BUI', 'LAST', null, 0, 0, null);
 END LOOP;

 RAISE NOTICE 'Calculate TCI raster...';
 tci := ST_MapAlgebra(ARRAY[ROW(lstmax,1), ROW(lstmin,1), ROW(lstlast,1)]::rastbandarg[],
                            'calculate_tci_raster(double precision[], int[], text[])'::regprocedure,
                           	'32BF', 'LAST', null, 0, 0, null);



 IF store = TRUE THEN
    RAISE NOTICE 'Check if it exists...';
    id_acquisizione_in := -1;
    select id_acquisizione into id_acquisizione_in
               from postgis.acquisizioni
               where extract('doy' from dtime) = doy_in
               and    extract('year' from dtime) = year_in
               and   id_imgtype = tcitype_in;


    IF id_acquisizione_in <> -1 THEN
        RAISE NOTICE 'deleting old TCI...';

        delete from postgis.tci
        where  id_acquisizione = id_acquisizione_in;

        delete from postgis.acquisizioni
        where  id_acquisizione = id_acquisizione_in;

        RAISE NOTICE 'done';

    END IF;

    RAISE NOTICE 'create new acquisition...';
    insert into postgis.acquisizioni (dtime, id_imgtype)
    values (dtime_in, tcitype_in);


    select id_acquisizione into id_acquisizione_in
    from   postgis.acquisizioni
    where  dtime = dtime_in
    and    id_imgtype = tcitype_in;

    RAISE NOTICE 'save TCI...';
    insert into postgis.tci (id_acquisizione, rast)
    values (id_acquisizione_in, tci);
    RAISE NOTICE 'done';

 END IF;

 RAISE NOTICE 'done.';
 RETURN tci;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE


-- Create rainfall monthly average
-- Input : month
-- Output : average raster
create or replace function postgis.calculate_rain_cum(
    month_in int, year_in int
    )
RETURNS RASTER AS
$BODY$
DECLARE
 --LST rasters
 rainavg RASTER;

 rain_in RASTER;

 rain_i RECORD;

 first_time BOOLEAN := true;

BEGIN
 RAISE NOTICE 'Calculating rain average raster for % , %',month_in,year_in;




 RAISE NOTICE 'Calculating cum raster';
 FOR rain_i IN select b.rast as rastin
 			from postgis.acquisizioni a inner join postgis.precipitazioni b using (id_acquisizione)
            where extract('month' from a.dtime) = month_in
            and   extract('year' from a.dtime) = year_in
            and   a.id_imgtype = 1
            order by a.dtime
 LOOP

    IF first_time = true THEN
        first_time := false;

        rainavg := st_addband(st_makeemptyraster(st_band(rain_i.rastin,1)),'32BF'::text);
    END IF;

 	rainavg := ST_MapAlgebra(ARRAY[ROW(rainavg,1), ROW(rain_i.rastin,1)]::rastbandarg[],
                            'calculate_cum_raster(double precision[], int[], text[])'::regprocedure,
                           	'32BF', 'LAST', null, 0, 0, null);


 END LOOP;


 RAISE NOTICE 'done.';
 RETURN rainavg;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE



-- calculate raincum

CREATE OR REPLACE FUNCTION postgis.calculate_cum_raster(value double precision[][][], pos integer[][], VARIADIC userargs text[])
	RETURNS double precision
	AS $$
	BEGIN

		RETURN (value[1][1][1] + value[2][1][1]);

	END;
	$$ LANGUAGE 'plpgsql' IMMUTABLE;








create or replace function postgis.calculate_lst_min(
    dtime_in timestamp
    )
RETURNS RASTER AS
$BODY$
DECLARE
 --LST rasters
 lstmin RASTER;
 lstlast RASTER;


 imgtype_in INT;
 tcitype_in INT;
 id_acquisizione_in INT;

 doy_in INT := extract('doy' from dtime_in);
 year_in INT := extract('year' from dtime_in);

 lst_i RECORD;

BEGIN



 select id_imgtype into imgtype_in
 from   postgis.imgtypes
 where  imgtype = 'LST';


 RAISE NOTICE 'Get Last LST';
 select b.rast into lstlast
 from postgis.acquisizioni a inner join postgis.lst b using (id_acquisizione)
 where  extract('year' from a.dtime) = year_in
 and   extract('doy' from a.dtime) = doy_in
 and   a.id_imgtype = imgtype_in;

 RAISE NOTICE 'Prepare lstmax and lstmin matrix';

 lstmin := st_addband(st_makeemptyraster(st_band(lstlast,1)),'16BUI'::text);



 RAISE NOTICE 'Calculating min and max rasters';
 FOR lst_i IN select b.rast as rastin
 			from postgis.acquisizioni a inner join postgis.lst b using (id_acquisizione)
            where extract('doy' from a.dtime) = doy_in
            and   extract('year' from a.dtime) <> year_in
            and   a.id_imgtype = imgtype_in
            order by a.dtime
 LOOP
 	lstmin := ST_MapAlgebra(ARRAY[ROW(lstmin,1), ROW(lst_i.rastin,1)]::rastbandarg[],
                            'ST_Min4ma(double precision[], int[], text[])'::regprocedure,
                           	'16BUI', 'LAST', null, 0, 0, null);


 END LOOP;


 RETURN lstmin;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE




﻿-- Create or replace tci views for Geoserver
-- Output : TRUE - OK, FLASE - ERROR
create or replace function postgis.organize_geoserver_views(imgtype_in VARCHAR)
RETURNS boolean AS
$BODY$
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
$BODY$
  LANGUAGE plpgsql VOLATILE

create or replace function postgis.calculate_lst_maxmin()
RETURNS BOOLEAN AS
$BODY$
DECLARE
 --LST rasters
 lstmax RASTER;
 lstmin RASTER;
 lstlast RASTER;
 lst_zero RASTER;

 year_in INT;
 imgtype_in INT;
 tcitype_in INT;
 id_acquisizione_in INT;

 num_element INT;
 perc_threshold INT;
 minstat INT;
 maxstat INT;
 minstat2 INT;
 maxstat2 INT;
 minstat3 INT;
 maxstat3 INT;
 lst_i RECORD;
 lst_doy_i RECORD;
BEGIN



 select id_imgtype into imgtype_in
 from   postgis.imgtypes
 where  imgtype = 'LST';

 RAISE NOTICE 'Prepare lstmax and lstmin matrix';



 RAISE NOTICE 'Get Last LST';
 select extract('year' from max(a.dtime)) into year_in
 from   postgis.acquisizioni a inner join postgis.lst b using (id_acquisizione)
 where  a.id_imgtype = imgtype_in;


 FOR lst_doy_i IN select ST_Union(b.rast) as lastrast, extract(doy from a.dtime) as lastdoy
                from   postgis.acquisizioni a inner join postgis.lst b using (id_acquisizione)
                where  extract('year' from a.dtime) = year_in
                and    a.id_imgtype = imgtype_in
				group by a.dtime
                order by a.dtime
 LOOP


        RAISE NOTICE 'Processing doy: %',lst_doy_i.lastdoy;

        lstmax := st_addband(st_makeemptyraster(st_band(lst_doy_i.lastrast,1)),'16BUI'::text);
        lstmin := st_addband(st_makeemptyraster(st_band(lst_doy_i.lastrast,1)),'16BUI'::text);
        lst_zero := st_addband(st_makeemptyraster(st_band(lst_doy_i.lastrast,1)),'16BUI'::text);

        RAISE NOTICE 'Calculating check threshold';
        select count(*) into num_element
        from postgis.acquisizioni a inner join postgis.lst b using (id_acquisizione)
        where extract('doy' from a.dtime) = lst_doy_i.lastdoy
        and   extract('year' from a.dtime) <> year_in
        and   a.id_imgtype = imgtype_in;

		RAISE NOTICE 'Number of elements: %',num_element;
        perc_threshold := (num_element / 100.0) * 25.0;
		RAISE NOTICE 'Perc: %',perc_threshold;

        RAISE NOTICE 'Calculating 0 occurrencies with %',perc_threshold;
        FOR lst_i IN select b.rast as rastin
                    from postgis.acquisizioni a inner join postgis.lst b using (id_acquisizione)
                    where extract('doy' from a.dtime) = lst_doy_i.lastdoy
                    and   extract('year' from a.dtime) <> year_in
                    and   a.id_imgtype = imgtype_in
                    order by a.dtime
        LOOP

            lst_zero := ST_MapAlgebra(ARRAY[ROW(lst_zero,1), ROW(lst_i.rastin,1)]::rastbandarg[],
                'calc_zero_occurrencies(double precision[], int[], text[])'::regprocedure,
                '16BUI', 'LAST', null, 0, 0, null);

        END LOOP;

        RAISE NOTICE 'Calculating min max';
        FOR lst_i IN select b.rast as rastin
                    from postgis.acquisizioni a inner join postgis.lst b using (id_acquisizione)
                    where extract('doy' from a.dtime) = lst_doy_i.lastdoy
                    and   extract('year' from a.dtime) <> year_in
                    and   a.id_imgtype = imgtype_in
                    order by a.dtime
        LOOP

            lstmin := ST_MapAlgebra(ARRAY[ROW(lst_zero,1), ROW(lstmin,1), ROW(lst_i.rastin,1)]::rastbandarg[],
                'calc_min(double precision[], int[], text[])'::regprocedure,
                '16BUI', 'LAST', null, 0, 0, VARIADIC ARRAY[perc_threshold]::text[]);

            lstmax := ST_MapAlgebra(ARRAY[ROW(lst_zero,1), ROW(lstmax,1), ROW(lst_i.rastin,1)]::rastbandarg[],
                'calc_max(double precision[], int[], text[])'::regprocedure,
                '16BUI', 'LAST', null, 0, 0, VARIADIC ARRAY[perc_threshold]::text[]);

        END LOOP;


        SELECT (stats).min, (stats).max, (stats2).min, (stats2).max, (stats3).min, (stats3).max INTO minstat, maxstat, minstat2, maxstat2, minstat3, maxstat3
        FROM (
            SELECT
                ST_SummaryStatsAgg(lstmin, 1, TRUE, 1) AS stats,
                ST_SummaryStatsAgg(lstmax, 1, TRUE, 1) AS stats2,
                ST_SummaryStatsAgg(lst_zero, 1, TRUE, 1) AS stats3

        ) bar;


        RAISE NOTICE 'max %, min % di lstmin', maxstat, minstat;
        RAISE NOTICE 'max %, min % di lstmax', maxstat2, minstat2;
        RAISE NOTICE 'max %, min % di lst_zero', maxstat3, minstat3;

        RAISE NOTICE 'saving max raster';
        IF EXISTS (SELECT *
                    FROM postgis.max_rasters
                    INNER JOIN postgis.imgtypes USING (id_imgtype)
                    WHERE imgtype = 'TCI'
                    AND   doy     = lst_doy_i.lastdoy) THEN

            RAISE NOTICE 'Found';

            UPDATE postgis.max_rasters
            SET    rast = lstmax
            WHERE  doy  = lst_doy_i.lastdoy
            AND    id_imgtype = imgtype_in;

        ELSE
            RAISE NOTICE 'Not Found, Creating';

            INSERT INTO postgis.max_rasters (id_imgtype, doy, rast)
            VALUES
            (imgtype_in, lst_doy_i.lastdoy, ST_Tile(lstmax,200,200));

        END IF;

        RAISE NOTICE 'saving min raster';
        IF EXISTS (SELECT *
                    FROM postgis.min_rasters
                    INNER JOIN postgis.imgtypes USING (id_imgtype)
                    WHERE imgtype = 'TCI'
                    AND   doy     = lst_doy_i.lastdoy) THEN

            RAISE NOTICE 'Found';

            UPDATE postgis.min_rasters
            SET    rast = lstmin
            WHERE  doy  = lst_doy_i.lastdoy
            AND    id_imgtype = imgtype_in;

        ELSE
            RAISE NOTICE 'Not Found, Creating';

            INSERT INTO postgis.min_rasters (id_imgtype, doy, rast)
            VALUES
            (imgtype_in, lst_doy_i.lastdoy, ST_Tile(lstmin,200,200));

        END IF;

 END LOOP;

 RETURN TRUE;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE;

-- calculate zero_occurrencies
-- INPUT
--  RASTER 1 - image to be checked
--  userargs - zero_value
CREATE OR REPLACE FUNCTION postgis.calc_zero_occurrencies(value double precision[][][], pos integer[][], VARIADIC userargs text[])
	RETURNS double precision
	AS $$
	DECLARE
	    zero_value INT := userargs[0];
	BEGIN


		IF value[1][1][1] is NULL THEN
            IF value[2][1][1] is NULL THEN

		    	RETURN 1;
			ELSE

			    RETURN 0;
		    END IF;

		ELSEIF value[2][1][1] = zero_value THEN

		    RETURN (value[1][1][1] + 1);
	    ELSEIF value[2][1][1] is NULL THEN

		    RETURN (value[1][1][1] + 1);
		ELSE

		    RETURN (value[1][1][1]);
		END IF;

	END;
	$$ LANGUAGE 'plpgsql' IMMUTABLE;
-- calculate min
-- INPUT
--  RASTER 1 - lst zero
--  RASTER 2 - lstmin
--  RASTER 3 - last_image
--  userargs - percentage threashold
CREATE OR REPLACE FUNCTION postgis.calc_min(value double precision[][][], pos integer[][], VARIADIC userargs text[])
	RETURNS double precision
	AS
	$BODY$
	DECLARE
	    perc_threashold INT := userargs[0];
	BEGIN

		IF value[1][1][1] > perc_threashold THEN
		    -- put -999
		    RETURN NULL;
		ELSEIF value[2][1][1] is NULL THEN
		    IF value[3][1][1] is NULL THEN
		       RETURN NULL;
		    ELSE
		       RETURN value[3][1][1];
		    END IF;
		ELSEIF value[2][1][1] > value[3][1][1] THEN
		    RETURN value[3][1][1];
		ELSE
		    RETURN value[2][1][1];
		END IF;

	END;
	$BODY$ LANGUAGE 'plpgsql' IMMUTABLE;


-- calculate max
-- INPUT
--  RASTER 1 - lst zero
--  RASTER 2 - lstmax
--  RASTER 3 - last_image
--  userargs - percentage threashold
CREATE OR REPLACE FUNCTION postgis.calc_max(value double precision[][][], pos integer[][], VARIADIC userargs text[])
	RETURNS double precision
	AS
	$BODY$
	DECLARE
	    perc_threashold INT := userargs[0];
	BEGIN

		IF value[1][1][1] > perc_threashold THEN
		    -- put -999
		    RETURN NULL;
		ELSEIF value[2][1][1] is NULL THEN
		    IF value[3][1][1] is NULL THEN
		       RETURN NULL;
		    ELSE
		       RETURN value[3][1][1];
		    END IF;
		ELSEIF value[2][1][1] < value[3][1][1] THEN
		    RETURN value[3][1][1];
		ELSE
		    RETURN value[2][1][1];
		END IF;

	END;
	$BODY$ LANGUAGE 'plpgsql' IMMUTABLE;




-- Create TCI image from LST serial
-- Input : dtime_in - timestamp - reference date for TCI calculation
-- Outpu : TCI raster
create or replace function postgis.calculate_tci(
    doy_in integer, year_in integer
    )
RETURNS RASTER AS
$BODY$
DECLARE
 --LST rasters

 --TCI raster
 tci RASTER;

 imgtype_in INT;
 tcitype_in INT;
 id_acquisizione_in INT;
 first_time BOOLEAN;

 lst_i RECORD;

BEGIN
 RAISE NOTICE 'Calculating TCI raster for doy: % and year: %',doy_in,year_in;

 first_time := TRUE;

 select id_imgtype into imgtype_in
 from   postgis.imgtypes
 where  imgtype = 'LST';


 RAISE NOTICE 'ImgType : %',imgtype_in;

 RAISE NOTICE 'Get LST Last min and max ';


 lst_i := calculate_minmax('tci', doy_in , year_in );

 RAISE NOTICE 'Calculate TCI raster...';
 tci := ST_MapAlgebra(ARRAY[ROW(lst_i.img_max,1), ROW(lst_i.img_min,1), ROW(lst_i.last_img,1)]::rastbandarg[],
                            'calculate_tci_raster(double precision[], int[], text[])'::regprocedure,
                           	'32BF', 'LAST', null, 0, 0, null);





 RAISE NOTICE 'done.';
 RETURN tci;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE



-- calculate tci

CREATE OR REPLACE FUNCTION postgis.calculate_tci_raster(value double precision[][][], pos integer[][], VARIADIC userargs text[])
	RETURNS double precision
	AS $$
	BEGIN



        IF (value[1][1][1] - value[2][1][1]) = 0 then

            RETURN -999.0;

        ELSEIF (value[1][1][1] - value[3][1][1]) < 0.0 THEN

            RETURN 0.0001;

        ELSE

			RETURN (((value[1][1][1] - value[3][1][1]) / (value[1][1][1] - value[2][1][1])) * 100);

		END IF;

	END;
	$$ LANGUAGE 'plpgsql' IMMUTABLE;


create or replace function postgis.get_acquisition(
    id_imgtype_in INT
    )
RETURNS TABLE (id_acquisizione int, dtime timestamp) AS
$BODY$
DECLARE
 lst_i RECORD;
 calculated_out boolean;
 derived_from_out varchar;

BEGIN

 -- Get image properties
 SELECT calculated, derived_from INTO calculated_out, derived_from_out
 FROM   postgis.imgtypes
 WHERE  id_imgtype = id_imgtype_in;

 IF calculated_out = TRUE THEN
 	-- get acquisition directly

	SELECT id_acquisizione, dtime
	FROM
 ELSE

 END IF;

 RETURN tci;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE


-- Create TCI image from LST serial
-- Input : dtime_in - timestamp - reference date for TCI calculation
-- Outpu : TCI raster
-- last version
create or replace function postgis.calculate_tci(
    doy_in integer, year_in integer
    )
RETURNS RASTER AS
$BODY$
DECLARE
 --LST rasters

 --TCI raster
 tci RASTER;

 imgtype_in INT;
 tcitype_in INT;
 id_acquisizione_in INT;
 first_time BOOLEAN;

 lst_i RECORD;
 minstat double precision;
 maxstat double precision;
 minstat2 double precision;
 maxstat2 double precision;
 minstat3 double precision;
 maxstat3 double precision;
 minstat4 double precision;
 maxstat4 double precision;
BEGIN
 RAISE NOTICE 'Calculating TCI raster for doy: % and year: %',doy_in,year_in;

 first_time := TRUE;

 select id_imgtype into imgtype_in
 from   postgis.imgtypes
 where  imgtype = 'LST';


 RAISE NOTICE 'ImgType : %',imgtype_in;

 RAISE NOTICE 'Get LST Last min and max ';

 lst_i := calculate_minmax('lst', doy_in , year_in );

 RAISE NOTICE 'Calculate statistics';

SELECT (stats2).min, (stats2).max, (stats3).min, (stats3).max,(stats4).min, (stats4).max
  INTO minstat, maxstat, minstat2, maxstat2, minstat3, maxstat3,minstat4, maxstat4
        FROM (
            SELECT

                ST_SummaryStatsAgg(lst_i.img_max, 1, TRUE, 1) AS stats2,
                ST_SummaryStatsAgg(lst_i.img_min, 1, TRUE, 1) AS stats3,
			ST_SummaryStatsAgg(lst_i.last_img, 1, TRUE, 1) AS stats4

        ) bar;

        RAISE NOTICE 'max %, min % di lst_max', maxstat2, minstat2;
        RAISE NOTICE 'max %, min % di lst_min', maxstat3, minstat3;
		RAISE NOTICE 'max %, min % di last_img', maxstat4, minstat4;

 RAISE NOTICE 'Calculate TCI raster...';
 tci := ST_MapAlgebra(ARRAY[ROW(lst_i.img_max,1), ROW(lst_i.img_min,1), ROW(lst_i.last_img,1)]::rastbandarg[],
                            'calculate_tci_raster(double precision[], int[], text[])'::regprocedure,
                           	'32BF', 'LAST', null, 0, 0, null);



  SELECT (stats).min, (stats).max, (stats2).min, (stats2).max, (stats3).min, (stats3).max,(stats4).min, (stats4).max
  INTO minstat, maxstat, minstat2, maxstat2, minstat3, maxstat3,minstat4, maxstat4
        FROM (
            SELECT
                ST_SummaryStatsAgg(tci, 1, TRUE, 1) AS stats,
                ST_SummaryStatsAgg(lst_i.img_max, 1, TRUE, 1) AS stats2,
                ST_SummaryStatsAgg(lst_i.img_min, 1, TRUE, 1) AS stats3,
			ST_SummaryStatsAgg(lst_i.last_img, 1, TRUE, 1) AS stats4

        ) bar;





 RAISE NOTICE 'done.';
 RETURN tci;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE

-- calculate TCI - LAST VERSION 2 Agosto 2018
create or replace function postgis.calculate_tci(doy_in int, year_in int)
RETURNS BOOLEAN AS
$BODY$
DECLARE
 --LST rasters
 lstmax RASTER;
 lstmin RASTER;
 tci    RASTER;
 lst_last RASTER;
 lst_zero RASTER;


 imgtype_in INT;

 id_acquisizione_in INT;

 num_element INT;
 perc_threshold INT;
 zero_value INT := 0;
 lst_i RECORD;

BEGIN



	select id_imgtype into imgtype_in
	from   postgis.imgtypes
	where  imgtype = 'LST';

    select id_acquisizione into id_acquisizione_in
	from   postgis.acquisizioni inner join postgis.imgtypes using (id_imgtype)
	where  imgtype = 'TCI'
	and    extract(doy from dtime) = doy_in
	and    extract(year from dtime) = year_in;

	RAISE NOTICE 'Get Last LST';
 	select ST_Union(b.rast) into lst_last
	from   postgis.acquisizioni a inner join postgis.lst b using (id_acquisizione)
	where  extract(year from a.dtime) = year_in
	and    extract(doy from a.dtime) = doy_in
	and    a.id_imgtype = imgtype_in;

	lstmax := st_addband(st_makeemptyraster(st_band(lst_last,1)),'16BUI'::text);
    lstmin := st_addband(st_makeemptyraster(st_band(lst_last,1)),'16BUI'::text);
    lst_zero := st_addband(st_makeemptyraster(st_band(lst_last,1)),'16BUI'::text);

  	RAISE NOTICE 'Calculating check threshold';
	select count(*) into num_element
	from postgis.acquisizioni a
	where extract(doy from a.dtime) = doy_in
	and   extract(year from a.dtime) < year_in
	and   a.id_imgtype = imgtype_in;

	RAISE NOTICE 'Number of elements: %',num_element;
	perc_threshold := (num_element / 100.0) * 25.0;
	RAISE NOTICE 'Perc: %',perc_threshold;


	RAISE NOTICE 'Calculating 0 occurrencies with %',perc_threshold;
	FOR lst_i IN select ST_Union(b.rast) as rastin
				from postgis.acquisizioni a inner join postgis.lst b using (id_acquisizione)
				where extract(doy from a.dtime) = doy_in
				and   extract(year from a.dtime) < year_in
				and   a.id_imgtype = imgtype_in

	LOOP

		lst_zero := ST_MapAlgebra(ARRAY[ROW(lst_zero,1), ROW(lst_i.rastin,1)]::rastbandarg[],
			'calc_zero_occurrencies(double precision[], int[], text[])'::regprocedure,
			'16BUI', 'LAST', null, 0, 0,  VARIADIC ARRAY[zero_value]::text[]);

	END LOOP;


	RAISE NOTICE 'Calculating min max';
	FOR lst_i IN select ST_Union(b.rast) as rastin
				from postgis.acquisizioni a inner join postgis.lst b using (id_acquisizione)
				where extract(doy from a.dtime) = doy_in
				and   extract(year from a.dtime) < year_in
				and   a.id_imgtype = imgtype_in

	LOOP

		lstmin := ST_MapAlgebra(ARRAY[ROW(lst_zero,1), ROW(lstmin,1), ROW(lst_i.rastin,1)]::rastbandarg[],
			'calc_min(double precision[], int[], text[])'::regprocedure,
			'16BUI', 'LAST', null, 0, 0, VARIADIC ARRAY[perc_threshold]::text[]);

		lstmax := ST_MapAlgebra(ARRAY[ROW(lst_zero,1), ROW(lstmax,1), ROW(lst_i.rastin,1)]::rastbandarg[],
			'calc_max(double precision[], int[], text[])'::regprocedure,
			'16BUI', 'LAST', null, 0, 0, VARIADIC ARRAY[perc_threshold]::text[]);

	END LOOP;


	tci := ST_MapAlgebra(ARRAY[ROW(lstmax,1), ROW(lstmin,1), ROW(lst_last,1)]::rastbandarg[],
                            'calculate_tci_raster(double precision[], int[], text[])'::regprocedure,
                           	'32BF', 'LAST', null, 0, 0, null);

	RAISE NOTICE 'saving min raster';
	IF EXISTS (SELECT id_acquisizione
			FROM postgis.tci
			INNER JOIN postgis.acquisizioni USING (id_acquisizione)
			WHERE extract(doy from dtime)=doy_in
			AND   extract(year from dtime)=year_in) THEN

			RAISE NOTICE 'Found';

			DELETE FROM postgis.tci
			WHERE  id_acquisizione = id_acquisizione_in;
	END IF;
	RAISE NOTICE 'ids %',id_acquisizione_in;

	INSERT INTO postgis.tci (id_acquisizione, rast)
	VALUES
	(id_acquisizione_in, ST_Tile(tci,512,512));

    RAISE NOTICE 'saved';
 RETURN TRUE;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE;

-- populate TCI - LAST VERSION 2 Agosto 2018
create or replace function postgis.populate_tci()
RETURNS BOOLEAN AS
$BODY$
DECLARE
 --LST rasters

 lst_i RECORD;
 retcode boolean;
BEGIN

	FOR lst_i IN select extract(doy from dtime)::integer as doy_out,
						extract(year from dtime)::integer as year_out
					from postgis.acquisizioni inner join postgis.imgtypes using (id_imgtype)
					where imgtype = 'LST'
					and   extract(year from dtime)>=2010
					order by dtime

	LOOP
		RAISE NOTICE 'Processing doy: % - year: %', lst_i.doy_out, lst_i.year_out;
		retcode := postgis.calculate_tci(lst_i.doy_out, lst_i.year_out);
		RAISE NOTICE 'Done.';

	END LOOP;
 RETURN TRUE;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE;



  -- calculate TCI - LAST VERSION 3  06/09/2018
create or replace function postgis.calculate_tci(doy_in int, year_in int)
RETURNS BOOLEAN AS
$BODY$
DECLARE
 --LST rasters
 lstmax RASTER;
 lstmin RASTER;
 tci    RASTER;
 lst_last RASTER;
 lst_zero RASTER;


 imgtype_in INT;

 id_acquisizione_in INT;

 num_element INT;
 perc_threshold INT;
 zero_value INT := 0;
 lst_i RECORD;

BEGIN



	select id_imgtype into imgtype_in
	from   postgis.imgtypes
	where  imgtype = 'LST';

    select id_acquisizione into id_acquisizione_in
	from   postgis.acquisizioni inner join postgis.imgtypes using (id_imgtype)
	where  imgtype = 'TCI'
	and    extract(doy from dtime) = doy_in
	and    extract(year from dtime) = year_in;

	RAISE NOTICE 'Get Last LST';
 	select ST_Union(b.rast) into lst_last
	from   postgis.acquisizioni a inner join postgis.lst b using (id_acquisizione)
	where  extract(year from a.dtime) = year_in
	and    extract(doy from a.dtime) = doy_in
	and    a.id_imgtype = imgtype_in;

	lstmax := st_addband(st_makeemptyraster(st_band(lst_last,1)),'16BUI'::text);
    lstmin := st_addband(st_makeemptyraster(st_band(lst_last,1)),'16BUI'::text);
    lst_zero := st_addband(st_makeemptyraster(st_band(lst_last,1)),'16BUI'::text);

  	RAISE NOTICE 'Calculating check threshold';

	RAISE NOTICE 'Calculating min';
    SELECT ST_Union(rast,'MIN') INTO  lstmin
    FROM   postgis.acquisizioni INNER JOIN postgis.evi USING (id_acquisizione)
    WHERE  extract(doy from dtime) = doy_in
    AND    extract(year from dtime) < year_in
    AND    id_imgtype = imgtype_in;

	RAISE NOTICE 'Calculating max';
    SELECT ST_Union(rast,'MAX') INTO return_rast.maxrast
    FROM   postgis.acquisizioni INNER JOIN postgis.evi USING (id_acquisizione)
    WHERE  extract(doy from dtime) = doy_in
    AND    extract(year from dtime) < year_in
    AND    id_imgtype = imgtype_in;


	tci := ST_SetBandNoDataValue(ST_MapAlgebra(ARRAY[ROW(lstmax,1), ROW(lstmin,1), ROW(lst_last,1)]::rastbandarg[],
                            'calculate_tci_raster(double precision[], int[], text[])'::regprocedure,
                           	'32BF', 'LAST', null, 0, 0, null),-999.0);

	RAISE NOTICE 'saving min raster';
	IF EXISTS (SELECT id_acquisizione
			FROM postgis.tci
			INNER JOIN postgis.acquisizioni USING (id_acquisizione)
			WHERE extract(doy from dtime)=doy_in
			AND   extract(year from dtime)=year_in) THEN

			RAISE NOTICE 'Found';

			DELETE FROM postgis.tci
			WHERE  id_acquisizione = id_acquisizione_in;
	END IF;
	RAISE NOTICE 'ids %',id_acquisizione_in;

	INSERT INTO postgis.tci (id_acquisizione, rast)
	VALUES
	(id_acquisizione_in, ST_Tile(tci,512,512));

    RAISE NOTICE 'saved';
 RETURN TRUE;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE;





-- TCI version 3.0 06-09-2018
--  This version optimizes min and max calculation during the whole process
--  it is composed by 2 main function, the first is used in first iteration
--  the second one is used for the other iterations
--  these functions use the custom type minmaxrast for passing min and max rasters
--  calculated in each iteration. In this way postgresql will not have to recalculate min and max for whole set of rasters
--  but only for the last doy.

--  function for the first iteration
create or replace function postgis.calculate_tci(doy_in int, year_in int)
RETURNS minmaxrast AS
$BODY$
DECLARE
 --LST rasters

 tci RASTER;


 return_rast minmaxrast;

 imgtype_in INT;
 id_acquisizione_in INT;


BEGIN

    RAISE NOTICE 'Calculating %, % for first iteration', doy_in, year_in;


	select id_imgtype into imgtype_in
	from   postgis.imgtypes
	where  imgtype = 'LST';

	RAISE NOTICE 'Get Last LST';
 	select ST_Union(b.rast) into return_rast.originrast
	from   postgis.acquisizioni a inner join postgis.lst b using (id_acquisizione)
	where  extract(year from a.dtime) = year_in
	and    extract(doy from a.dtime) = doy_in
	and    id_imgtype = imgtype_in;

	return_rast.maxrast := st_addband(st_makeemptyraster(st_band(return_rast.originrast,1)),'16BUI'::text);
    return_rast.minrast := st_addband(st_makeemptyraster(st_band(return_rast.originrast,1)),'16BUI'::text);


	RAISE NOTICE 'Calculating min';
    SELECT ST_Union(rast,'MIN') INTO  return_rast.minrast
    FROM   postgis.acquisizioni INNER JOIN postgis.lst USING (id_acquisizione)
    WHERE  extract(doy from dtime) = doy_in
    AND    extract(year from dtime) < year_in
    AND    id_imgtype = imgtype_in;

	RAISE NOTICE 'Calculating max';
    SELECT ST_Union(rast,'MAX') INTO return_rast.maxrast
    FROM   postgis.acquisizioni INNER JOIN postgis.lst USING (id_acquisizione)
    WHERE  extract(doy from dtime) = doy_in
    AND    extract(year from dtime) < year_in
    AND    id_imgtype = imgtype_in;

    RAISE NOTICE 'Calculating tci';
	tci := ST_SetBandNoDataValue(ST_MapAlgebra(ARRAY[ROW(return_rast.maxrast,1), ROW(return_rast.minrast,1), ROW(return_rast.originrast,1)]::rastbandarg[],
                            'calculate_tci_raster(double precision[], int[], text[])'::regprocedure,
                           	'32BF', 'LAST', null, 0, 0, null,-999.0);

	RAISE NOTICE 'saving tci raster';
    IF EXISTS ( select id_acquisizione
	            from   postgis.acquisizioni inner join postgis.imgtypes using (id_imgtype)
	            where  imgtype = 'TCI'
	            and    extract(doy from dtime) = doy_in
	            and    extract(year from dtime) = year_in)  THEN

        RAISE NOTICE 'Found';

        select id_acquisizione into id_acquisizione_in
	    from   postgis.acquisizioni inner join postgis.imgtypes using (id_imgtype)
	    where  imgtype = 'TCI'
	    and    extract(doy from dtime) = doy_in
	    and    extract(year from dtime) = year_in;

	    DELETE FROM postgis.tci
		WHERE  id_acquisizione = id_acquisizione_in;

	ELSE

        RAISE NOTICE 'not found, create';
        INSERT INTO postgis.acquisizioni (dtime, id_imgtype)
        VALUES (to_timestamp(''||year_in||' '||doy_in||'', 'YYYY DDD'),(select id_imgtype from postgis.imgtypes where imgtype='TCI'));

        select id_acquisizione into id_acquisizione_in
        from   postgis.acquisizioni inner join postgis.imgtypes using (id_imgtype)
        where  imgtype = 'TCI'
        and    extract(doy from dtime) = doy_in
        and    extract(year from dtime) = year_in;


	END IF;
	RAISE NOTICE 'ids %',id_acquisizione_in;

    INSERT INTO postgis.tci (id_acquisizione, rast)
	VALUES
	(id_acquisizione_in, ST_Tile(tci,512,512));




    RAISE NOTICE 'saved';
 RETURN return_rast;
END;
$BODY$
  LANGUAGE plpgsql;

--  function for the other iterations
create or replace function postgis.calculate_tci(doy_in int, year_in int, return_rast minmaxrast)
RETURNS minmaxrast AS
$BODY$
DECLARE
 --LST rasters

 tci    RASTER;

 lst_last RASTER;
 imgtype_in INT;

 id_acquisizione_in INT;


BEGIN

    RAISE NOTICE 'Calculating %, %', doy_in, year_in;

	select id_imgtype into imgtype_in
	from   postgis.imgtypes
	where  imgtype = 'LST';



	RAISE NOTICE 'Get Last LST';
 	select ST_Union(b.rast) into lst_last
	from   postgis.acquisizioni a inner join postgis.lst b using (id_acquisizione)
	where  extract(year from a.dtime) = year_in
	and    extract(doy from a.dtime) = doy_in
	and    a.id_imgtype = imgtype_in;





    RAISE NOTICE 'calculating min';
    WITH base_min AS (SELECT return_rast.minrast as rast
                  UNION
                  SELECT return_rast.originrast)
    SELECT ST_Union(rast,'MIN') INTO return_rast.minrast FROM base_min;

    RAISE NOTICE 'calculating max';
    WITH base_max AS (SELECT return_rast.maxrast as rast
                  UNION
                  SELECT return_rast.originrast)
    SELECT ST_Union(rast,'MAX') INTO return_rast.maxrast FROM base_max;

    RAISE NOTICE 'Calculating tci';
	tci := ST_SetBandNoDataValue(ST_MapAlgebra(ARRAY[ROW(return_rast.maxrast,1), ROW(return_rast.minrast,1), ROW(lst_last,1)]::rastbandarg[],
                            'calculate_tci_raster(double precision[], int[], text[])'::regprocedure,
                           	'32BF', 'LAST', null, 0, 0, null),-999.0);

    return_rast.originrast := lst_last;

	RAISE NOTICE 'saving vci raster';
	IF EXISTS (  select id_acquisizione
	            from   postgis.acquisizioni inner join postgis.imgtypes using (id_imgtype)
	            where  imgtype = 'TCI'
	            and    extract(doy from dtime) = doy_in
	            and    extract(year from dtime) = year_in)  THEN

        RAISE NOTICE 'Found';

        select id_acquisizione into id_acquisizione_in
	    from   postgis.acquisizioni inner join postgis.imgtypes using (id_imgtype)
	    where  imgtype = 'TCI'
	    and    extract(doy from dtime) = doy_in
	    and    extract(year from dtime) = year_in;

	    DELETE FROM postgis.tci
		WHERE  id_acquisizione = id_acquisizione_in;

	ELSE

        RAISE NOTICE 'not found, create';
        INSERT INTO postgis.acquisizioni (dtime, id_imgtype)
        VALUES (to_timestamp(''||year_in||' '||doy_in||'', 'YYYY DDD'),(select id_imgtype from postgis.imgtypes where imgtype='TCI'));

        select id_acquisizione into id_acquisizione_in
        from   postgis.acquisizioni inner join postgis.imgtypes using (id_imgtype)
        where  imgtype = 'TCI'
        and    extract(doy from dtime) = doy_in
        and    extract(year from dtime) = year_in;


	END IF;
	RAISE NOTICE 'ids %',id_acquisizione_in;

	INSERT INTO postgis.tci (id_acquisizione, rast)
	VALUES
	(id_acquisizione_in, ST_Tile(tci,512,512));

    RAISE NOTICE 'saved';
 RETURN return_rast;
END;
$BODY$
  LANGUAGE plpgsql;


-- main function for generating e-vci rasters
create or replace function postgis.populate_tci(doy_begin INT, year_begin INT,
											   	doy_end INT, year_end INT)
RETURNS BOOLEAN AS
$BODY$
DECLARE
 --LST rasters

 lst_i RECORD;
 retcode boolean;
 return_rast minmaxrast;
 firstone boolean:=true;
 actual_doy int:=0;
BEGIN

	FOR lst_i IN select extract(doy from dtime)::integer as doy_out,
						extract(year from dtime)::integer as year_out
					from postgis.acquisizioni inner join postgis.imgtypes using (id_imgtype)
					where imgtype = 'LST'
					and    extract(doy from dtime) between doy_begin and doy_end
					and    extract(year from dtime) between year_begin and year_end
					order by 1,2

	LOOP
		RAISE NOTICE 'Processing doy: % - year: %', lst_i.doy_out, lst_i.year_out;
		IF actual_doy < lst_i.doy_out then
		    return_rast := postgis.calculate_tci(lst_i.doy_out, lst_i.year_out);

		    actual_doy := lst_i.doy_out;
		else
		    return_rast := postgis.calculate_tci(lst_i.doy_out, lst_i.year_out, return_rast);
		end if;


--        RAISE NOTICE 'Returned min raster: % , % and max raster: %, %', ST_Width(return_rast.minrast), ST_Height(return_rast.minrast), ST_Width(return_rast.maxrast), ST_Height(return_rast.maxrast);
--        RAISE NOTICE 'Returned origin raster: % , % ', ST_Width(return_rast.originrast), ST_Height(return_rast.originrast);
--        RAISE NOTICE 'Saved: %',aaaa;

		RAISE NOTICE 'Done.';

	END LOOP;
 RETURN TRUE;
END;
$BODY$
  LANGUAGE plpgsql;



--
--Versione 16-12-2019
--

CREATE OR REPLACE FUNCTION postgis.calculate_tci(
    doy_in integer,
    year_in integer)
    RETURNS minmaxrast
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE
AS $BODY$DECLARE
    --LST rasters

    tci RASTER;

    return_rast minmaxrast;
    nband INT;
    imgtype_in INT;
    id_acquisizione_in INT;

BEGIN

    RAISE NOTICE 'Calculating %, % for first iteration', doy_in, year_in;

    select id_imgtype into imgtype_in
    from   postgis.imgtypes
    where  imgtype = 'LST';

    RAISE NOTICE 'Get Last LST';
    select ST_Union(b.rast) into return_rast.originrast
    from   postgis.acquisizioni a inner join postgis.lst b using (id_acquisizione)
    where  extract(year from a.dtime) = year_in
      and    extract(doy from a.dtime) = doy_in
      and    id_imgtype = imgtype_in;

    nband := ST_NumBands(return_rast.originrast);
    IF nband is NULL THEN
        RAISE NOTICE 'LST %, % not found. Exit.',doy_in,year_in;
        RETURN NULL;
    END IF;

    return_rast.maxrast := st_addband(st_makeemptyraster(st_band(return_rast.originrast,1)),'16BUI'::text);
    return_rast.minrast := st_addband(st_makeemptyraster(st_band(return_rast.originrast,1)),'16BUI'::text);

    RAISE NOTICE 'Calculating min';
    SELECT ST_Union(rast,'MIN') INTO  return_rast.minrast
    FROM   postgis.acquisizioni INNER JOIN postgis.lst USING (id_acquisizione)
    WHERE  extract(doy from dtime) = doy_in
      AND    extract(year from dtime) < year_in
      AND    id_imgtype = imgtype_in;

    RAISE NOTICE 'Calculating max';
    SELECT ST_Union(rast,'MAX') INTO return_rast.maxrast
    FROM   postgis.acquisizioni INNER JOIN postgis.lst USING (id_acquisizione)
    WHERE  extract(doy from dtime) = doy_in
      AND    extract(year from dtime) < year_in
      AND    id_imgtype = imgtype_in;

    RAISE NOTICE 'Calculating tci';
    tci := ST_MapAlgebra(ARRAY[ROW(return_rast.maxrast,1), ROW(return_rast.minrast,1), ROW(return_rast.originrast,1)]::rastbandarg[],
                         'calculate_tci_raster(double precision[], int[], text[])'::regprocedure,
                         '32BF', 'LAST', null, 0, 0, null);

    RAISE NOTICE 'saving tci raster';
    IF EXISTS ( select id_acquisizione
                from   postgis.acquisizioni inner join postgis.imgtypes using (id_imgtype)
                where  imgtype = 'TCI'
                  and    extract(doy from dtime) = doy_in
                  and    extract(year from dtime) = year_in)  THEN

        RAISE NOTICE 'Found';

        select id_acquisizione into id_acquisizione_in
        from   postgis.acquisizioni inner join postgis.imgtypes using (id_imgtype)
        where  imgtype = 'TCI'
          and    extract(doy from dtime) = doy_in
          and    extract(year from dtime) = year_in;

        DELETE FROM postgis.tci
        WHERE  id_acquisizione = id_acquisizione_in;

    ELSE

        RAISE NOTICE 'not found, create';
        INSERT INTO postgis.acquisizioni (dtime, id_imgtype)
        VALUES (to_timestamp(''||year_in||' '||doy_in||'', 'YYYY DDD'),(select id_imgtype from postgis.imgtypes where imgtype='TCI'));

        select id_acquisizione into id_acquisizione_in
        from   postgis.acquisizioni inner join postgis.imgtypes using (id_imgtype)
        where  imgtype = 'TCI'
          and    extract(doy from dtime) = doy_in
          and    extract(year from dtime) = year_in;

    END IF;
    RAISE NOTICE 'ids %',id_acquisizione_in;

    INSERT INTO postgis.tci (id_acquisizione, rast)
    VALUES
    (id_acquisizione_in, ST_Tile(tci,512,512));

    RAISE NOTICE 'saved';
    RETURN return_rast;
END;
$BODY$;

ALTER FUNCTION postgis.calculate_tci(integer, integer)
    OWNER TO postgres;





-- FUNCTION: postgis.calculate_tci(integer, integer)

-- DROP FUNCTION postgis.calculate_tci(integer, integer);
-- VERSIONE 2.0 - attuale versione in produzione
CREATE OR REPLACE FUNCTION postgis.calculate_tci(
    doy_in integer,
    year_in integer)
    RETURNS minmaxrast
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE
AS $BODY$DECLARE
    --LST rasters

    tci RASTER;

    return_rast minmaxrast;
    nband INT;
    imgtype_in INT;
    id_acquisizione_in INT;

BEGIN

    RAISE NOTICE 'Calculating %, % for first iteration', doy_in, year_in;

    select id_imgtype into imgtype_in
    from   postgis.imgtypes
    where  imgtype = 'LST';

    RAISE NOTICE 'Get Last LST';
    select ST_Union(b.rast) into return_rast.originrast
    from   postgis.acquisizioni a inner join postgis.lst b using (id_acquisizione)
    where  extract(year from a.dtime) = year_in
      and    extract(doy from a.dtime) = doy_in
      and    id_imgtype = imgtype_in;

    nband := ST_NumBands(return_rast.originrast);
    IF nband is NULL THEN
        RAISE NOTICE 'LST %, % not found. Exit.',doy_in,year_in;
        RETURN NULL;
    END IF;

    return_rast.maxrast := st_addband(st_makeemptyraster(st_band(return_rast.originrast,1)),'16BUI'::text);
    return_rast.minrast := st_addband(st_makeemptyraster(st_band(return_rast.originrast,1)),'16BUI'::text);

    RAISE NOTICE 'Calculating min';
    SELECT ST_Union(rast,'MIN') INTO  return_rast.minrast
    FROM   postgis.acquisizioni INNER JOIN postgis.lst USING (id_acquisizione)
    WHERE  extract(doy from dtime) = doy_in
      AND    extract(year from dtime) < year_in
      AND    id_imgtype = imgtype_in;

    RAISE NOTICE 'Calculating max';
    SELECT ST_Union(rast,'MAX') INTO return_rast.maxrast
    FROM   postgis.acquisizioni INNER JOIN postgis.lst USING (id_acquisizione)
    WHERE  extract(doy from dtime) = doy_in
      AND    extract(year from dtime) < year_in
      AND    id_imgtype = imgtype_in;

    RAISE NOTICE 'Calculating tci';
    tci := ST_MapAlgebra(ARRAY[ROW(return_rast.maxrast,1), ROW(return_rast.minrast,1), ROW(return_rast.originrast,1)]::rastbandarg[],
                         'calculate_tci_raster(double precision[], int[], text[])'::regprocedure,
                         '32BF', 'LAST', null, 0, 0, null);

    RAISE NOTICE 'saving tci raster';
    IF EXISTS ( select id_acquisizione
                from   postgis.acquisizioni inner join postgis.imgtypes using (id_imgtype)
                where  imgtype = 'TCI'
                  and    extract(doy from dtime) = doy_in
                  and    extract(year from dtime) = year_in)  THEN

        RAISE NOTICE 'Found';

        select id_acquisizione into id_acquisizione_in
        from   postgis.acquisizioni inner join postgis.imgtypes using (id_imgtype)
        where  imgtype = 'TCI'
          and    extract(doy from dtime) = doy_in
          and    extract(year from dtime) = year_in;

        DELETE FROM postgis.tci
        WHERE  id_acquisizione = id_acquisizione_in;

    ELSE

        RAISE NOTICE 'not found, create';
        INSERT INTO postgis.acquisizioni (dtime, id_imgtype)
        VALUES (to_timestamp(''||year_in||' '||doy_in||'', 'YYYY DDD'),(select id_imgtype from postgis.imgtypes where imgtype='TCI'));

        select id_acquisizione into id_acquisizione_in
        from   postgis.acquisizioni inner join postgis.imgtypes using (id_imgtype)
        where  imgtype = 'TCI'
          and    extract(doy from dtime) = doy_in
          and    extract(year from dtime) = year_in;

    END IF;
    RAISE NOTICE 'ids %',id_acquisizione_in;

    INSERT INTO postgis.tci (id_acquisizione, rast)
    VALUES
    (id_acquisizione_in, ST_Tile(tci,512,512));

    RAISE NOTICE 'saved';
    RETURN return_rast;
END;
$BODY$;

ALTER FUNCTION postgis.calculate_tci(integer, integer)
    OWNER TO postgres;


-- Calculate_TCI --
-- VERSION 3.0 --
-- This version doesn't work with united tiles but with grouped tiles in a for cycle.
-- Each iteration calculates min, max and tci for tiles with the same boundaries
-- it is faster then previous version
CREATE OR REPLACE FUNCTION postgis.calculate_tci(
    doy_in integer,
    year_in integer)
    RETURNS boolean
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE
AS $BODY$DECLARE
    --LST rasters

    tci RASTER;
    maxrast RASTER;
    minrast RASTER;
    lst_i RECORD;
    icount INT;
    nband INT;
    imgtype_in INT;
    id_acquisizione_in INT;

BEGIN

    RAISE NOTICE 'Calculating %, % for first iteration', doy_in, year_in;

    select id_imgtype into imgtype_in
    from   postgis.imgtypes
    where  imgtype = 'LST';

    IF EXISTS ( select id_acquisizione
                from   postgis.acquisizioni inner join postgis.imgtypes using (id_imgtype)
                where  imgtype = 'TCI'
                  and    extract(doy from dtime) = doy_in
                  and    extract(year from dtime) = year_in)  THEN

        RAISE NOTICE 'Found';

        select id_acquisizione into id_acquisizione_in
        from   postgis.acquisizioni inner join postgis.imgtypes using (id_imgtype)
        where  imgtype = 'TCI'
          and    extract(doy from dtime) = doy_in
          and    extract(year from dtime) = year_in;

        RAISE NOTICE 'Deleting old tci';
        DELETE FROM postgis.tci
        WHERE  id_acquisizione = id_acquisizione_in;

    ELSE

        RAISE NOTICE 'not found, create';
        INSERT INTO postgis.acquisizioni (dtime, id_imgtype)
        VALUES (to_timestamp(''||year_in||' '||doy_in||'', 'YYYY DDD'),(select id_imgtype from postgis.imgtypes where imgtype='TCI'));

        select id_acquisizione into id_acquisizione_in
        from   postgis.acquisizioni inner join postgis.imgtypes using (id_imgtype)
        where  imgtype = 'TCI'
          and    extract(doy from dtime) = doy_in
          and    extract(year from dtime) = year_in;

    END IF;

    RAISE NOTICE 'Get Last LST tiles';
    icount := 0;
    FOR lst_i IN select rast, ST_Envelope(rast) as ext
                 from postgis.lst inner join postgis.acquisizioni
                                             using (id_acquisizione)
                 where extract(doy from dtime) = doy_in
                   and   extract(year from dtime) = year_in


        LOOP
            icount := icount + 1;
            RAISE NOTICE 'processing tile: %', icount;

            RAISE NOTICE 'calculating min, max';
            -- maxrast := st_addband(st_makeemptyraster(st_band(lst_i.rast,1)),'16BUI'::text);
            -- minrast := st_addband(st_makeemptyraster(st_band(lst_i.rast,1)),'16BUI'::text);

            select ST_Union(rast,'MIN'),ST_Union(rast,'MAX')
            into   minrast, maxrast
            from postgis.lst inner join postgis.acquisizioni
                                        using (id_acquisizione)
            where extract(doy from dtime) = doy_in
              and   extract(year from dtime) < year_in
              and   ST_Envelope(rast) = lst_i.ext;

            -- RAISE NOTICE 'min: %, % max: %, %', ST_Width(minrast), ST_Height(minrast), ST_Width(maxrast), ST_Height(maxrast);

            tci := ST_MapAlgebra(ARRAY[ROW(maxrast,1), ROW(minrast,1), ROW(lst_i.rast,1)]::rastbandarg[],
                                 'calculate_tci_raster(double precision[], int[], text[])'::regprocedure,
                                 '32BF', 'LAST', null, 0, 0, null);


            RAISE NOTICE 'saving tci raster';


            --	RAISE NOTICE 'ids %',id_acquisizione_in;
            --  RAISE NOTICE 'tci: %, %', ST_Width(tci), ST_Height(tci);

            INSERT INTO postgis.tci (id_acquisizione, rast)
            VALUES
            (id_acquisizione_in, ST_Tile(tci,240,240));

            RAISE NOTICE 'saved';
        END LOOP;


    RAISE NOTICE 'done';
    RETURN true;
END;
$BODY$;

ALTER FUNCTION postgis.calculate_tci(integer, integer)
    OWNER TO postgres;

---------------------------------------------------------

-- Calculate_TCI --
-- VERSION 3.1 --
-- This version doesn't work with united tiles but with grouped tiles in a for cycle.
-- Each iteration calculates min, max and tci for tiles with the same boundaries
-- it is faster then previous version
-- this version doesn't delete previous TCI
CREATE OR REPLACE FUNCTION postgis.calculate_tci(
    doy_in integer,
    year_in integer)
    RETURNS boolean
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE
AS $BODY$DECLARE
    --LST rasters

    tci RASTER;
    maxrast RASTER;
    minrast RASTER;
    lst_i RECORD;
    icount INT;
    nband INT;
    imgtype_in INT;
    id_acquisizione_in INT;

BEGIN

    RAISE NOTICE 'Calculating %, % for first iteration', doy_in, year_in;

    select id_imgtype into imgtype_in
    from   postgis.imgtypes
    where  imgtype = 'LST';

    IF EXISTS ( select id_acquisizione
                from   postgis.acquisizioni inner join postgis.imgtypes using (id_imgtype)
                where  imgtype = 'TCI'
                  and    extract(doy from dtime) = doy_in
                  and    extract(year from dtime) = year_in)  THEN

        RAISE NOTICE 'Found';

        select id_acquisizione into id_acquisizione_in
        from   postgis.acquisizioni inner join postgis.imgtypes using (id_imgtype)
        where  imgtype = 'TCI'
          and    extract(doy from dtime) = doy_in
          and    extract(year from dtime) = year_in;

    --    RAISE NOTICE 'Deleting old tci';
    --    DELETE FROM postgis.tci
    --    WHERE  id_acquisizione = id_acquisizione_in;

    ELSE

        RAISE NOTICE 'not found, create';
        INSERT INTO postgis.acquisizioni (dtime, id_imgtype)
        VALUES (to_timestamp(''||year_in||' '||doy_in||'', 'YYYY DDD'),(select id_imgtype from postgis.imgtypes where imgtype='TCI'));

        select id_acquisizione into id_acquisizione_in
        from   postgis.acquisizioni inner join postgis.imgtypes using (id_imgtype)
        where  imgtype = 'TCI'
          and    extract(doy from dtime) = doy_in
          and    extract(year from dtime) = year_in;

    END IF;

    RAISE NOTICE 'Get Last LST tiles';
    icount := 0;
    FOR lst_i IN select rast, ST_Envelope(rast) as ext
                 from postgis.lst inner join postgis.acquisizioni
                                             using (id_acquisizione)
                 where extract(doy from dtime) = doy_in
                   and   extract(year from dtime) = year_in


        LOOP
            icount := icount + 1;
            RAISE NOTICE 'processing tile: %', icount;

            RAISE NOTICE 'calculating min, max';
            -- maxrast := st_addband(st_makeemptyraster(st_band(lst_i.rast,1)),'16BUI'::text);
            -- minrast := st_addband(st_makeemptyraster(st_band(lst_i.rast,1)),'16BUI'::text);

            select ST_Union(rast,'MIN'),ST_Union(rast,'MAX')
            into   minrast, maxrast
            from postgis.lst inner join postgis.acquisizioni
                                        using (id_acquisizione)
            where extract(doy from dtime) = doy_in
              and   extract(year from dtime) < year_in
              and   ST_Envelope(rast) = lst_i.ext;

            -- RAISE NOTICE 'min: %, % max: %, %', ST_Width(minrast), ST_Height(minrast), ST_Width(maxrast), ST_Height(maxrast);

            tci := ST_MapAlgebra(ARRAY[ROW(maxrast,1), ROW(minrast,1), ROW(lst_i.rast,1)]::rastbandarg[],
                                 'calculate_tci_raster(double precision[], int[], text[])'::regprocedure,
                                 '32BF', 'LAST', null, 0, 0, null);

            RAISE NOTICE 'saving tci raster';


            --	RAISE NOTICE 'ids %',id_acquisizione_in;
            --  RAISE NOTICE 'tci: %, %', ST_Width(tci), ST_Height(tci);

            INSERT INTO postgis.tci (id_acquisizione, rast)
            VALUES
            (id_acquisizione_in, ST_Tile(tci,240,240));

            RAISE NOTICE 'saved';
        END LOOP;


    RAISE NOTICE 'done';
    RETURN true;
END;
$BODY$;

ALTER FUNCTION postgis.calculate_tci(integer, integer)
    OWNER TO postgres;



