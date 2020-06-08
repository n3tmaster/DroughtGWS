-- calculate tci
-- input
--  1- NDVI/EVI max
--  2- NDVI/EVI min
--  3- NDVI/EVI last
CREATE OR REPLACE FUNCTION postgis.calculate_vci_raster(value double precision[][][], pos integer[][], VARIADIC userargs text[])
	RETURNS double precision
	AS $$
	BEGIN



        if (value[1][1][1] - value[2][1][1]) = 0 then
            RETURN -1000;

        else

			RETURN (((value[3][1][1] - value[1][1][1]) / (value[1][1][1] - value[2][1][1])) * 100);

		end if;

	END;
	$$ LANGUAGE 'plpgsql' IMMUTABLE;





-- Create VCI image from EVI or NDVI serial
-- Input : dtime_in - timestamp - reference date for TCI calculation, use_evi: use evi image instead ndvi
--       : poly_in  - geometry  - polygon used for perfroming operation within this boundaries
--       : data_type - varchar -  data type used as source data (EVI or NDVI)
--       : store - store or update result
-- Output: VCI raster
create or replace function postgis.calculate_vci(
    dtime_in timestamp, poly_in geometry, data_type varchar, store boolean
    )
RETURNS RASTER AS
$BODY$
DECLARE
 --NDVI rasters
 ndvimax RASTER;
 ndvimin RASTER;
 ndvilast RASTER;
 ndvi_i   RASTER;

 --VCI raster
 vci RASTER;

 imgtype_in INT;
 vcitype_in INT;

 id_acquisizione_in INT;
 first_time BOOLEAN;
 doy_in INT := extract('doy' from dtime_in);
 year_in INT := extract('year' from dtime_in);


 acquisizioni_i RECORD;

BEGIN
 RAISE NOTICE 'Calculating VCI raster for doy: % and year: %',doy_in,year_in;

 first_time := TRUE;

 select id_imgtype into imgtype_in
 from   postgis.imgtypes
 where  imgtype = 'NDVI';

 select id_imgtype into vcitype_in
 from   postgis.imgtypes
 where  imgtype = 'VCI';

 RAISE NOTICE 'ImgType : %',imgtype_in;

 RAISE NOTICE 'Get Last NDVI';



 EXECUTE 'select ST_Union(b.rast) '
         'from postgis.acquisizioni a inner join postgis.'||data_type||' b using (id_acquisizione)
          where  extract(year from a.dtime) = $1
          and    extract(doy from a.dtime) =  $2
          and   ST_Intersects(b.rast,$3)
          and   a.id_imgtype = $4' INTO ndvilast USING year_in, doy_in, poly_in, imgtype_in;



 RAISE NOTICE 'Get min max rasters';

 EXECUTE 'select ST_Union(rast) '
         'from postgis.max_rasters
          where extract(doy from a.dtime) =  $1
          and   ST_Intersects(b.rast,$2)
          and   a.id_imgtype = $3' INTO ndvimax USING doy_in, poly_in, imgtype_in;

  EXECUTE 'select ST_Union(rast) '
         'from postgis.min_rasters
          where extract(doy from a.dtime) =  $1
          and   ST_Intersects(b.rast,$2)
          and   a.id_imgtype = $3' INTO ndvimin USING doy_in, poly_in, imgtype_in;


 vci    := st_addband(st_makeemptyraster(st_band(ndvilast,1)),'32BF'::text);



 RAISE NOTICE 'Calculate VCI raster...';
 vci := ST_MapAlgebra(ARRAY[ROW(ndvimax,1), ROW(ndvimin,1), ROW(ndvilast,1)]::rastbandarg[],
                            'calculate_vci_raster(double precision[], int[], text[])'::regprocedure,
                           	'32BF', 'LAST', null, 0, 0, null);



 -- IF store = TRUE THEN
 --    RAISE NOTICE 'Check if it exists...';
 --    id_acquisizione_in := -1;
 --    select id_acquisizione into id_acquisizione_in
 --               from postgis.acquisizioni
 --               where extract('doy' from dtime) = doy_in
 --               and    extract('year' from dtime) = year_in
 --               and   id_imgtype = vcitype_in;


 --    IF id_acquisizione_in <> -1 THEN
 --        RAISE NOTICE 'deleting old VCI...';

 --        delete from postgis.vci
 --        where  id_acquisizione = id_acquisizione_in;

 --        delete from postgis.acquisizioni
 --        where  id_acquisizione = id_acquisizione_in;

 --        RAISE NOTICE 'done';

 --    END IF;

 --    RAISE NOTICE 'create new acquisition...';
 --    insert into postgis.acquisizioni (dtime, id_imgtype)
 --    values (dtime_in, vcitype_in);


 --    select id_acquisizione into id_acquisizione_in
 --    from   postgis.acquisizioni
 --    where  dtime = dtime_in
 --    and    id_imgtype = vcitype_in;

 --    RAISE NOTICE 'save VCI...';
 --    insert into postgis.vci (id_acquisizione, rast)
 --    values (id_acquisizione_in, vci);
 --    RAISE NOTICE 'done';

 -- END IF;

 RAISE NOTICE 'Clipping...';

 vci := ST_Clip(vci, poly_in, true);

 RAISE NOTICE 'done.';
 RETURN vci;
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


create or replace function postgis.calculate_lst_max(
    dtime_in timestamp
    )
RETURNS RASTER AS
$BODY$
DECLARE
 --LST rasters
 lstmax RASTER;
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

 lstmax := st_addband(st_makeemptyraster(st_band(lstlast,1)),'16BUI'::text);



 RAISE NOTICE 'Calculating min and max rasters';
 FOR lst_i IN select b.rast as rastin
 			from postgis.acquisizioni a inner join postgis.lst b using (id_acquisizione)
            where extract('doy' from a.dtime) = doy_in
            and   extract('year' from a.dtime) <> year_in
            and   a.id_imgtype = imgtype_in
            order by a.dtime
 LOOP
 	lstmax := ST_MapAlgebra(ARRAY[ROW(lstmax,1), ROW(lst_i.rastin,1)]::rastbandarg[],
                            'ST_Max4ma(double precision[], int[], text[])'::regprocedure,
                           	'16BUI', 'LAST', null, 0, 0, null);


 END LOOP;


 RETURN lstmax;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE


create or replace function postgis.calculate_evci(
    dtime_in timestamp, store boolean
    )
RETURNS RASTER AS
$BODY$
DECLARE
 --NDVI rasters
 ndvimax RASTER;
 ndvimin RASTER;
 ndvilast RASTER;

 --VCI raster
 vci RASTER;

 imgtype_in INT;
 vcitype_in INT;

 id_acquisizione_in INT;
 first_time BOOLEAN;
 doy_in INT := extract('doy' from dtime_in);
 year_in INT := extract('year' from dtime_in);

 vci_i RECORD;

BEGIN
 RAISE NOTICE 'Calculating VCI raster for doy: % and year: %',doy_in,year_in;

 first_time := TRUE;

 select id_imgtype into imgtype_in
 from   postgis.imgtypes
 where  imgtype = 'EVI';

 select id_imgtype into vcitype_in
 from   postgis.imgtypes
 where  imgtype = 'EVCI';

 RAISE NOTICE 'ImgType : %',imgtype_in;

 RAISE NOTICE 'Get Last EVI';

 select (b.rast) into ndvilast
 from postgis.acquisizioni a inner join postgis.evi b using (id_acquisizione)
 where  extract('year' from a.dtime) = year_in
 and   extract('doy' from a.dtime) = doy_in
 and   a.id_imgtype = imgtype_in;

 RAISE NOTICE 'Prepare ndvimax and ndvimin matrix';

 ndvimax := st_addband(st_makeemptyraster(st_band(ndvilast,1)),'16BSI'::text);
 ndvimin := st_addband(st_makeemptyraster(st_band(ndvilast,1)),'16BSI'::text);
 vci    := st_addband(st_makeemptyraster(st_band(ndvilast,1)),'32BF'::text);

 RAISE NOTICE 'Calculating min and max rasters';
 FOR vci_i IN select (b.rast) as rastin
 			from postgis.acquisizioni a inner join postgis.evi b using (id_acquisizione)
            where extract('doy' from a.dtime) = doy_in
            and   extract('year' from a.dtime) < year_in
            and   a.id_imgtype = imgtype_in
            order by a.dtime
 LOOP

    RAISE NOTICE 'Ciclo';
 	ndvimax := ST_MapAlgebra(ARRAY[ROW(ndvimax,1), ROW(vci_i.rastin,1)]::rastbandarg[],
                            'ST_Max4ma(double precision[], int[], text[])'::regprocedure,
                           	'16BSI', 'LAST', null, 0, 0, null);

    ndvimin := ST_MapAlgebra(ARRAY[ROW(ndvimin,1), ROW(vci_i.rastin,1)]::rastbandarg[],
                            'ST_Min4ma(double precision[], int[], text[])'::regprocedure,
                           	'16BSI', 'LAST', null, 0, 0, null);
 END LOOP;

 RAISE NOTICE 'Calculate VCI raster...';
 vci := ST_MapAlgebra(ARRAY[ROW(ndvimax,1), ROW(ndvimin,1), ROW(ndvilast,1)]::rastbandarg[],
                            'calculate_vci_raster(double precision[], int[], text[])'::regprocedure,
                           	'32BF', 'LAST', null, 0, 0, null);



 IF store = TRUE THEN
    RAISE NOTICE 'Check if it exists...';
    id_acquisizione_in := -1;
    select id_acquisizione into id_acquisizione_in
               from postgis.acquisizioni
               where extract('doy' from dtime) = doy_in
               and    extract('year' from dtime) = year_in
               and   id_imgtype = vcitype_in;


    IF id_acquisizione_in <> -1 THEN
        RAISE NOTICE 'deleting old EVCI...';

        delete from postgis.vci
        where  id_acquisizione = id_acquisizione_in;

        delete from postgis.acquisizioni
        where  id_acquisizione = id_acquisizione_in;

        RAISE NOTICE 'done';

    END IF;

    RAISE NOTICE 'create new acquisition...';
    insert into postgis.acquisizioni (dtime, id_imgtype)
    values (dtime_in, vcitype_in);


    select id_acquisizione into id_acquisizione_in
    from   postgis.acquisizioni
    where  dtime = dtime_in
    and    id_imgtype = vcitype_in;

    RAISE NOTICE 'save EVCI...';
    insert into postgis.evci (id_acquisizione, rast)
    values (id_acquisizione_in, ST_Tile(vci,240,240));
    RAISE NOTICE 'done';

 END IF;

 RAISE NOTICE 'done.';
 RETURN vci;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE




-- calculate VCI - LAST VERSION 28 Agosto 2018
create or replace function postgis.calculate_vci(doy_in int, year_in int)
RETURNS BOOLEAN AS
$BODY$
DECLARE
 --LST rasters
 ndvimax RASTER;
 ndvimin RASTER;
 vci    RASTER;
 ndvi_last RASTER;



 imgtype_in INT;

 id_acquisizione_in INT;


BEGIN

    RAISE NOTICE 'Calculating %, %', doy_in, year_in;

	select id_imgtype into imgtype_in
	from   postgis.imgtypes
	where  imgtype = 'NDVI';

    select id_acquisizione into id_acquisizione_in
	from   postgis.acquisizioni inner join postgis.imgtypes using (id_imgtype)
	where  imgtype = 'VCI'
	and    extract(doy from dtime) = doy_in
	and    extract(year from dtime) = year_in;

	RAISE NOTICE 'Get Last NDVI';
 	select ST_Union(b.rast) into ndvi_last
	from   postgis.acquisizioni a inner join postgis.ndvi b using (id_acquisizione)
	where  extract(year from a.dtime) = year_in
	and    extract(doy from a.dtime) = doy_in
	and    a.id_imgtype = imgtype_in;

	ndvimax := st_addband(st_makeemptyraster(st_band(ndvi_last,1)),'16BUI'::text);
    ndvimin := st_addband(st_makeemptyraster(st_band(ndvi_last,1)),'16BUI'::text);


	RAISE NOTICE 'Calculating min max';
    SELECT ST_Union(rast,'MAX'), ST_Union(rast,'MIN') INTO ndvimax, ndvimin
    FROM   postgis.acquisizioni INNER JOIN postgis.ndvi USING (id_acquisizione)
    WHERE  extract(doy from dtime) = doy_in
    AND    extract(year from dtime) < year_in
    AND    id_imgtype = imgtype_in;


    RAISE NOTICE 'Calculating vci';
	vci := ST_MapAlgebra(ARRAY[ROW(ndvimax,1), ROW(ndvimin,1), ROW(ndvi_last,1)]::rastbandarg[],
                            'calculate_tci_raster(double precision[], int[], text[])'::regprocedure,
                           	'32BF', 'LAST', null, 0, 0, null);

	RAISE NOTICE 'saving min raster';
	IF EXISTS (SELECT id_acquisizione
			FROM postgis.vci
			INNER JOIN postgis.acquisizioni USING (id_acquisizione)
			WHERE extract(doy from dtime)=doy_in
			AND   extract(year from dtime)=year_in) THEN

			RAISE NOTICE 'Found';

			DELETE FROM postgis.vci
			WHERE  id_acquisizione = id_acquisizione_in;
	END IF;
	RAISE NOTICE 'ids %',id_acquisizione_in;

	INSERT INTO postgis.vci (id_acquisizione, rast)
	VALUES
	(id_acquisizione_in, ST_Tile(vci,240,240));

    RAISE NOTICE 'saved';
 RETURN TRUE;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE;

-- populate VCI - LAST VERSION 28 Agosto 2018
create or replace function postgis.populate_vci()
RETURNS BOOLEAN AS
$BODY$
DECLARE
 --LST rasters

 lst_i RECORD;
 retcode boolean;
BEGIN

	FOR lst_i IN select extract(doy from dtime) as doy_out,
						extract(year from dtime) as year_out
					from postgis.acquisizioni inner join postgis.imgtypes using (id_imgtype)
					where imgtype = 'NDVI'
					order by dtime

	LOOP
		RAISE NOTICE 'Processing doy: % - year: %', lst_i.doy_out, lst_i.year_out;
		retcode := postgis.calculate_vci(lst_i.doy_out, lst_i.year_out);
		RAISE NOTICE 'Done.';

	END LOOP;
 RETURN TRUE;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE;


-- populate VCI - LAST VERSION 28 Agosto 2018 con tempo parametrico
create or replace function postgis.populate_vci(doy_begin INT, year_begin INT,
											   	doy_end INT, year_end INT)
RETURNS BOOLEAN AS
$BODY$
DECLARE
 --LST rasters

 lst_i RECORD;
 retcode boolean;
BEGIN

	FOR lst_i IN select extract(doy from dtime) as doy_out,
						extract(year from dtime) as year_out
					from postgis.acquisizioni inner join postgis.imgtypes using (id_imgtype)
					where imgtype = 'NDVI'
					and    extract(doy from dtime) between doy_begin and doy_end
					and    extract(year from dtime) between year_begin and year_end
					order by dtime

	LOOP
		RAISE NOTICE 'Processing doy: % - year: %', lst_i.doy_out, lst_i.year_out;
		retcode := postgis.calculate_vci(lst_i.doy_out, lst_i.year_out);
		RAISE NOTICE 'Done.';

	END LOOP;
 RETURN TRUE;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE;





  -- VCI version 4.0 05-09-2018
--  This version optimizes min and max calculation during the whole process
--  it is composed by 2 main function, the first is used in first iteration
--  the second one is used for the other iterations
--  these functions use the custom type minmaxrast for passing min and max rasters
--  calculated in each iteration. In this way postgresql will not have to recalculate min and max for whole set of rasters
--  but only for the last doy.

--  function for the first iteration
create or replace function postgis.calculate_vci(doy_in int, year_in int, zero_value int)
RETURNS minmaxrast AS
$BODY$
DECLARE
 --LST rasters

 vci    RASTER;


 return_rast minmaxrast;
 num_element INT;
 imgtype_in INT;
 id_acquisizione_in INT;
 ndvi_i RECORD;

BEGIN

    RAISE NOTICE 'Calculating %, % for first iteration', doy_in, year_in;


	select id_imgtype into imgtype_in
	from   postgis.imgtypes
	where  imgtype = 'NDVI';

	RAISE NOTICE 'Get Last NDVI';
 	select ST_Union(b.rast) into return_rast.originrast
	from   postgis.acquisizioni a inner join postgis.ndvi b using (id_acquisizione)
	where  extract(year from a.dtime) = year_in
	and    extract(doy from a.dtime) = doy_in
	and    id_imgtype = imgtype_in;

	return_rast.maxrast := st_addband(st_makeemptyraster(st_band(return_rast.originrast,1)),'16BSI'::text);
    return_rast.minrast := st_addband(st_makeemptyraster(st_band(return_rast.originrast,1)),'16BSI'::text);

    RAISE NOTICE 'Calculating check threshold';
	select count(*) into num_element
	from postgis.acquisizioni a
	where extract(doy from a.dtime) = doy_in
	and   extract(year from a.dtime) < year_in
	and   a.id_imgtype = imgtype_in;

	RAISE NOTICE 'Number of elements: %',num_element;
	return_rast.perc_threshold := (num_element / 100.0) * 25.0;
	RAISE NOTICE 'Perc: %',return_rast.perc_threshold;

	RAISE NOTICE 'Calculating 0 occurrencies with %',return_rast.perc_threshold;
	FOR ndvi_i IN select ST_Union(b.rast) as rastin
				from postgis.acquisizioni a inner join postgis.ndvi b using (id_acquisizione)
				where extract(doy from a.dtime) = doy_in
				and   extract(year from a.dtime) < year_in
				and   a.id_imgtype = imgtype_in

	LOOP

		return_rast.zero_values := ST_MapAlgebra(ARRAY[ROW(return_rast.zero_values,1), ROW(ndvi_i.rastin,1)]::rastbandarg[],
			'calc_zero_occurrencies(double precision[], int[], text[])'::regprocedure,
			'16BUI', 'LAST', null, 0, 0,  VARIADIC ARRAY[zero_value]::text[]);

	END LOOP;


	RAISE NOTICE 'Calculating min max';
	FOR ndvi_i IN select ST_Union(b.rast) as rastin
				from postgis.acquisizioni a inner join postgis.ndvi b using (id_acquisizione)
				where extract(doy from a.dtime) = doy_in
				and   extract(year from a.dtime) < year_in
				and   a.id_imgtype = imgtype_in

	LOOP

		return_rast.minrast := ST_MapAlgebra(ARRAY[ROW(return_rast.zero_values,1), ROW(return_rast.minrast,1), ROW(ndvi_i.rastin,1)]::rastbandarg[],
			'calc_min(double precision[], int[], text[])'::regprocedure,
			'16BSI', 'LAST', null, 0, 0, VARIADIC ARRAY[return_rast.perc_threshold]::text[]);

		return_rast.maxrast := ST_MapAlgebra(ARRAY[ROW(return_rast.zero_values,1), ROW(return_rast.maxrast,1), ROW(ndvi_i.rastin,1)]::rastbandarg[],
			'calc_max(double precision[], int[], text[])'::regprocedure,
			'16BSI', 'LAST', null, 0, 0, VARIADIC ARRAY[return_rast.perc_threshold]::text[]);

	END LOOP;





    RAISE NOTICE 'Calculating vci';
	vci := ST_MapAlgebra(ARRAY[ROW(return_rast.maxrast,1), ROW(return_rast.minrast,1), ROW(return_rast.originrast,1)]::rastbandarg[],
                            'calculate_vci_raster(double precision[], int[], text[])'::regprocedure,
                           	'32BF', 'LAST', null, 0, 0, null);

	RAISE NOTICE 'saving vci raster';
    IF EXISTS ( select id_acquisizione
	            from   postgis.acquisizioni inner join postgis.imgtypes using (id_imgtype)
	            where  imgtype = 'VCI'
	            and    extract(doy from dtime) = doy_in
	            and    extract(year from dtime) = year_in)  THEN

        RAISE NOTICE 'Found';

        select id_acquisizione into id_acquisizione_in
	    from   postgis.acquisizioni inner join postgis.imgtypes using (id_imgtype)
	    where  imgtype = 'VCI'
	    and    extract(doy from dtime) = doy_in
	    and    extract(year from dtime) = year_in;

	    DELETE FROM postgis.vci
		WHERE  id_acquisizione = id_acquisizione_in;

	ELSE

        RAISE NOTICE 'not found, create';
        INSERT INTO postgis.acquisizioni (dtime, id_imgtype)
        VALUES (to_timestamp(''||year_in||' '||doy_in||'', 'YYYY DDD'),(select id_imgtype from postgis.imgtypes where imgtype='VCI'));

        select id_acquisizione into id_acquisizione_in
        from   postgis.acquisizioni inner join postgis.imgtypes using (id_imgtype)
        where  imgtype = 'VCI'
        and    extract(doy from dtime) = doy_in
        and    extract(year from dtime) = year_in;


	END IF;
	RAISE NOTICE 'ids %',id_acquisizione_in;

    INSERT INTO postgis.vci (id_acquisizione, rast)
	VALUES
	(id_acquisizione_in, ST_Tile(vci,240,240));




    RAISE NOTICE 'saved';
 RETURN return_rast;
END;
$BODY$
  LANGUAGE plpgsql;





--  function for the other iterations
create or replace function postgis.calculate_vci(doy_in int, year_in int, return_rast minmaxrast, zero_value int)
RETURNS minmaxrast AS
$BODY$
DECLARE
 --LST rasters

 vci    RASTER;

 ndvi_last RASTER;
 imgtype_in INT;

 id_acquisizione_in INT;


BEGIN

    RAISE NOTICE 'Calculating %, %', doy_in, year_in;

	select id_imgtype into imgtype_in
	from   postgis.imgtypes
	where  imgtype = 'NDVI';



	RAISE NOTICE 'Get Last NDVI';
 	select ST_Union(b.rast) into ndvi_last
	from   postgis.acquisizioni a inner join postgis.ndvi b using (id_acquisizione)
	where  extract(year from a.dtime) = year_in
	and    extract(doy from a.dtime) = doy_in
	and    a.id_imgtype = imgtype_in;


    RAISE NOTICE 'calculating zeros';
    return_rast.zero_values := ST_MapAlgebra(ARRAY[ROW(return_rast.zero_values,1), ROW(return_rast.originrast,1)]::rastbandarg[],
			'calc_zero_occurrencies(double precision[], int[], text[])'::regprocedure,
			'16BUI', 'LAST', null, 0, 0,  VARIADIC ARRAY[zero_value]::text[]);

    RAISE NOTICE 'calculating min';
    return_rast.minrast := ST_MapAlgebra(ARRAY[ROW(return_rast.minrast,1), ROW(return_rast.originrast,1)]::rastbandarg[],
                            'calc_min(double precision[], int[], text[])'::regprocedure,
                           	'16BSI', 'LAST', null, 0, 0, null);

    RAISE NOTICE 'calculating max';
    return_rast.maxrast := ST_MapAlgebra(ARRAY[ROW(return_rast.maxrast,1), ROW(return_rast.originrast,1)]::rastbandarg[],
                            'calc_max(double precision[], int[], text[])'::regprocedure,
                           	'16BSI', 'LAST', null, 0, 0, null);

    RAISE NOTICE 'Calculating vci';
	vci := ST_MapAlgebra(ARRAY[ROW(return_rast.maxrast,1), ROW(return_rast.minrast,1), ROW(ndvi_last,1)]::rastbandarg[],
                            'calculate_vci_raster(double precision[], int[], text[])'::regprocedure,
                           	'32BF', 'LAST', null, 0, 0, null);

    return_rast.originrast := ndvi_last;

	RAISE NOTICE 'saving vci raster';
	IF EXISTS (  select id_acquisizione
	            from   postgis.acquisizioni inner join postgis.imgtypes using (id_imgtype)
	            where  imgtype = 'VCI'
	            and    extract(doy from dtime) = doy_in
	            and    extract(year from dtime) = year_in)  THEN

        RAISE NOTICE 'Found';

        select id_acquisizione into id_acquisizione_in
	    from   postgis.acquisizioni inner join postgis.imgtypes using (id_imgtype)
	    where  imgtype = 'VCI'
	    and    extract(doy from dtime) = doy_in
	    and    extract(year from dtime) = year_in;

	    DELETE FROM postgis.vci
		WHERE  id_acquisizione = id_acquisizione_in;

	ELSE

        RAISE NOTICE 'not found, create';
        INSERT INTO postgis.acquisizioni (dtime, id_imgtype)
        VALUES (to_timestamp(''||year_in||' '||doy_in||'', 'YYYY DDD'),(select id_imgtype from postgis.imgtypes where imgtype='VCI'));

        select id_acquisizione into id_acquisizione_in
        from   postgis.acquisizioni inner join postgis.imgtypes using (id_imgtype)
        where  imgtype = 'VCI'
        and    extract(doy from dtime) = doy_in
        and    extract(year from dtime) = year_in;


	END IF;
	RAISE NOTICE 'ids %',id_acquisizione_in;

	INSERT INTO postgis.vci (id_acquisizione, rast)
	VALUES
	(id_acquisizione_in, ST_Tile(vci,240,240));

    RAISE NOTICE 'saved';
 RETURN return_rast;
END;
$BODY$
  LANGUAGE plpgsql;

-- main function for generating vci rasters
create or replace function postgis.populate_vci(doy_begin INT, year_begin INT,
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
					where imgtype = 'NDVI'
					and    extract(doy from dtime) between doy_begin and doy_end
					and    extract(year from dtime) between year_begin and year_end
					order by 1,2

	LOOP
		RAISE NOTICE 'Processing doy: % - year: %', lst_i.doy_out, lst_i.year_out;
		IF actual_doy < lst_i.doy_out then
		    return_rast := postgis.calculate_vci(lst_i.doy_out, lst_i.year_out,-3000);

		    actual_doy := lst_i.doy_out;
		else
		    return_rast := postgis.calculate_vci(lst_i.doy_out, lst_i.year_out, return_rast, -3000);
		end if;


		RAISE NOTICE 'Done.';

	END LOOP;
 RETURN TRUE;
END;
$BODY$
  LANGUAGE plpgsql;




  -- EVCI version 4.0 05-09-2018
--  This version optimizes min and max calculation during the whole process
--  it is composed by 2 main function, the first is used in first iteration
--  the second one is used for the other iterations
--  these functions use the custom type minmaxrast for passing min and max rasters
--  calculated in each iteration. In this way postgresql will not have to recalculate min and max for whole set of rasters
--  but only for the last doy.

--  function for the first iteration
create or replace function postgis.calculate_evci(doy_in int, year_in int, zero_value int)
RETURNS minmaxrast AS
$BODY$
DECLARE
 --LST rasters

 evci    RASTER;


 return_rast minmaxrast;
 num_element INT;
 imgtype_in INT;
 id_acquisizione_in INT;
 evi_i RECORD;

BEGIN

    RAISE NOTICE 'Calculating %, % for first iteration', doy_in, year_in;


	select id_imgtype into imgtype_in
	from   postgis.imgtypes
	where  imgtype = 'EVI';

	RAISE NOTICE 'Get Last EVI';
 	select ST_Union(b.rast) into return_rast.originrast
	from   postgis.acquisizioni a inner join postgis.evi b using (id_acquisizione)
	where  extract(year from a.dtime) = year_in
	and    extract(doy from a.dtime) = doy_in
	and    id_imgtype = imgtype_in;

	return_rast.maxrast := st_addband(st_makeemptyraster(st_band(return_rast.originrast,1)),'16BSI'::text);
    return_rast.minrast := st_addband(st_makeemptyraster(st_band(return_rast.originrast,1)),'16BSI'::text);

    RAISE NOTICE 'Calculating check threshold';
	select count(*) into num_element
	from postgis.acquisizioni a
	where extract(doy from a.dtime) = doy_in
	and   extract(year from a.dtime) < year_in
	and   a.id_imgtype = imgtype_in;

	RAISE NOTICE 'Number of elements: %',num_element;
	return_rast.perc_threshold := (num_element / 100.0) * 25.0;
	RAISE NOTICE 'Perc: %',return_rast.perc_threshold;

	RAISE NOTICE 'Calculating 0 occurrencies with %',return_rast.perc_threshold;
	FOR evi_i IN select ST_Union(b.rast) as rastin
				from postgis.acquisizioni a inner join postgis.evi b using (id_acquisizione)
				where extract(doy from a.dtime) = doy_in
				and   extract(year from a.dtime) < year_in
				and   a.id_imgtype = imgtype_in

	LOOP

		return_rast.zero_values := ST_MapAlgebra(ARRAY[ROW(return_rast.zero_values,1), ROW(evi_i.rastin,1)]::rastbandarg[],
			'calc_zero_occurrencies(double precision[], int[], text[])'::regprocedure,
			'16BUI', 'LAST', null, 0, 0,  VARIADIC ARRAY[zero_value]::text[]);

	END LOOP;


	RAISE NOTICE 'Calculating min max';
	FOR evi_i IN select ST_Union(b.rast) as rastin
				from postgis.acquisizioni a inner join postgis.evi b using (id_acquisizione)
				where extract(doy from a.dtime) = doy_in
				and   extract(year from a.dtime) < year_in
				and   a.id_imgtype = imgtype_in

	LOOP

		return_rast.minrast := ST_MapAlgebra(ARRAY[ROW(return_rast.zero_values,1), ROW(return_rast.minrast,1), ROW(evi_i.rastin,1)]::rastbandarg[],
			'calc_min(double precision[], int[], text[])'::regprocedure,
			'16BSI', 'LAST', null, 0, 0, VARIADIC ARRAY[return_rast.perc_threshold]::text[]);

		return_rast.maxrast := ST_MapAlgebra(ARRAY[ROW(return_rast.zero_values,1), ROW(return_rast.maxrast,1), ROW(evi_i.rastin,1)]::rastbandarg[],
			'calc_max(double precision[], int[], text[])'::regprocedure,
			'16BSI', 'LAST', null, 0, 0, VARIADIC ARRAY[return_rast.perc_threshold]::text[]);

	END LOOP;





    RAISE NOTICE 'Calculating evci';
	evci := ST_MapAlgebra(ARRAY[ROW(return_rast.maxrast,1), ROW(return_rast.minrast,1), ROW(return_rast.originrast,1)]::rastbandarg[],
                            'calculate_vci_raster(double precision[], int[], text[])'::regprocedure,
                           	'32BF', 'LAST', null, 0, 0, null);

	RAISE NOTICE 'saving evci raster';
    IF EXISTS ( select id_acquisizione
	            from   postgis.acquisizioni inner join postgis.imgtypes using (id_imgtype)
	            where  imgtype = 'EVCI'
	            and    extract(doy from dtime) = doy_in
	            and    extract(year from dtime) = year_in)  THEN

        RAISE NOTICE 'Found';

        select id_acquisizione into id_acquisizione_in
	    from   postgis.acquisizioni inner join postgis.imgtypes using (id_imgtype)
	    where  imgtype = 'EVCI'
	    and    extract(doy from dtime) = doy_in
	    and    extract(year from dtime) = year_in;

	    DELETE FROM postgis.evci
		WHERE  id_acquisizione = id_acquisizione_in;

	ELSE

        RAISE NOTICE 'not found, create';
        INSERT INTO postgis.acquisizioni (dtime, id_imgtype)
        VALUES (to_timestamp(''||year_in||' '||doy_in||'', 'YYYY DDD'),(select id_imgtype from postgis.imgtypes where imgtype='EVCI'));

        select id_acquisizione into id_acquisizione_in
        from   postgis.acquisizioni inner join postgis.imgtypes using (id_imgtype)
        where  imgtype = 'EVCI'
        and    extract(doy from dtime) = doy_in
        and    extract(year from dtime) = year_in;


	END IF;
	RAISE NOTICE 'ids %',id_acquisizione_in;

    INSERT INTO postgis.evci (id_acquisizione, rast)
	VALUES
	(id_acquisizione_in, ST_Tile(evci,240,240));




    RAISE NOTICE 'saved';
 RETURN return_rast;
END;
$BODY$
  LANGUAGE plpgsql;





--  function for the other iterations
create or replace function postgis.calculate_evci(doy_in int, year_in int, return_rast minmaxrast, zero_value int)
RETURNS minmaxrast AS
$BODY$
DECLARE
 --LST rasters

 evci    RASTER;

 evi_last RASTER;
 imgtype_in INT;

 id_acquisizione_in INT;


BEGIN

    RAISE NOTICE 'Calculating %, %', doy_in, year_in;

	select id_imgtype into imgtype_in
	from   postgis.imgtypes
	where  imgtype = 'EVI';



	RAISE NOTICE 'Get Last EVI';
 	select ST_Union(b.rast) into evi_last
	from   postgis.acquisizioni a inner join postgis.evi b using (id_acquisizione)
	where  extract(year from a.dtime) = year_in
	and    extract(doy from a.dtime) = doy_in
	and    a.id_imgtype = imgtype_in;


    RAISE NOTICE 'calculating zeros';
    return_rast.zero_values := ST_MapAlgebra(ARRAY[ROW(return_rast.zero_values,1), ROW(return_rast.originrast,1)]::rastbandarg[],
			'calc_zero_occurrencies(double precision[], int[], text[])'::regprocedure,
			'16BUI', 'LAST', null, 0, 0,  VARIADIC ARRAY[zero_value]::text[]);

    RAISE NOTICE 'calculating min';
    return_rast.minrast := ST_MapAlgebra(ARRAY[ROW(return_rast.minrast,1), ROW(return_rast.originrast,1)]::rastbandarg[],
                            'calc_min(double precision[], int[], text[])'::regprocedure,
                           	'16BSI', 'LAST', null, 0, 0, null);

    RAISE NOTICE 'calculating max';
    return_rast.maxrast := ST_MapAlgebra(ARRAY[ROW(return_rast.maxrast,1), ROW(return_rast.originrast,1)]::rastbandarg[],
                            'calc_max(double precision[], int[], text[])'::regprocedure,
                           	'16BSI', 'LAST', null, 0, 0, null);

    RAISE NOTICE 'Calculating vci';
	evci := ST_MapAlgebra(ARRAY[ROW(return_rast.maxrast,1), ROW(return_rast.minrast,1), ROW(evi_last,1)]::rastbandarg[],
                            'calculate_vci_raster(double precision[], int[], text[])'::regprocedure,
                           	'32BF', 'LAST', null, 0, 0, null);

    return_rast.originrast := evi_last;

	RAISE NOTICE 'saving evci raster';
	IF EXISTS (  select id_acquisizione
	            from   postgis.acquisizioni inner join postgis.imgtypes using (id_imgtype)
	            where  imgtype = 'EVCI'
	            and    extract(doy from dtime) = doy_in
	            and    extract(year from dtime) = year_in)  THEN

        RAISE NOTICE 'Found';

        select id_acquisizione into id_acquisizione_in
	    from   postgis.acquisizioni inner join postgis.imgtypes using (id_imgtype)
	    where  imgtype = 'EVCI'
	    and    extract(doy from dtime) = doy_in
	    and    extract(year from dtime) = year_in;

	    DELETE FROM postgis.evci
		WHERE  id_acquisizione = id_acquisizione_in;

	ELSE

        RAISE NOTICE 'not found, create';
        INSERT INTO postgis.acquisizioni (dtime, id_imgtype)
        VALUES (to_timestamp(''||year_in||' '||doy_in||'', 'YYYY DDD'),(select id_imgtype from postgis.imgtypes where imgtype='EVCI'));

        select id_acquisizione into id_acquisizione_in
        from   postgis.acquisizioni inner join postgis.imgtypes using (id_imgtype)
        where  imgtype = 'EVCI'
        and    extract(doy from dtime) = doy_in
        and    extract(year from dtime) = year_in;


	END IF;
	RAISE NOTICE 'ids %',id_acquisizione_in;

	INSERT INTO postgis.evci (id_acquisizione, rast)
	VALUES
	(id_acquisizione_in, ST_Tile(evci,240,240));

    RAISE NOTICE 'saved';
 RETURN return_rast;
END;
$BODY$
  LANGUAGE plpgsql;

-- main function for generating evci rasters
create or replace function postgis.populate_evci(doy_begin INT, year_begin INT,
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
					where imgtype = 'EVI'
					and    extract(doy from dtime) between doy_begin and doy_end
					and    extract(year from dtime) between year_begin and year_end
					order by 1,2

	LOOP
		RAISE NOTICE 'Processing doy: % - year: %', lst_i.doy_out, lst_i.year_out;
		IF actual_doy < lst_i.doy_out then
		    return_rast := postgis.calculate_evci(lst_i.doy_out, lst_i.year_out,-3000);

		    actual_doy := lst_i.doy_out;
		else
		    return_rast := postgis.calculate_evci(lst_i.doy_out, lst_i.year_out, return_rast, -3000);
		end if;


		RAISE NOTICE 'Done.';

	END LOOP;
 RETURN TRUE;
END;
$BODY$
  LANGUAGE plpgsql;



-- VCI version 3.0 29-08-2018
--  This version optimizes min and max calculation during the whole process
--  it is composed by 2 main function, the first is used in first iteration
--  the second one is used for the other iterations
--  these functions use the custom type minmaxrast for passing min and max rasters
--  calculated in each iteration. In this way postgresql will not have to recalculate min and max for whole set of rasters
--  but only for the last doy.

--  function for the first iteration
create or replace function postgis.calculate_vci(doy_in int, year_in int)
RETURNS minmaxrast AS
$BODY$
DECLARE
 --LST rasters

 vci    RASTER;


 return_rast minmaxrast;
 puppa INT;
 imgtype_in INT;
 id_acquisizione_in INT;


BEGIN

    RAISE NOTICE 'Calculating %, % for first iteration', doy_in, year_in;


	select id_imgtype into imgtype_in
	from   postgis.imgtypes
	where  imgtype = 'NDVI';

	RAISE NOTICE 'Get Last NDVI';
 	select ST_Union(b.rast) into return_rast.originrast
	from   postgis.acquisizioni a inner join postgis.ndvi b using (id_acquisizione)
	where  extract(year from a.dtime) = year_in
	and    extract(doy from a.dtime) = doy_in
	and    id_imgtype = imgtype_in;

	return_rast.maxrast := st_addband(st_makeemptyraster(st_band(return_rast.originrast,1)),'16BSI'::text);
    return_rast.minrast := st_addband(st_makeemptyraster(st_band(return_rast.originrast,1)),'16BSI'::text);


	RAISE NOTICE 'Calculating min';
    SELECT ST_Union(rast,'MIN') INTO  return_rast.minrast
    FROM   postgis.acquisizioni INNER JOIN postgis.ndvi USING (id_acquisizione)
    WHERE  extract(doy from dtime) = doy_in
    AND    extract(year from dtime) < year_in
    AND    id_imgtype = imgtype_in;

    RAISE NOTICE 'Calculating max';
    SELECT ST_Union(rast,'MAX') INTO return_rast.maxrast
    FROM   postgis.acquisizioni INNER JOIN postgis.ndvi USING (id_acquisizione)
    WHERE  extract(doy from dtime) = doy_in
    AND    extract(year from dtime) < year_in
    AND    id_imgtype = imgtype_in;


    RAISE NOTICE 'Calculating vci';
	vci := ST_MapAlgebra(ARRAY[ROW(return_rast.maxrast,1), ROW(return_rast.minrast,1), ROW(return_rast.originrast,1)]::rastbandarg[],
                            'calculate_vci_raster(double precision[], int[], text[])'::regprocedure,
                           	'32BF', 'LAST', null, 0, 0, null);

	RAISE NOTICE 'saving vci raster';
    IF EXISTS ( select id_acquisizione
	            from   postgis.acquisizioni inner join postgis.imgtypes using (id_imgtype)
	            where  imgtype = 'VCI'
	            and    extract(doy from dtime) = doy_in
	            and    extract(year from dtime) = year_in)  THEN

        RAISE NOTICE 'Found';

        select id_acquisizione into id_acquisizione_in
	    from   postgis.acquisizioni inner join postgis.imgtypes using (id_imgtype)
	    where  imgtype = 'VCI'
	    and    extract(doy from dtime) = doy_in
	    and    extract(year from dtime) = year_in;

	    DELETE FROM postgis.vci
		WHERE  id_acquisizione = id_acquisizione_in;

	ELSE

        RAISE NOTICE 'not found, create';
        INSERT INTO postgis.acquisizioni (dtime, id_imgtype)
        VALUES (to_timestamp(''||year_in||' '||doy_in||'', 'YYYY DDD'),(select id_imgtype from postgis.imgtypes where imgtype='VCI'));

        select id_acquisizione into id_acquisizione_in
        from   postgis.acquisizioni inner join postgis.imgtypes using (id_imgtype)
        where  imgtype = 'VCI'
        and    extract(doy from dtime) = doy_in
        and    extract(year from dtime) = year_in;


	END IF;
	RAISE NOTICE 'ids %',id_acquisizione_in;

    INSERT INTO postgis.vci (id_acquisizione, rast)
	VALUES
	(id_acquisizione_in, ST_Tile(vci,512,512));




    RAISE NOTICE 'saved';
 RETURN return_rast;
END;
$BODY$
  LANGUAGE plpgsql;

--  function for the other iterations
create or replace function postgis.calculate_vci(doy_in int, year_in int, return_rast minmaxrast)
RETURNS minmaxrast AS
$BODY$
DECLARE
 --LST rasters

 vci    RASTER;

 ndvi_last RASTER;
 imgtype_in INT;

 id_acquisizione_in INT;


BEGIN

    RAISE NOTICE 'Calculating %, %', doy_in, year_in;

	select id_imgtype into imgtype_in
	from   postgis.imgtypes
	where  imgtype = 'NDVI';



	RAISE NOTICE 'Get Last NDVI';
 	select ST_Union(b.rast) into ndvi_last
	from   postgis.acquisizioni a inner join postgis.ndvi b using (id_acquisizione)
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


    RAISE NOTICE 'Calculating vci';
	vci := ST_MapAlgebra(ARRAY[ROW(return_rast.maxrast,1), ROW(return_rast.minrast,1), ROW(ndvi_last,1)]::rastbandarg[],
                            'calculate_vci_raster(double precision[], int[], text[])'::regprocedure,
                           	'32BF', 'LAST', null, 0, 0, null);

    return_rast.originrast := ndvi_last;

	RAISE NOTICE 'saving vci raster';
	IF EXISTS (  select id_acquisizione
	            from   postgis.acquisizioni inner join postgis.imgtypes using (id_imgtype)
	            where  imgtype = 'VCI'
	            and    extract(doy from dtime) = doy_in
	            and    extract(year from dtime) = year_in)  THEN

        RAISE NOTICE 'Found';

        select id_acquisizione into id_acquisizione_in
	    from   postgis.acquisizioni inner join postgis.imgtypes using (id_imgtype)
	    where  imgtype = 'VCI'
	    and    extract(doy from dtime) = doy_in
	    and    extract(year from dtime) = year_in;

	    DELETE FROM postgis.vci
		WHERE  id_acquisizione = id_acquisizione_in;

	ELSE

        RAISE NOTICE 'not found, create';
        INSERT INTO postgis.acquisizioni (dtime, id_imgtype)
        VALUES (to_timestamp(''||year_in||' '||doy_in||'', 'YYYY DDD'),(select id_imgtype from postgis.imgtypes where imgtype='VCI'));

        select id_acquisizione into id_acquisizione_in
        from   postgis.acquisizioni inner join postgis.imgtypes using (id_imgtype)
        where  imgtype = 'VCI'
        and    extract(doy from dtime) = doy_in
        and    extract(year from dtime) = year_in;


	END IF;
	RAISE NOTICE 'ids %',id_acquisizione_in;

	INSERT INTO postgis.vci (id_acquisizione, rast)
	VALUES
	(id_acquisizione_in, ST_Tile(vci,512,512));

    RAISE NOTICE 'saved';
 RETURN return_rast;
END;
$BODY$
  LANGUAGE plpgsql;


-- main function for generating vci rasters
create or replace function postgis.populate_vci(doy_begin INT, year_begin INT,
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
					where imgtype = 'NDVI'
					and    extract(doy from dtime) between doy_begin and doy_end
					and    extract(year from dtime) between year_begin and year_end
					order by 1,2

	LOOP
		RAISE NOTICE 'Processing doy: % - year: %', lst_i.doy_out, lst_i.year_out;
		IF actual_doy < lst_i.doy_out then
		    return_rast := postgis.calculate_vci(lst_i.doy_out, lst_i.year_out);

		    actual_doy := lst_i.doy_out;
		else
		    return_rast := postgis.calculate_vci(lst_i.doy_out, lst_i.year_out, return_rast);
		end if;


		RAISE NOTICE 'Done.';

	END LOOP;
 RETURN TRUE;
END;
$BODY$
  LANGUAGE plpgsql;


-- E-VCI version 3.0 29-08-2018
--  This version optimizes min and max calculation during the whole process
--  it is composed by 2 main function, the first is used in first iteration
--  the second one is used for the other iterations
--  these functions use the custom type minmaxrast for passing min and max rasters
--  calculated in each iteration. In this way postgresql will not have to recalculate min and max for whole set of rasters
--  but only for the last doy.

--  function for the first iteration
create or replace function postgis.calculate_evci(doy_in int, year_in int)
RETURNS minmaxrast AS
$BODY$
DECLARE
 --LST rasters

 evci RASTER;


 return_rast minmaxrast;

 imgtype_in INT;
 id_acquisizione_in INT;


BEGIN

    RAISE NOTICE 'Calculating %, % for first iteration', doy_in, year_in;


	select id_imgtype into imgtype_in
	from   postgis.imgtypes
	where  imgtype = 'EVI';

	RAISE NOTICE 'Get Last EVI';
 	select ST_Union(b.rast) into return_rast.originrast
	from   postgis.acquisizioni a inner join postgis.evi b using (id_acquisizione)
	where  extract(year from a.dtime) = year_in
	and    extract(doy from a.dtime) = doy_in
	and    id_imgtype = imgtype_in;

	return_rast.maxrast := st_addband(st_makeemptyraster(st_band(return_rast.originrast,1)),'16BSI'::text);
    return_rast.minrast := st_addband(st_makeemptyraster(st_band(return_rast.originrast,1)),'16BSI'::text);


	RAISE NOTICE 'Calculating min';
    SELECT ST_Union(rast,'MIN') INTO  return_rast.minrast
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

    RAISE NOTICE 'Calculating vci';
	evci := ST_MapAlgebra(ARRAY[ROW(return_rast.maxrast,1), ROW(return_rast.minrast,1), ROW(return_rast.originrast,1)]::rastbandarg[],
                            'calculate_vci_raster(double precision[], int[], text[])'::regprocedure,
                           	'32BF', 'LAST', null, 0, 0, null);

	RAISE NOTICE 'saving vci raster';
    IF EXISTS ( select id_acquisizione
	            from   postgis.acquisizioni inner join postgis.imgtypes using (id_imgtype)
	            where  imgtype = 'EVCI'
	            and    extract(doy from dtime) = doy_in
	            and    extract(year from dtime) = year_in)  THEN

        RAISE NOTICE 'Found';

        select id_acquisizione into id_acquisizione_in
	    from   postgis.acquisizioni inner join postgis.imgtypes using (id_imgtype)
	    where  imgtype = 'EVCI'
	    and    extract(doy from dtime) = doy_in
	    and    extract(year from dtime) = year_in;

	    DELETE FROM postgis.evci
		WHERE  id_acquisizione = id_acquisizione_in;

	ELSE

        RAISE NOTICE 'not found, create';
        INSERT INTO postgis.acquisizioni (dtime, id_imgtype)
        VALUES (to_timestamp(''||year_in||' '||doy_in||'', 'YYYY DDD'),(select id_imgtype from postgis.imgtypes where imgtype='EVCI'));

        select id_acquisizione into id_acquisizione_in
        from   postgis.acquisizioni inner join postgis.imgtypes using (id_imgtype)
        where  imgtype = 'EVCI'
        and    extract(doy from dtime) = doy_in
        and    extract(year from dtime) = year_in;


	END IF;
	RAISE NOTICE 'ids %',id_acquisizione_in;

    INSERT INTO postgis.evci (id_acquisizione, rast)
	VALUES
	(id_acquisizione_in, ST_Tile(evci,512,512));




    RAISE NOTICE 'saved';
 RETURN return_rast;
END;
$BODY$
  LANGUAGE plpgsql;

--  function for the other iterations
create or replace function postgis.calculate_evci(doy_in int, year_in int, return_rast minmaxrast)
RETURNS minmaxrast AS
$BODY$
DECLARE
 --LST rasters

 evci    RASTER;

 evi_last RASTER;
 imgtype_in INT;

 id_acquisizione_in INT;


BEGIN

    RAISE NOTICE 'Calculating %, %', doy_in, year_in;

	select id_imgtype into imgtype_in
	from   postgis.imgtypes
	where  imgtype = 'EVI';



	RAISE NOTICE 'Get Last EVI';
 	select ST_Union(b.rast) into evi_last
	from   postgis.acquisizioni a inner join postgis.evi b using (id_acquisizione)
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

    RAISE NOTICE 'Calculating vci';
	evci := ST_MapAlgebra(ARRAY[ROW(return_rast.maxrast,1), ROW(return_rast.minrast,1), ROW(evi_last,1)]::rastbandarg[],
                            'calculate_vci_raster(double precision[], int[], text[])'::regprocedure,
                           	'32BF', 'LAST', null, 0, 0, null);

    return_rast.originrast := evi_last;

	RAISE NOTICE 'saving vci raster';
	IF EXISTS (  select id_acquisizione
	            from   postgis.acquisizioni inner join postgis.imgtypes using (id_imgtype)
	            where  imgtype = 'EVCI'
	            and    extract(doy from dtime) = doy_in
	            and    extract(year from dtime) = year_in)  THEN

        RAISE NOTICE 'Found';

        select id_acquisizione into id_acquisizione_in
	    from   postgis.acquisizioni inner join postgis.imgtypes using (id_imgtype)
	    where  imgtype = 'EVCI'
	    and    extract(doy from dtime) = doy_in
	    and    extract(year from dtime) = year_in;

	    DELETE FROM postgis.evci
		WHERE  id_acquisizione = id_acquisizione_in;

	ELSE

        RAISE NOTICE 'not found, create';
        INSERT INTO postgis.acquisizioni (dtime, id_imgtype)
        VALUES (to_timestamp(''||year_in||' '||doy_in||'', 'YYYY DDD'),(select id_imgtype from postgis.imgtypes where imgtype='EVCI'));

        select id_acquisizione into id_acquisizione_in
        from   postgis.acquisizioni inner join postgis.imgtypes using (id_imgtype)
        where  imgtype = 'EVCI'
        and    extract(doy from dtime) = doy_in
        and    extract(year from dtime) = year_in;


	END IF;
	RAISE NOTICE 'ids %',id_acquisizione_in;

	INSERT INTO postgis.evci (id_acquisizione, rast)
	VALUES
	(id_acquisizione_in, ST_Tile(evci,512,512));

    RAISE NOTICE 'saved';
 RETURN return_rast;
END;
$BODY$
  LANGUAGE plpgsql;


-- main function for generating e-vci rasters
create or replace function postgis.populate_evci(doy_begin INT, year_begin INT,
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
					where imgtype = 'EVI'
					and    extract(doy from dtime) between doy_begin and doy_end
					and    extract(year from dtime) between year_begin and year_end
					order by 1,2

	LOOP
		RAISE NOTICE 'Processing doy: % - year: %', lst_i.doy_out, lst_i.year_out;
		IF actual_doy < lst_i.doy_out then
		    return_rast := postgis.calculate_evci(lst_i.doy_out, lst_i.year_out);

		    actual_doy := lst_i.doy_out;
		else
		    return_rast := postgis.calculate_evci(lst_i.doy_out, lst_i.year_out, return_rast);
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

-- calculate vci
--  INPUT
--  1- MAX NDVI/EVI
--  2- MIN NDVI/EVI
--  3- LAST NDVI/EVI
CREATE OR REPLACE FUNCTION postgis.calculate_vci_raster(value double precision[][][], pos integer[][], VARIADIC userargs text[])
	RETURNS double precision
	AS $$
	BEGIN



        IF (value[1][1][1] - value[2][1][1]) = 0 then

            RETURN -999.0;

        ELSEIF (value[3][1][1] - value[2][1][1]) < 0.0 THEN

            RETURN 0;

        ELSE

			RETURN (((value[3][1][1] - value[2][1][1]) / (value[1][1][1] - value[2][1][1])) * 100);

		END IF;

	END;
	$$ LANGUAGE 'plpgsql' IMMUTABLE;

-----




create or replace function postgis.calculate_ndvi_min(
    doy_in int, year_in int
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


 lst_i RECORD;

BEGIN




 RAISE NOTICE 'Calculating min rasters';
 select ST_Union(b.rast,'MIN') into lstmin
 			from postgis.acquisizioni a inner join postgis.ndvi b using (id_acquisizione)
            where extract('doy' from a.dtime) = doy_in
            and   extract('year' from a.dtime) < year_in;

 RETURN lstmin;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE



create or replace function postgis.calculate_ndvi_max(
    doy_in int, year_in int
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


 lst_i RECORD;

BEGIN



 RAISE NOTICE 'Calculating max rasters';
 select ST_Union(b.rast,'MAX') INTO lstmin
 			from postgis.acquisizioni a inner join postgis.ndvi b using (id_acquisizione)
            where extract('doy' from a.dtime) = doy_in
            and   extract('year' from a.dtime) < year_in;

 RETURN lstmin;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE



-- FUNCTION: postgis.calculate_vci2(integer, integer)
-- new version with optimazed procedure
-- DROP FUNCTION postgis.calculate_vci2(integer, integer);

CREATE OR REPLACE FUNCTION postgis.calculate_vci2(
    doy_in integer,
    year_in integer)
    RETURNS boolean
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE
AS $BODY$
DECLARE
    --LST rasters

    vci    RASTER;
    lst_i RECORD;
    maxrast RASTER;
    minrast RASTER;
    icount INT;
    return_rast minmaxrast;
    puppa INT;
    imgtype_in INT;
    id_acquisizione_in INT;

BEGIN

    RAISE NOTICE 'Calculating %, % for first iteration', doy_in, year_in;

    select id_imgtype into imgtype_in
    from   postgis.imgtypes
    where  imgtype = 'NDVI';

    IF EXISTS ( select id_acquisizione
                from   postgis.acquisizioni inner join postgis.imgtypes using (id_imgtype)
                where  imgtype = 'VCI'
                  and    extract(doy from dtime) = doy_in
                  and    extract(year from dtime) = year_in)  THEN

        RAISE NOTICE 'Found';

        select id_acquisizione into id_acquisizione_in
        from   postgis.acquisizioni inner join postgis.imgtypes using (id_imgtype)
        where  imgtype = 'VCI'
          and    extract(doy from dtime) = doy_in
          and    extract(year from dtime) = year_in;

        RAISE NOTICE 'Deleting old tci';
        DELETE FROM postgis.vci
        WHERE  id_acquisizione = id_acquisizione_in;

    ELSE

        RAISE NOTICE 'not found, create';
        INSERT INTO postgis.acquisizioni (dtime, id_imgtype)
        VALUES (to_timestamp(''||year_in||' '||doy_in||'', 'YYYY DDD'),(select id_imgtype from postgis.imgtypes where imgtype='VCI'));

        select id_acquisizione into id_acquisizione_in
        from   postgis.acquisizioni inner join postgis.imgtypes using (id_imgtype)
        where  imgtype = 'VCI'
          and    extract(doy from dtime) = doy_in
          and    extract(year from dtime) = year_in;

    END IF;

    RAISE NOTICE 'Get Last NDVI tiles';
    icount := 0;
    FOR lst_i IN select rast, ST_Envelope(rast) as ext
                 from postgis.ndvi inner join postgis.acquisizioni
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
            from postgis.ndvi inner join postgis.acquisizioni
                                         using (id_acquisizione)
            where extract(doy from dtime) = doy_in
              and   extract(year from dtime) < year_in
              and   ST_Envelope(rast) = lst_i.ext;

            -- RAISE NOTICE 'min: %, % max: %, %', ST_Width(minrast), ST_Height(minrast), ST_Width(maxrast), ST_Height(maxrast);

            vci := ST_MapAlgebra(ARRAY[ROW(maxrast,1), ROW(minrast,1), ROW(lst_i.rast,1)]::rastbandarg[],
                                 'calculate_vci_raster(double precision[], int[], text[])'::regprocedure,
                                 '32BF', 'LAST', null, 0, 0, null);

            RAISE NOTICE 'saving tci raster';


            --	RAISE NOTICE 'ids %',id_acquisizione_in;
            --  RAISE NOTICE 'tci: %, %', ST_Width(tci), ST_Height(tci);

            INSERT INTO postgis.vci (id_acquisizione, rast)
            VALUES
            (id_acquisizione_in, ST_Tile(vci,240,240));

            RAISE NOTICE 'saved';
        END LOOP;



    RAISE NOTICE 'done';
    RETURN TRUE;
END;
$BODY$;

ALTER FUNCTION postgis.calculate_vci2(integer, integer)
    OWNER TO postgres;





-- FUNCTION: postgis.calculate_vci2(integer, integer, geometry)

-- DROP FUNCTION postgis.calculate_vci2(integer, integer, geometry);

CREATE OR REPLACE FUNCTION postgis.calculate_vci2(
	doy_in integer,
	year_in integer,
	poly_in geometry)
    RETURNS boolean
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE
AS $BODY$
DECLARE
    --LST rasters

    vci    RASTER;
    lst_i RECORD;
    maxrast RASTER;
    minrast RASTER;
    icount INT;
    return_rast minmaxrast;
    puppa INT;

    id_acquisizione_in INT;

BEGIN
    RAISE NOTICE 'Process starts... %', current_timestamp;

       select id_acquisizione into id_acquisizione_in
        from   postgis.acquisizioni inner join postgis.imgtypes using (id_imgtype)
        where  imgtype = 'VCI'
          and    extract(doy from dtime) = doy_in
          and    extract(year from dtime) = year_in;

	RAISE NOTICE 'ids: %',id_acquisizione_in;

    RAISE NOTICE 'Get Last NDVI tiles';
    icount := 0;
    FOR lst_i IN select rast, ST_ConvexHull(rast) as ext
                 from postgis.ndvi inner join postgis.acquisizioni
                                              using (id_acquisizione)
                 where extract(doy from dtime) = doy_in
                   and   extract(year from dtime) = year_in
                   and   ST_Intersects(rast, poly_in)

        LOOP
            icount := icount + 1;
            RAISE NOTICE 'processing tile: %', icount;

            RAISE NOTICE 'calculating min, max';
            -- maxrast := st_addband(st_makeemptyraster(st_band(lst_i.rast,1)),'16BUI'::text);
            -- minrast := st_addband(st_makeemptyraster(st_band(lst_i.rast,1)),'16BUI'::text);

            select ST_Union(rast,'MIN'),ST_Union(rast,'MAX')
            into   minrast, maxrast
            from postgis.ndvi inner join postgis.acquisizioni
                                         using (id_acquisizione)
            where extract(doy from dtime) = doy_in
              and   extract(year from dtime) < year_in
			  and   ST_Intersects(rast, lst_i.ext);

			--and   ST_ConvexHull(rast) = lst_i.ext;

            maxrast := ST_Clip(maxrast, lst_i.ext, true);
			minrast := ST_Clip(minrast, lst_i.ext, true);

            -- RAISE NOTICE 'min: %, % max: %, %', ST_Width(minrast), ST_Height(minrast), ST_Width(maxrast), ST_Height(maxrast);

            vci := ST_MapAlgebra(ARRAY[ROW(maxrast,1), ROW(minrast,1), ROW(lst_i.rast,1)]::rastbandarg[],
                                 'calculate_vci_raster(double precision[], int[], text[])'::regprocedure,
                                 '32BF', 'LAST', null, 0, 0, null);

            RAISE NOTICE 'saving vci raster';

            --	RAISE NOTICE 'ids %',id_acquisizione_in;
            --  RAISE NOTICE 'tci: %, %', ST_Width(tci), ST_Height(tci);

            INSERT INTO postgis.vci (id_acquisizione, rast)
            VALUES
            (id_acquisizione_in, ST_Tile(vci,240,240));

            RAISE NOTICE 'saved';
        END LOOP;
    RAISE NOTICE 'Process concluded... %', current_timestamp;
    RAISE NOTICE 'done';
    RETURN TRUE;
END;
$BODY$;

ALTER FUNCTION postgis.calculate_vci2(integer, integer, geometry)
    OWNER TO postgres;


-- FUNCTION: postgis.calculate_vci2(integer, integer)

-- DROP FUNCTION postgis.deduplicate_vci(integer, integer);

CREATE OR REPLACE FUNCTION postgis.deduplicate_vci(
    doy_in integer,
    year_in integer)
    RETURNS boolean
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE
AS $BODY$
DECLARE
    --LST rasters

    vci    RASTER;
    lst_i RECORD;
    maxrast RASTER;
    minrast RASTER;
    icount INT;
    return_rast minmaxrast;
    puppa INT;
    imgtype_in INT;
    id_acquisizione_in INT;

BEGIN


    select id_acquisizione into id_acquisizione_in
    from   postgis.acquisizioni inner join postgis.imgtypes using (id_imgtype)
    where  imgtype = 'VCI'
      and    extract(doy from dtime) = doy_in
      and    extract(year from dtime) = year_in;


    FOR lst_i IN select ST_ConvexHull(rast) as ext, count(*) as num_tiles
                 from postgis.vci
                 where id_acquisizione = id_acquisizione_in
                 group by st_convexhull(rast)
                 having count(*) > 1

        LOOP
            DELETE FROM postgis.vci
            WHERE gid = (SELECT gid FROM postgis.vci
                         WHERE id_acquisizione = id_acquisizione_in
                           AND   ST_ConvexHull(rast) = lst_i.ext
                         LIMIT 1);

            RAISE NOTICE 'erased';
        END LOOP;
    RAISE NOTICE 'Process concluded... %', current_timestamp;
    RAISE NOTICE 'done';
    RETURN TRUE;
END;
$BODY$;




-- FUNCTION: postgis.calculate_evci2(integer, integer)

-- DROP FUNCTION postgis.calculate_evci2(integer, integer);

CREATE OR REPLACE FUNCTION postgis.calculate_evci2(
    doy_in integer,
    year_in integer,
    poly_in geometry)
    RETURNS boolean
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE
AS $BODY$
DECLARE
    --LST rasters

    evci    RASTER;
    lst_i RECORD;
    maxrast RASTER;
    minrast RASTER;
    icount INT;
    return_rast minmaxrast;
    puppa INT;
    imgtype_in INT;
    id_acquisizione_in INT;

BEGIN
    RAISE NOTICE 'Process starts... %', current_timestamp;
    select id_imgtype into imgtype_in
    from   postgis.imgtypes
    where  imgtype = 'EVI';

    IF EXISTS ( select id_acquisizione
                from   postgis.acquisizioni inner join postgis.imgtypes using (id_imgtype)
                where  imgtype = 'EVCI'
                  and    extract(doy from dtime) = doy_in
                  and    extract(year from dtime) = year_in)  THEN

        RAISE NOTICE 'Found';

        select id_acquisizione into id_acquisizione_in
        from   postgis.acquisizioni inner join postgis.imgtypes using (id_imgtype)
        where  imgtype = 'EVCI'
          and    extract(doy from dtime) = doy_in
          and    extract(year from dtime) = year_in;


    ELSE

        RAISE NOTICE 'not found, create';
        INSERT INTO postgis.acquisizioni (dtime, id_imgtype)
        VALUES (to_timestamp(''||year_in||' '||doy_in||'', 'YYYY DDD'),(select id_imgtype from postgis.imgtypes where imgtype='EVCI'))
        RETURNING id_acquisizione INTO id_acquisizione_in;

        RAISE NOTICE 'ids : %',id_acquisizione_in;


    END IF;

    RAISE NOTICE 'Get Last EVI tiles';
    icount := 0;
    FOR lst_i IN select rast, ST_ConvexHull(rast) as ext
                 from postgis.evi inner join postgis.acquisizioni
                                             using (id_acquisizione)
                 where extract(doy from dtime) = doy_in
                   and   extract(year from dtime) = year_in
                   and   ST_Intersects(rast, poly_in)

        LOOP
            icount := icount + 1;
            RAISE NOTICE 'processing tile: %', icount;

            RAISE NOTICE 'calculating min, max';
            -- maxrast := st_addband(st_makeemptyraster(st_band(lst_i.rast,1)),'16BUI'::text);
            -- minrast := st_addband(st_makeemptyraster(st_band(lst_i.rast,1)),'16BUI'::text);

            select ST_Union(rast,'MIN'),ST_Union(rast,'MAX')
            into   minrast, maxrast
            from postgis.evi inner join postgis.acquisizioni
                                        using (id_acquisizione)
            where extract(doy from dtime) = doy_in
              and   extract(year from dtime) < year_in
              and   ST_ConvexHull(rast) = lst_i.ext;

            -- RAISE NOTICE 'min: %, % max: %, %', ST_Width(minrast), ST_Height(minrast), ST_Width(maxrast), ST_Height(maxrast);

            evci := ST_MapAlgebra(ARRAY[ROW(maxrast,1), ROW(minrast,1), ROW(lst_i.rast,1)]::rastbandarg[],
                                  'calculate_vci_raster(double precision[], int[], text[])'::regprocedure,
                                  '32BF', 'LAST', null, 0, 0, null);

            RAISE NOTICE 'saving tci raster';

            --	RAISE NOTICE 'ids %',id_acquisizione_in;
            --  RAISE NOTICE 'tci: %, %', ST_Width(tci), ST_Height(tci);

            INSERT INTO postgis.evci (id_acquisizione, rast)
            VALUES
            (id_acquisizione_in, ST_Tile(evci,240,240));

            RAISE NOTICE 'saved';
        END LOOP;
    RAISE NOTICE 'Process concluded... %', current_timestamp;
    RAISE NOTICE 'done';
    RETURN TRUE;
END;
$BODY$;





-- FUNCTION: postgis.calculate_evci2(integer, integer)

-- DROP FUNCTION postgis.deduplicate_evci(integer, integer);

CREATE OR REPLACE FUNCTION postgis.deduplicate_evci(
    doy_in integer,
    year_in integer)
    RETURNS boolean
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE
AS $BODY$
DECLARE
    --LST rasters

    evci    RASTER;
    lst_i RECORD;
    maxrast RASTER;
    minrast RASTER;
    icount INT;
    return_rast minmaxrast;
    puppa INT;
    imgtype_in INT;
    id_acquisizione_in INT;

BEGIN


    select id_acquisizione into id_acquisizione_in
    from   postgis.acquisizioni inner join postgis.imgtypes using (id_imgtype)
    where  imgtype = 'EVCI'
      and    extract(doy from dtime) = doy_in
      and    extract(year from dtime) = year_in;


    FOR lst_i IN select ST_ConvexHull(rast) as ext, count(*) as num_tiles
                 from postgis.evci
                 where id_acquisizione = id_acquisizione_in
                 group by st_convexhull(rast)
                 having count(*) > 1

        LOOP
            DELETE FROM postgis.evci
            WHERE gid = (SELECT gid FROM postgis.evci
                         WHERE id_acquisizione = id_acquisizione_in
                           AND   ST_ConvexHull(rast) = lst_i.ext
                         LIMIT 1);

            RAISE NOTICE 'erased';
        END LOOP;
    RAISE NOTICE 'Process concluded... %', current_timestamp;
    RAISE NOTICE 'done';
    RETURN TRUE;
END;
$BODY$;

-- FUNCTION: postgis.calculate_evci2(integer, integer)

-- DROP FUNCTION postgis.deduplicate_evci(integer, integer);

CREATE OR REPLACE FUNCTION postgis.deduplicate_vhi(
    doy_in integer,
    year_in integer)
    RETURNS boolean
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE
AS $BODY$
DECLARE
    --LST rasters

    vhi    RASTER;
    lst_i RECORD;
    maxrast RASTER;
    minrast RASTER;
    icount INT;
    return_rast minmaxrast;
    puppa INT;
    imgtype_in INT;
    id_acquisizione_in INT;

BEGIN


    select id_acquisizione into id_acquisizione_in
    from   postgis.acquisizioni inner join postgis.imgtypes using (id_imgtype)
    where  imgtype = 'VHI'
      and    extract(doy from dtime) = doy_in
      and    extract(year from dtime) = year_in;


    FOR lst_i IN select ST_ConvexHull(rast) as ext, count(*) as num_tiles
                 from postgis.vhi
                 where id_acquisizione = id_acquisizione_in
                 group by st_convexhull(rast)
                 having count(*) > 1

        LOOP
            DELETE FROM postgis.vhi
            WHERE gid = (SELECT gid FROM postgis.vhi
                         WHERE id_acquisizione = id_acquisizione_in
                           AND   ST_ConvexHull(rast) = lst_i.ext
                         LIMIT 1);

            RAISE NOTICE 'erased';
        END LOOP;
    RAISE NOTICE 'Process concluded... %', current_timestamp;
    RAISE NOTICE 'done';
    RETURN TRUE;
END;
$BODY$;



