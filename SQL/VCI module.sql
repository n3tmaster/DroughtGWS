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


create or replace function postgis.calculate_vci(
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
 where  imgtype = 'VCI';

 RAISE NOTICE 'ImgType : %',imgtype_in;

 RAISE NOTICE 'Get Last NDVI';
 select b.rast into ndvilast
 from postgis.acquisizioni a inner join postgis.evi b using (id_acquisizione)
 where  extract('year' from a.dtime) = year_in
 and   extract('doy' from a.dtime) = doy_in
 and   a.id_imgtype = imgtype_in;

 RAISE NOTICE 'Prepare ndvimax and ndvimin matrix';

 ndvimax := st_addband(st_makeemptyraster(st_band(ndvilast,1)),'16BSI'::text);
 ndvimin := st_addband(st_makeemptyraster(st_band(ndvilast,1)),'16BSI'::text);
 vci    := st_addband(st_makeemptyraster(st_band(ndvilast,1)),'32BF'::text);

 RAISE NOTICE 'Calculating min and max rasters';
 FOR vci_i IN select b.rast as rastin
 			from postgis.acquisizioni a inner join postgis.evi b using (id_acquisizione)
            where extract('doy' from a.dtime) = doy_in
            and   extract('year' from a.dtime) < year_in
            and   a.id_imgtype = imgtype_in
            order by a.dtime
 LOOP
 	ndvimax := ST_MapAlgebra(ARRAY[ROW(ndvimax,1), ROW(vci_i.rastin,1)]::rastbandarg[],
                            'ST_Max4ma(double precision[], int[], text[])'::regprocedure,
                           	'16BSI', 'LAST', null, 0, 0, null);

    ndvimin := ST_MapAlgebra(ARRAY[ROW(ndvimin,1), ROW(vci_i.rastin,1)]::rastbandarg[],
                            'ST_Min4ma(double precision[], int[], text[])'::regprocedure,
                           	'16BSI', 'LAST', null, 0, 0, null);
 END LOOP;

 RAISE NOTICE 'Calculate TCI raster...';
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
        RAISE NOTICE 'deleting old VCI...';

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

    RAISE NOTICE 'save VCI...';
    insert into postgis.vci (id_acquisizione, rast)
    values (id_acquisizione_in, vci);
    RAISE NOTICE 'done';

 END IF;

 RAISE NOTICE 'done.';
 RETURN vci;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE


