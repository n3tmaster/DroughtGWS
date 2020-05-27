-- Create VHI image from VCI and TCI
-- Input : dtime_in - timestamp - reference date for VHI calculation
-- Output : VHI raster
create or replace function postgis.calculate_vhi(
    doy_in int, year_in int
    )
RETURNS RASTER AS
$BODY$
DECLARE


 tci RASTER;
 tci2 RASTER;
 vci RASTER;
 vhi RASTER;

 imgtype_in INT;
 tcitype_in INT;
 vcitype_in INT;
 id_acquisizione_in INT;



BEGIN
 RAISE NOTICE 'Calculating VHI raster for doy: % and year: %',doy_in,year_in;



 select id_imgtype into imgtype_in
 from   postgis.imgtypes
 where  imgtype = 'VHI';

 select id_imgtype into tcitype_in
 from   postgis.imgtypes
 where  imgtype = 'TCI';

 select id_imgtype into vcitype_in
 from   postgis.imgtypes
 where  imgtype = 'VCI';

 RAISE NOTICE 'ImgType : %',imgtype_in;

 RAISE NOTICE 'Get VCI';
 select ST_Union(b.rast) into vci
 from postgis.acquisizioni a inner join postgis.vci b using (id_acquisizione)
 where  extract('year' from a.dtime) = year_in
 and   extract('doy' from a.dtime) = doy_in
 and   a.id_imgtype = vcitype_in;


 IF ST_IsEmpty(vci) = TRUE THEN
    RAISE NOTICE 'No VCI existence, exit';
    RETURN null;
 END IF;

 RAISE NOTICE 'Get mean TCI';
 select ST_Union(b.rast) into tci
 from postgis.acquisizioni a inner join postgis.tci b using (id_acquisizione)
 where  extract('year' from a.dtime) = year_in
 and   extract('doy' from a.dtime) = doy_in
 and   a.id_imgtype = tcitype_in;

 RAISE NOTICE 'Get mean TCI2';
 select ST_Union(b.rast) into tci2
 from postgis.acquisizioni a inner join postgis.tci b using (id_acquisizione)
 where  extract('year' from a.dtime) = year_in
 and   extract('doy' from a.dtime) = (doy_in + 8)
 and   a.id_imgtype = tcitype_in;


 IF ST_IsEmpty(tci) = TRUE THEN
    RAISE NOTICE 'No TCI existence, exit';
    RETURN null;
 END IF;
 IF ST_IsEmpty(tci2) = TRUE THEN
    RAISE NOTICE 'No TCI2 existence, exit';
    RETURN null;
 END IF;

 WITH base_mean AS (SELECT tci as rast
                  UNION
                  SELECT tci2)
 SELECT ST_Union(rast,'MEAN') INTO tci2 FROM base_mean;


 RAISE NOTICE 'Resampling TCI';
 tci2 := ST_Resample(tci2, vci);


 RAISE NOTICE 'Calculate VHI raster...';
 vhi := ST_MapAlgebra(ARRAY[ROW(vci,1), ROW(tci2,1)]::rastbandarg[],
                            'postgis.calculate_vhi_raster(double precision[], int[], text[])'::regprocedure,
                           	'32BF', 'CUSTOM', vci, 0, 0, null);



 RAISE NOTICE 'saving vhi raster';
    IF EXISTS ( select id_acquisizione
	            from   postgis.acquisizioni inner join postgis.imgtypes using (id_imgtype)
	            where  imgtype = 'VHI'
	            and    extract(doy from dtime) = doy_in
	            and    extract(year from dtime) = year_in)  THEN

        RAISE NOTICE 'Found';

        select id_acquisizione into id_acquisizione_in
	    from   postgis.acquisizioni inner join postgis.imgtypes using (id_imgtype)
	    where  imgtype = 'VHI'
	    and    extract(doy from dtime) = doy_in
	    and    extract(year from dtime) = year_in;

	    DELETE FROM postgis.vhi
		WHERE  id_acquisizione = id_acquisizione_in;

	ELSE

        RAISE NOTICE 'not found, create';
        INSERT INTO postgis.acquisizioni (dtime, id_imgtype)
        VALUES (to_timestamp(''||year_in||' '||doy_in||'', 'YYYY DDD'),(select id_imgtype from postgis.imgtypes where imgtype='VHI'));

        select id_acquisizione into id_acquisizione_in
        from   postgis.acquisizioni inner join postgis.imgtypes using (id_imgtype)
        where  imgtype = 'VHI'
        and    extract(doy from dtime) = doy_in
        and    extract(year from dtime) = year_in;


	END IF;
	RAISE NOTICE 'ids %',id_acquisizione_in;

    INSERT INTO postgis.vhi (id_acquisizione, rast)
	VALUES
	(id_acquisizione_in, ST_Tile(vhi,512,512));

 RAISE NOTICE 'done.';
 RETURN vhi;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE


-- calculate vhi

CREATE OR REPLACE FUNCTION postgis.calculate_vhi_raster(value double precision[][][], pos integer[][], VARIADIC userargs text[])
	RETURNS double precision
	AS $$
	BEGIN


        -- (VCI * 0.5) + (TCI * 0.5)

	    RETURN ((value[1][1][1] * 0.5) + (value[2][1][1] * 0.5));


	END;
	$$ LANGUAGE 'plpgsql' IMMUTABLE;



-- main function for generating e-vci rasters
create or replace function postgis.populate_vhi(doy_begin INT, year_begin INT,
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
					where imgtype = 'TCI'
					and    extract(doy from dtime) between doy_begin and doy_end
					and    extract(year from dtime) between year_begin and year_end
					order by 1,2

	LOOP
		RAISE NOTICE 'Processing doy: % - year: %', lst_i.doy_out, lst_i.year_out;

        postgis.calculate_vhi(lst_i.doy_out, lst_i.year_out);

--        RAISE NOTICE 'Returned min raster: % , % and max raster: %, %', ST_Width(return_rast.minrast), ST_Height(return_rast.minrast), ST_Width(return_rast.maxrast), ST_Height(return_rast.maxrast);
--        RAISE NOTICE 'Returned origin raster: % , % ', ST_Width(return_rast.originrast), ST_Height(return_rast.originrast);
--        RAISE NOTICE 'Saved: %',aaaa;

		RAISE NOTICE 'Done.';

	END LOOP;
 RETURN TRUE;
END;
$BODY$
  LANGUAGE plpgsql;


create or replace function postgis.calculate_mean_tci(doy_in int, year_in int)
RETURNS RASTER AS
$BODY$
DECLARE


 tci RASTER;
 tci2 RASTER;
 vci RASTER;
 vhi RASTER;

 imgtype_in INT;
 tcitype_in INT;
 vcitype_in INT;
 id_acquisizione_in INT;



BEGIN
 RAISE NOTICE 'Calculating TCI raster for doy: % and year: %',doy_in,year_in;


 select id_imgtype into tcitype_in
 from   postgis.imgtypes
 where  imgtype = 'TCI';



 RAISE NOTICE 'ImgType : %',tcitype_in;

 RAISE NOTICE 'Get mean TCI';
 select ST_Union(b.rast,'MEAN') into tci
 from postgis.acquisizioni a inner join postgis.tci b using (id_acquisizione)
 where  extract('year' from a.dtime) = year_in
 and   extract('doy' from a.dtime) between (doy_in) and (doy_in + 8)
 and   a.id_imgtype = tcitype_in;


 RETURN tci;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE


create or replace function postgis.calculate_resample_tci(
    doy_in int, year_in int
    )
RETURNS RASTER AS
$BODY$
DECLARE


 tci RASTER;
 tci2 RASTER;
 vci RASTER;
 vhi RASTER;

 imgtype_in INT;
 tcitype_in INT;
 vcitype_in INT;
 id_acquisizione_in INT;



BEGIN
 RAISE NOTICE 'Calculating TCI raster for doy: % and year: %',doy_in,year_in;


 select id_imgtype into tcitype_in
 from   postgis.imgtypes
 where  imgtype = 'TCI';



 RAISE NOTICE 'ImgType : %',tcitype_in;
 RAISE NOTICE 'Get VCI';
 select ST_Union(b.rast) into vci
 from postgis.acquisizioni a inner join postgis.vci b using (id_acquisizione)
 where  extract('year' from a.dtime) = year_in
 and   extract('doy' from a.dtime) = doy_in
 and   a.id_imgtype = vcitype_in;

 RAISE NOTICE 'Get mean TCI';
 select ST_Union(b.rast) into tci
 from postgis.acquisizioni a inner join postgis.tci b using (id_acquisizione)
 where  extract('year' from a.dtime) = year_in
 and   extract('doy' from a.dtime) = doy_in
 and   a.id_imgtype = tcitype_in;

 RAISE NOTICE 'Get mean TCI2';
 select ST_Union(b.rast) into tci2
 from postgis.acquisizioni a inner join postgis.tci b using (id_acquisizione)
 where  extract('year' from a.dtime) = year_in
 and   extract('doy' from a.dtime) = (doy_in + 8)
 and   a.id_imgtype = tcitype_in;

 WITH base_mean AS (SELECT tci as rast
                  UNION
                  SELECT tci2)
 SELECT ST_Union(rast,'MEAN') INTO tci2 FROM base_mean;


 RAISE NOTICE 'Resampling TCI';
 tci2 := ST_Resample(tci2, vci);


 RETURN tci2;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE




--- EVHI

-- main function for generating e-vci rasters
create or replace function postgis.populate_evhi(doy_begin INT, year_begin INT,
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
					where imgtype = 'TCI'
					and    extract(doy from dtime) between doy_begin and doy_end
					and    extract(year from dtime) between year_begin and year_end
					order by 1,2

	LOOP
		RAISE NOTICE 'Processing doy: % - year: %', lst_i.doy_out, lst_i.year_out;

        postgis.calculate_evhi(lst_i.doy_out, lst_i.year_out);

--        RAISE NOTICE 'Returned min raster: % , % and max raster: %, %', ST_Width(return_rast.minrast), ST_Height(return_rast.minrast), ST_Width(return_rast.maxrast), ST_Height(return_rast.maxrast);
--        RAISE NOTICE 'Returned origin raster: % , % ', ST_Width(return_rast.originrast), ST_Height(return_rast.originrast);
--        RAISE NOTICE 'Saved: %',aaaa;

		RAISE NOTICE 'Done.';

	END LOOP;
 RETURN TRUE;
END;
$BODY$
  LANGUAGE plpgsql;



-- Create E-VHI image from VCI and TCI
-- Input : dtime_in - timestamp - reference date for VHI calculation
-- Output : VHI raster
create or replace function postgis.calculate_evhi(
    doy_in int, year_in int
    )
RETURNS RASTER AS
$BODY$
DECLARE


 tci RASTER;
 tci2 RASTER;
 evci RASTER;
 evhi RASTER;

 imgtype_in INT;
 tcitype_in INT;
 evcitype_in INT;
 id_acquisizione_in INT;



BEGIN
 RAISE NOTICE 'Calculating EVHI raster for doy: % and year: %',doy_in,year_in;



 select id_imgtype into imgtype_in
 from   postgis.imgtypes
 where  imgtype = 'EVHI';

 select id_imgtype into tcitype_in
 from   postgis.imgtypes
 where  imgtype = 'TCI';

 select id_imgtype into evcitype_in
 from   postgis.imgtypes
 where  imgtype = 'EVCI';

 RAISE NOTICE 'ImgType : %',imgtype_in;

 RAISE NOTICE 'Get EVCI';
 select ST_Union(b.rast) into evci
 from postgis.acquisizioni a inner join postgis.evci b using (id_acquisizione)
 where  extract('year' from a.dtime) = year_in
 and   extract('doy' from a.dtime) = doy_in
 and   a.id_imgtype = evcitype_in;


 IF ST_IsEmpty(evci) = TRUE THEN
    RAISE NOTICE 'No EVCI existence, exit';
    RETURN null;
 END IF;

 RAISE NOTICE 'Get mean TCI';
 select ST_Union(b.rast) into tci
 from postgis.acquisizioni a inner join postgis.tci b using (id_acquisizione)
 where  extract('year' from a.dtime) = year_in
 and   extract('doy' from a.dtime) = doy_in
 and   a.id_imgtype = tcitype_in;

 RAISE NOTICE 'Get mean TCI2';
 select ST_Union(b.rast) into tci2
 from postgis.acquisizioni a inner join postgis.tci b using (id_acquisizione)
 where  extract('year' from a.dtime) = year_in
 and   extract('doy' from a.dtime) = (doy_in + 8)
 and   a.id_imgtype = tcitype_in;


 IF ST_IsEmpty(tci) = TRUE THEN
    RAISE NOTICE 'No TCI existence, exit';
    RETURN null;
 END IF;
 IF ST_IsEmpty(tci2) = TRUE THEN
    RAISE NOTICE 'No TCI2 existence, exit';
    RETURN null;
 END IF;

 WITH base_mean AS (SELECT tci as rast
                  UNION
                  SELECT tci2)
 SELECT ST_Union(rast,'MEAN') INTO tci2 FROM base_mean;


 RAISE NOTICE 'Resampling TCI';
 tci2 := ST_Resample(tci2, evci);


 RAISE NOTICE 'Calculate EVHI raster...';
 evhi := ST_MapAlgebra(ARRAY[ROW(evci,1), ROW(tci2,1)]::rastbandarg[],
                            'postgis.calculate_vhi_raster(double precision[], int[], text[])'::regprocedure,
                           	'32BF', 'CUSTOM', evci, 0, 0, null);



 RAISE NOTICE 'saving evhi raster';
    IF EXISTS ( select id_acquisizione
	            from   postgis.acquisizioni inner join postgis.imgtypes using (id_imgtype)
	            where  imgtype = 'EVHI'
	            and    extract(doy from dtime) = doy_in
	            and    extract(year from dtime) = year_in)  THEN

        RAISE NOTICE 'Found';

        select id_acquisizione into id_acquisizione_in
	    from   postgis.acquisizioni inner join postgis.imgtypes using (id_imgtype)
	    where  imgtype = 'EVHI'
	    and    extract(doy from dtime) = doy_in
	    and    extract(year from dtime) = year_in;

	    DELETE FROM postgis.evhi
		WHERE  id_acquisizione = id_acquisizione_in;

	ELSE

        RAISE NOTICE 'not found, create';
        INSERT INTO postgis.acquisizioni (dtime, id_imgtype)
        VALUES (to_timestamp(''||year_in||' '||doy_in||'', 'YYYY DDD'),(select id_imgtype from postgis.imgtypes where imgtype='EVHI'));

        select id_acquisizione into id_acquisizione_in
        from   postgis.acquisizioni inner join postgis.imgtypes using (id_imgtype)
        where  imgtype = 'EVHI'
        and    extract(doy from dtime) = doy_in
        and    extract(year from dtime) = year_in;


	END IF;
	RAISE NOTICE 'ids %',id_acquisizione_in;

    INSERT INTO postgis.evhi (id_acquisizione, rast)
	VALUES
	(id_acquisizione_in, ST_Tile(evhi,512,512));

 RAISE NOTICE 'done.';
 RETURN evhi;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE



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

-- FUNCTION: versione vhi2 per Multi threads

CREATE OR REPLACE FUNCTION postgis.calculate_vhi2(
    doy_in integer,
    year_in integer,
    poly_in geometry)
    RETURNS boolean
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE
AS $BODY$  DECLARE

    tci RASTER;
    tci2 RASTER;

    vhi RASTER;

    id_acquisizione_in INT;
    tci_count INT;
    lst_i RECORD;
BEGIN
    RAISE NOTICE 'Calculating VHI raster for doy: % and year: %',doy_in,year_in;





        select id_acquisizione into id_acquisizione_in
        from   postgis.acquisizioni inner join postgis.imgtypes using (id_imgtype)
        where  imgtype = 'VHI'
          and    extract(doy from dtime) = doy_in
          and    extract(year from dtime) = year_in;

	RAISE NOTICE 'ids: %',id_acquisizione_in;

    FOR lst_i IN select rast as vci_rast, ST_ConvexHull(rast) as ext
                 from postgis.vci inner join postgis.acquisizioni
                                             using (id_acquisizione)
                 where extract(doy from dtime) = doy_in
                   and   extract(year from dtime) = year_in
                   and   ST_Intersects(rast, poly_in)
        LOOP

			RAISE NOTICE 'Calculte TCI mean';
            WITH base_mean AS (select rast
            from postgis.tci inner join postgis.acquisizioni
                                        using (id_acquisizione)
            where extract(doy from dtime) = doy_in
              and   extract(year from dtime) = year_in
              and   ST_ConvexHull(rast) = lst_i.ext
			UNION
			select rast
            from postgis.tci inner join postgis.acquisizioni
                                        using (id_acquisizione)
            where extract(doy from dtime) = (doy_in + 8)
              and   extract(year from dtime) = year_in
              and   ST_ConvexHull(rast) = lst_i.ext)
            SELECT ST_Union(rast,'MEAN') INTO tci2 FROM base_mean;

            RAISE NOTICE 'Resampling TCI';
            tci2 := ST_Resample(tci2, lst_i.vci_rast);


            RAISE NOTICE 'Calculate VHI raster...';
            vhi := ST_MapAlgebra(ARRAY[ROW(lst_i.vci_rast,1), ROW(tci2,1)]::rastbandarg[],
                                 'postgis.calculate_vhi_raster(double precision[], int[], text[])'::regprocedure,
                                 '32BF', 'CUSTOM',lst_i.vci_rast, 0, 0, null);


            INSERT INTO postgis.vhi (id_acquisizione, rast)
            VALUES
            (id_acquisizione_in, ST_Tile(vhi,240,240));
        END LOOP;


    RAISE NOTICE 'done.';
    RETURN TRUE;
END;
$BODY$;
