
-- FUNCTION: postgis.import_lst_image(integer, integer)

-- DROP FUNCTION postgis.import_lst_image(integer, integer);
-- NEW version with resample
CREATE OR REPLACE FUNCTION postgis.import_lst_image(
    year_start integer,
    gg_start integer)
    RETURNS integer
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE
AS $BODY$
DECLARE
    mcount INT;
    ycount INT;
    id_ins INT;
    sqlStr VARCHAR;
    ref_rast RASTER;
    imgtypeid INT;
BEGIN

    ycount := year_start;
    mcount := gg_start;
    id_ins := -1;

    SELECT id_imgtype INTO imgtypeid
    FROM   postgis.imgtypes
    WHERE  imgtype = 'LST';

    IF EXISTS (SELECT id_acquisizione
               FROM postgis.acquisizioni
               WHERE dtime = to_timestamp(''||ycount||'-'||mcount||' 00:00:00','YYYY-DDD HH24:MI:SS')
                 AND   id_imgtype = imgtypeid) THEN

        SELECT id_acquisizione INTO id_ins
        FROM postgis.acquisizioni
        WHERE dtime = to_timestamp(''||ycount||'-'||mcount||' 00:00:00','YYYY-DDD HH24:MI:SS')
          AND   id_imgtype = imgtypeid;
        RAISE NOTICE 'Esiste con questo id: %',id_ins;

    ELSE
        RAISE NOTICE 'Inserisco acquisizione : % - %: %', ycount, mcount,''||ycount||'-'||mcount||' 00:00:00';

        insert into postgis.acquisizioni (dtime, id_imgtype) values (to_timestamp(''||ycount||'-'||mcount||' 00:00:00','YYYY-DDD HH24:MI:SS'),imgtypeid);

        RAISE NOTICE 'OK';

        select max(id_acquisizione) into id_ins
        from   postgis.acquisizioni;

        RAISE NOTICE 'New id: %',id_ins;
    END IF;

    IF EXISTS (SELECT * FROM pg_tables WHERE tablename = 'lst_'||ycount||'_'||mcount) THEN
        RAISE NOTICE 'Found';
        RAISE NOTICE 'Get reference tile';

        sqlStr := 'SELECT rast FROM postgis.lst LIMIT 1';
        EXECUTE sqlStr INTO ref_rast;

        RAISE NOTICE 'Resampling and saving...';
        sqlStr := 'INSERT INTO postgis.lst (id_acquisizione, rast)
			   SELECT '||id_ins||', ST_Tile(ST_Resample(rast,$1),240,240)
			   FROM lst_'||ycount||'_'||mcount||'';

        EXECUTE sqlStr USING ref_rast;

        RAISE NOTICE 'done.';
    ELSE
        RAISE NOTICE 'Not Found, Skipping';

    END IF;

    RAISE NOTICE 'OK';

    RETURN id_ins;
END;
$BODY$;

ALTER FUNCTION postgis.import_lst_image(integer, integer)
    OWNER TO postgres;



-- FUNCTION: postgis.import_ndvi_image(integer, integer)

-- DROP FUNCTION postgis.import_ndvi_image(integer, integer);
-- NEW version with resample
CREATE OR REPLACE FUNCTION postgis.import_ndvi_image(
    year_start integer,
    gg_start integer)
    RETURNS integer
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE
AS $BODY$
DECLARE
    mcount INT;
    ycount INT;
    id_ins INT;
    sqlStr VARCHAR;
    ref_rast RASTER;
    imgtypeid INT;
BEGIN

    ycount := year_start;
    mcount := gg_start;
    id_ins := -1;

    SELECT id_imgtype INTO imgtypeid
    FROM   postgis.imgtypes
    WHERE  imgtype = 'NDVI';

    IF EXISTS (SELECT id_acquisizione
               FROM postgis.acquisizioni
               WHERE dtime = to_timestamp(''||ycount||'-'||mcount||' 00:00:00','YYYY-DDD HH24:MI:SS')
                 AND   id_imgtype = imgtypeid) THEN

        SELECT id_acquisizione INTO id_ins
        FROM postgis.acquisizioni
        WHERE dtime = to_timestamp(''||ycount||'-'||mcount||' 00:00:00','YYYY-DDD HH24:MI:SS')
          AND   id_imgtype = imgtypeid;
        RAISE NOTICE 'Esiste con questo id: %',id_ins;

    ELSE
        RAISE NOTICE 'Inserisco acquisizione : % - %: %', ycount, mcount,''||ycount||'-'||mcount||' 00:00:00';

        insert into postgis.acquisizioni (dtime, id_imgtype) values (to_timestamp(''||ycount||'-'||mcount||' 00:00:00','YYYY-DDD HH24:MI:SS'),imgtypeid);

        RAISE NOTICE 'OK';

        select max(id_acquisizione) into id_ins
        from   postgis.acquisizioni;

        RAISE NOTICE 'New id: %',id_ins;
    END IF;

    IF EXISTS (SELECT * FROM pg_tables WHERE tablename = 'ndvi_'||ycount||'_'||mcount) THEN
        RAISE NOTICE 'Found';
        RAISE NOTICE 'Get reference tile';

        sqlStr := 'SELECT rast FROM postgis.ndvi LIMIT 1';
        EXECUTE sqlStr INTO ref_rast;

        RAISE NOTICE 'Resampling and saving...';
        sqlStr := 'INSERT INTO postgis.ndvi (id_acquisizione, rast)
			   SELECT '||id_ins||', ST_Tile(ST_Resample(rast,$1),240,240)
			   FROM ndvi_'||ycount||'_'||mcount||'';

        EXECUTE sqlStr USING ref_rast;

        RAISE NOTICE 'done.';
    ELSE
        RAISE NOTICE 'Not Found, Skipping';
        id_ins := -1;
    END IF;

    RAISE NOTICE 'OK';

    RETURN id_ins;
END;
$BODY$;

ALTER FUNCTION postgis.import_ndvi_image(integer, integer)
    OWNER TO postgres;





-- FUNCTION: postgis.import_evi_image(integer, integer)

-- DROP FUNCTION postgis.import_evi_image(integer, integer);
-- NEW version with resample
CREATE OR REPLACE FUNCTION postgis.import_evi_image(
    year_start integer,
    gg_start integer)
    RETURNS integer
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE
AS $BODY$
DECLARE
    mcount INT;
    ycount INT;
    id_ins INT;
    sqlStr VARCHAR;
    ref_rast RASTER;
    imgtypeid INT;
BEGIN

    ycount := year_start;
    mcount := gg_start;
    id_ins := -1;

    SELECT id_imgtype INTO imgtypeid
    FROM   postgis.imgtypes
    WHERE  imgtype = 'EVI';

    IF EXISTS (SELECT id_acquisizione
               FROM postgis.acquisizioni
               WHERE dtime = to_timestamp(''||ycount||'-'||mcount||' 00:00:00','YYYY-DDD HH24:MI:SS')
                 AND   id_imgtype = imgtypeid) THEN

        SELECT id_acquisizione INTO id_ins
        FROM postgis.acquisizioni
        WHERE dtime = to_timestamp(''||ycount||'-'||mcount||' 00:00:00','YYYY-DDD HH24:MI:SS')
          AND   id_imgtype = imgtypeid;
        RAISE NOTICE 'Esiste con questo id: %',id_ins;

    ELSE
        RAISE NOTICE 'Inserisco acquisizione : % - %: %', ycount, mcount,''||ycount||'-'||mcount||' 00:00:00';

        insert into postgis.acquisizioni (dtime, id_imgtype) values (to_timestamp(''||ycount||'-'||mcount||' 00:00:00','YYYY-DDD HH24:MI:SS'),imgtypeid);

        RAISE NOTICE 'OK';

        select max(id_acquisizione) into id_ins
        from   postgis.acquisizioni;

        RAISE NOTICE 'New id: %',id_ins;
    END IF;

    IF EXISTS (SELECT * FROM pg_tables WHERE tablename = 'evi_'||ycount||'_'||mcount) THEN
        RAISE NOTICE 'Found';
        RAISE NOTICE 'Get reference tile';

        sqlStr := 'SELECT rast FROM postgis.evi LIMIT 1';
        EXECUTE sqlStr INTO ref_rast;

        RAISE NOTICE 'Resampling and saving...';
        sqlStr := 'INSERT INTO postgis.evi (id_acquisizione, rast)
			   SELECT '||id_ins||', ST_Tile(ST_Resample(rast,$1),240,240)
			   FROM evi_'||ycount||'_'||mcount||'';

        EXECUTE sqlStr USING ref_rast;

        RAISE NOTICE 'done.';
    ELSE
        RAISE NOTICE 'Not Found, Skipping';

    END IF;

    RAISE NOTICE 'OK';

    RETURN id_ins;
END;
$BODY$;

ALTER FUNCTION postgis.import_evi_image(integer, integer)
    OWNER TO postgres;




-- FUNCTION: check_product_presence
-- this function checks if there are last source product (eg. NDVI, EVI)
-- but not last derived product (eg. VCI , EVCI)
--  PARAMETER

--  RETURN
--   True: product exists
--   False: product doesn't exist
CREATE OR REPLACE FUNCTION postgis.check_product_presence(
	source_prod varchar,
	derived_prod varchar,
	tiles_list varchar)
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

