
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

