-- list of functions for foreign table operations

-- Create and save ETR image
--
--
--  INPUT
--   poly_in - given polygon for raster extraction
--   dtime_in - given dtime
--   ids_in - id for acquisitions table
--  OUTPUT
--     INTEGER : 0 - OK - 1 ERROR

CREATE OR REPLACE FUNCTION postgis.calculate_save_etr(
    poly_in geometry,
    dtime_in timestamp,
    ids_in integer)
    RETURNS INTEGER
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE

AS $BODY$
DECLARE

    etr_result RASTER;

BEGIN

    RAISE NOTICE 'Call ETR calculation method';
    etr_result := postgis.calculate_real_et(poly_in, dtime_in);

    IF etr_result IS NULL THEN
        RAISE NOTICE 'image is null';
        RETURN 1;
    ELSE
        DELETE FROM postgis.etr WHERE id_acquisizione = ids_in;


        INSERT INTO postgis.etr(id_acquisizione, rast)
        VALUES
        (ids_in, postgis.ST_Tile(etr_result, 240, 240));
        RETURN 0;
    end if;

END;
$BODY$;


-- Calculate Real Evapotraspiration using global ETP dataset and NDVI images
--
--  INPUT
--   poly_in - given polygon for raster extraction
--   dtime_in - given dtime
--  OUTPUT
--    table with raster tiles

-- sqlstr := 'SELECT (gv).x, (gv).y, (gv).val, (gv).geom
--FROM(
 --       SELECT ST_PixelAsPolygons((SELECT ST_Union(sowing) FROM postgis.sowings)) gv ) foo';
CREATE OR REPLACE FUNCTION postgis.calculate_real_et(
    poly_in geometry,
    dtime_in timestamp)
    RETURNS RASTER
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE

AS $BODY$
DECLARE

    et0 RASTER;
    ndvi0 RASTER;
    fvc RASTER;
    aw RASTER;
    cws RASTER;

    rain_sum RASTER;  -- 29 days
    etp_sum RASTER;   -- 29 days
    rain_sum2 RASTER; -- 59 days
    etp_sum2 RASTER; -- 59 days

    landuse RASTER; -- mask image for method selection

    eta RASTER;  -- output raster

    apply_check integer;

BEGIN

    --check if it is necessary to perform control on FVC values (only for crops area)
    IF extract(month from dtime_in) >= 6
        AND extract(month from dtime_in) <= 9 THEN
        apply_check := 1;
    else
        apply_check := 0;
    end if;



    RAISE NOTICE 'Extract ETP tiles';
    SELECT postgis.st_union(etp_out) INTO et0
    FROM   agrosat.extract_etp_tiles(poly_in, dtime_in);

    RAISE NOTICE 'ETP digit to real';
    et0 := postgis.ST_MapAlgebra(et0, 1, '32BF', '[rast.val]*0.01');

    RAISE NOTICE 'calculate ETP sum 29 days -  dtime: %',dtime_in;
    SELECT postgis.ST_Union(etp_out, 'SUM') INTO etp_sum FROM agrosat.extract_etp_tiles(poly_in, dtime_in, 29);

    RAISE NOTICE 'SumETP Digit to Real';
    etp_sum := postgis.ST_MapAlgebra(etp_sum, 1, '32BF', '[rast.val]*0.01');

    RAISE NOTICE 'calculate rain sum 29 days - dtime: %',dtime_in;
    SELECT postgis.ST_Union(rain_out, 'SUM') INTO rain_sum FROM agrosat.extract_rain_tiles(poly_in, dtime_in, 29);

    RAISE NOTICE 'calculate ETP sum 59 days -  dtime: %',dtime_in;
    SELECT postgis.ST_Union(etp_out, 'SUM') INTO etp_sum2 FROM agrosat.extract_etp_tiles(poly_in, dtime_in, 59);

    RAISE NOTICE 'SumETP Digit to Real';
    etp_sum2 := postgis.ST_MapAlgebra(etp_sum2, 1, '32BF', '[rast.val]*0.01');

    RAISE NOTICE 'calculate rain sum 59 days - dtime: %',dtime_in;
    SELECT postgis.ST_Union(rain_out, 'SUM') INTO rain_sum2 FROM agrosat.extract_rain_tiles(poly_in, dtime_in, 59);


    RAISE NOTICE 'Extract NDVI';
    SELECT postgis.st_union(rast) INTO ndvi0
    FROM   postgis.ndvi INNER JOIN postgis.acquisizioni USING (id_acquisizione)
    WHERE  dtime = dtime_in
      AND    postgis.ST_Intersects(rast,poly_in);

    IF ndvi0 IS NULL THEN
        RAISE NOTICE 'ndvi not found, extract previous 8 days';
        SELECT postgis.st_union(rast) INTO ndvi0
        FROM   postgis.ndvi INNER JOIN postgis.acquisizioni USING (id_acquisizione)
        WHERE  dtime = (dtime_in - interval '8 days')
          AND    postgis.ST_Intersects(rast,poly_in);

        IF ndvi0 IS NULL THEN
            RAISE NOTICE 'image ndvi not found - Exit';
            RETURN null;
        end if;

    end if;

    RAISE NOTICE 'NDVI Digit to Real';
    ndvi0 := postgis.ST_MapAlgebra(ndvi0, 1, '32BF', '[rast.val]*0.0001');


    RAISE NOTICE 'calculate FVC';
    fvc :=  postgis.ST_MapAlgebra(ARRAY[ROW(ndvi0,1)]::rastbandarg[],
                                  'postgis.calculate_fvc_raster(double precision[], int[], text[])'::regprocedure,
                                  '32BF', 'LAST', null, 0, 0, null);

    RAISE NOTICE 'extract land cover';
    SELECT postgis.ST_Union(rast) INTO landuse
        FROM postgis.landuse
        WHERE postgis.ST_Intersects(rast, poly_in);

    RAISE NOTICE 'resample rainfall and ETP grid to landuse grid';
    rain_sum := postgis.ST_Resample(rain_sum,landuse);
    etp_sum := postgis.ST_Resample(etp_sum,landuse);
    rain_sum2 := postgis.ST_Resample(rain_sum2,landuse);
    etp_sum2 := postgis.ST_Resample(etp_sum2,landuse);
    et0 := postgis.ST_Resample(et0,landuse);
    RAISE NOTICE 'resample fvc';
    fvc := postgis.ST_Resample(fvc,landuse);

    RAISE NOTICE 'creating mask - part 1 - crops';
    landuse :=  postgis.ST_MapAlgebra(ARRAY[ROW(landuse,1),ROW(fvc,1)]::rastbandarg[],
                                  'postgis.mask_crops(double precision[], int[], text[])'::regprocedure,
                                  '8BUI', 'LAST', null, 0, 0, VARIADIC ARRAY[apply_check]::text[]);

    RAISE NOTICE 'creating mask - part 2 - forest';
    landuse :=  postgis.ST_MapAlgebra(ARRAY[ROW(landuse,1)]::rastbandarg[],
                                      'postgis.mask_forest(double precision[], int[], text[])'::regprocedure,
                                      '8BUI', 'LAST', null, 0, 0, null);

    RAISE NOTICE 'creating mask - part 3 - grassland';
    landuse :=  postgis.ST_MapAlgebra(ARRAY[ROW(landuse,1)]::rastbandarg[],
                                      'postgis.mask_grassland(double precision[], int[], text[])'::regprocedure,
                                      '8BUI', 'LAST', null, 0, 0, null);

    RAISE NOTICE 'creating mask - part 4 - other';
    landuse :=  postgis.ST_MapAlgebra(ARRAY[ROW(landuse,1)]::rastbandarg[],
                                      'postgis.mask_other(double precision[], int[], text[])'::regprocedure,
                                      '8BUI', 'LAST', null, 0, 0, null);




    RAISE NOTICE 'calculate AW phase 1';
    aw := postgis.ST_MapAlgebra(ARRAY[ROW(rain_sum,1),ROW(etp_sum,1),ROW(landuse,1)]::rastbandarg[],
                                'postgis.calculate_aw1_raster(double precision[], int[], text[])'::regprocedure,
                                '32BF', 'LAST', null, 0, 0, null);

    RAISE NOTICE 'calculate AW phase 2';
    aw := postgis.ST_MapAlgebra(ARRAY[ROW(aw,1),ROW(rain_sum2,1),ROW(etp_sum2,1)]::rastbandarg[],
                                'postgis.calculate_aw2_raster(double precision[], int[], text[])'::regprocedure,
                                '32BF', 'LAST', null, 0, 0, null);


    --  1- eto
--  2- fvc
--  4- aw
--  5- mask
    RAISE NOTICE 'calculate ETa';
    eta := postgis.ST_MapAlgebra(ARRAY[ROW(et0,1),ROW(fvc,1),ROW(aw,1),ROW(landuse,1)]::rastbandarg[],
                                 'postgis.calculate_eta_raster(double precision[], int[], text[])'::regprocedure,
                                 '32BF', 'LAST', null, 0, 0, null);

    return eta;

END;
$BODY$;


-- Extract ETP tiles from given polygon and dtime
--  simplify version
--  INPUT
--   poly_in - given polygon for extraction
--   dtime_in - given dtime
--
--  OUTPUT
--    table with raster tiles
CREATE OR REPLACE FUNCTION agrosat.extract_etp_tiles(
    poly_in geometry,
    dtime_in timestamp)
    RETURNS TABLE(etp_out raster)
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE
    ROWS 1000
AS $BODY$
DECLARE

    rowrecord RECORD;

BEGIN

    RAISE NOTICE 'Extract ETP tiles';
    FOR rowrecord IN EXECUTE 'SELECT etp as etpo FROM agrosat.r_etp
			 WHERE postgis.ST_Intersects(etp,$1) AND
			 dtime = $2' USING poly_in,dtime_in
        LOOP
            etp_out := rowrecord.etpo;
            RETURN NEXT;
        END LOOP;
END;
$BODY$;


-- Extract rainfall tiles combining preliminary and definitive data
--  INPUT
--   poly_in - given polygon for extraction
--   dtime_in - given dtime
--   days - giben back trace days
--  OUTPUT
--    table with raster tiles
CREATE OR REPLACE FUNCTION agrosat.extract_rain_tiles(
    poly_in geometry,
    dtime_in timestamp,
    days_in integer)
    RETURNS TABLE(rain_out raster, dtime_out timestamp)
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE
    ROWS 1000
AS $BODY$
DECLARE

    rowrecord RECORD;
    sqlString VARCHAR;
BEGIN

    RAISE NOTICE 'Extract rainfall tiles';
    sqlString := 'SELECT ST_Union(rast) as raino, dtime as dtimeo FROM postgis.pre_rains ' ||
                 'INNER JOIN postgis.acquisizioni USING (id_acquisizione) ' ||
                 'WHERE postgis.ST_Intersects(rast,$1) AND '||
                 'dtime between ($2 - interval '''||days_in||' days'') and $2 ' ||
                 'GROUP BY dtime ' ||
                 'UNION ' ||
                 'SELECT ST_Union(rast) as raino, dtime as dtimeo FROM postgis.precipitazioni ' ||
                 'INNER JOIN postgis.acquisizioni USING (id_acquisizione) ' ||
                 'WHERE postgis.ST_Intersects(rast,$1) AND '||
                 'dtime between ($2 - interval '''||days_in||' days'') and $2 ' ||
                 'GROUP BY dtime ' ||
                 'ORDER BY dtimeo';

    RAISE NOTICE 'SQL: %',sqlString;

    FOR rowrecord IN EXECUTE sqlString USING poly_in,dtime_in
        LOOP
            rain_out := rowrecord.raino;
            dtime_out := rowrecord.dtimeo;
            RETURN NEXT;
        END LOOP;
END;
$BODY$;


-- Extract ETP images from given polygon and dtime
--  INPUT
--   poly_in - given polygon for extraction
--   dtime_in - given dtime
--   days - giben back trace days
--  OUTPUT
--    table with raster tiles
CREATE OR REPLACE FUNCTION agrosat.extract_etp_tiles(
    poly_in geometry,
    dtime_in timestamp,
    days_in integer)
    RETURNS TABLE(etp_out raster, dtime_out timestamp)
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE
    ROWS 1000
AS $BODY$
DECLARE

    rowrecord RECORD;
    sqlString VARCHAR;
BEGIN

    RAISE NOTICE 'Extract ETP tiles';
    sqlString := 'SELECT ST_Union(etp) as etpo, dtime as dtimeo FROM agrosat.r_etp ' ||
                 'WHERE postgis.ST_Intersects(etp,$1) AND '||
                 'dtime between ($2 - interval '''||days_in||' days'') and $2 ' ||
                 'GROUP BY dtime ' ||
                 'ORDER BY dtime';

    RAISE NOTICE 'SQL: %',sqlString;

    FOR rowrecord IN EXECUTE sqlString USING poly_in,dtime_in
        LOOP
            etp_out := rowrecord.etpo;
            dtime_out := rowrecord.dtimeo;
            RETURN NEXT;
        END LOOP;
END;
$BODY$;


-- calculate FVC
-- input
--  1- NDVI
CREATE OR REPLACE FUNCTION postgis.calculate_fvc_raster(value double precision[][][], pos integer[][], VARIADIC userargs text[])
    RETURNS double precision
AS $$
BEGIN

    RETURN ((value[1][1][1] - 0.15) / (0.9 - 0.15));

END;
$$ LANGUAGE 'plpgsql' IMMUTABLE;


-- calculate aw from rain/etp at 29 days
--  this procedure will check pixel values in order to set them to 1 if fvc condition will happen
-- input
--  1- rainfall
--  2- etp
--  3- mask
-- parameters: fvc threshold and apply correction trigger
--

CREATE OR REPLACE FUNCTION postgis.calculate_aw1_raster(value double precision[][][], pos integer[][], VARIADIC userargs text[])
    RETURNS double precision
AS $$
DECLARE
    retval double precision;
BEGIN

RAISE NOTICE 'landuse: %', value[3][1][1];
    IF value[3][1][1] = 4 THEN         -- annual crop fvc >0.6
        retval := 1.0;
    ELSEIF value[3][1][1] = 1 THEN   -- annual crop fvc <= 0.6
        retval := (value[1][1][1] / value[2][1][1]);
    ELSEIF value[3][1][1] = 2 THEN   --forest
        RAISE NOTICE 'forest';
        retval := -999.0;
    ELSEIF value[3][1][1] = 3 THEN      -- grassland
        retval := (value[1][1][1] / value[2][1][1]);
    ELSE
        RAISE NOTICE 'null';
        retval := null;
    end if;

    RETURN retval;

END;
$$ LANGUAGE 'plpgsql' IMMUTABLE;

-- calculate AW final
--  this procedure will check pixel values in order to set them to 1 if fvc condition will happen
-- input
--  1- aw
--  2- rainfall2
--  3- etp2
-- parameters: fvc threshold and apply correction trigger
--

CREATE OR REPLACE FUNCTION postgis.calculate_aw2_raster(value double precision[][][], pos integer[][], VARIADIC userargs text[])
    RETURNS double precision
AS $$
DECLARE
    retval double precision;
BEGIN


   IF value[1][1][1] = -999.0 THEN
       retval := (value[2][1][1] / value[3][1][1]);
    ELSE
       retval := value[1][1][1];
   end if;

    RETURN retval;

END;
$$ LANGUAGE 'plpgsql' IMMUTABLE;

-- calculate Cws
--  this procedure will check pixel values in order to set them to 1 if fvc condition will happen
-- input
--  1- aw
--  2- mask

CREATE OR REPLACE FUNCTION postgis.calculate_cws_raster(value double precision[][][], pos integer[][], VARIADIC userargs text[])
    RETURNS double precision
AS $$
DECLARE
    retval double precision;
BEGIN
    IF value[3][1][1] = 4 THEN
        retval := 1.0;
    ELSE
        retval := (0.5 + (0.5 * value[1][1][1]));
    end if;

   -- RAISE NOTICE '% - % - % ---> %',value[2][1][1], fvc_check, value[1][1][1], retval;
    RETURN retval;

END;
$$ LANGUAGE 'plpgsql' IMMUTABLE;




-- mask forest
-- input
--  1- landuse
CREATE OR REPLACE FUNCTION postgis.mask_forest(value double precision[][][], pos integer[][], VARIADIC userargs text[])
    RETURNS double precision
AS $$
DECLARE
    retval double precision;
BEGIN

    IF value[1][1][1] = 50 OR value[1][1][1] = 60 OR value[1][1][1] = 61 OR value[1][1][1] = 62 OR value[1][1][1] = 70 OR
       value[1][1][1] = 71 OR value[1][1][1] = 72 OR value[1][1][1] = 80 OR value[1][1][1] = 81 OR value[1][1][1] = 82 OR
       value[1][1][1] = 90 OR value[1][1][1] = 100 OR value[1][1][1] = 110 OR value[1][1][1] = 120 OR value[1][1][1] = 121 OR
       value[1][1][1] = 122 THEN
        retval := 2;
    else
        retval := value[1][1][1];
    end if;

    RETURN retval;

END;
$$ LANGUAGE 'plpgsql' IMMUTABLE;

-- mask grassland
--  1- landuse
CREATE OR REPLACE FUNCTION postgis.mask_grassland(value double precision[][][], pos integer[][], VARIADIC userargs text[])
    RETURNS double precision
AS $$
DECLARE
    retval double precision;
BEGIN

    IF value[1][1][1] = 130 THEN
        retval := 3;
    else
        retval := value[1][1][1];
    end if;

    RETURN retval;

END;
$$ LANGUAGE 'plpgsql' IMMUTABLE;

-- mask not grassland, forest, crops
--  1- landuse
CREATE OR REPLACE FUNCTION postgis.mask_other(value double precision[][][], pos integer[][], VARIADIC userargs text[])
    RETURNS double precision
AS $$
DECLARE
    retval double precision;
BEGIN

    IF value[1][1][1] >= 1 AND value[1][1][1] <= 4 THEN
        retval := value[1][1][1];
    else
        retval := null;
    end if;

    RETURN retval;

END;
$$ LANGUAGE 'plpgsql' IMMUTABLE;

-- mask crops
-- input
--  1- landuse
--  2- cws
CREATE OR REPLACE FUNCTION postgis.mask_crops(value double precision[][][], pos integer[][], VARIADIC userargs text[])
    RETURNS double precision
AS $$
DECLARE
    apply_check integer := userargs[1]::integer;
    retval double precision;
BEGIN

    IF value[1][1][1] = 10 OR value[1][1][1] = 11 OR value[1][1][1] = 12 OR value[1][1][1] = 20 OR value[1][1][1] = 30 OR
       value[1][1][1] = 40 THEN
        IF apply_check = 1 AND value[2][1][1] > 0.6 THEN
            retval := 4;
        ELSE
            retval := 1;
        end if;

    else
        retval := value[1][1][1];
    end if;

    RETURN retval;

END;
$$ LANGUAGE 'plpgsql' IMMUTABLE;




-- calculate ETa - First version
-- input
--  1- eto
--  2- fvc
--  3- aw
--  4- mask: possible values - 1: annual crop FVC<=0.6 - 2: forest - 3: grassland - 4: annual crop FVC > 0.6
CREATE OR REPLACE FUNCTION postgis.calculate_eta_raster(value double precision[][][], pos integer[][], VARIADIC userargs text[])
    RETURNS double precision
AS $$
DECLARE
    kcsoil double precision := 0.2;
    kcveg double precision;
    retval double precision;
    cws_val double precision;
BEGIN
    IF value[1][1][1] is not NULL and
       value[2][1][1] is not NULL and
       value[3][1][1] is not NULL and
       value[4][1][1] is not NULL THEN


        IF value[4][1][1] = 4 THEN
            kcveg := 1.2;
            cws_val = 1.0;
        elseIf value[4][1][1] = 3 OR value[4][1][1] = 1 THEN
            kcveg := 1.2;
            cws_val := 0.5 + (0.5 * value[3][1][1]);
        ELSE
            kcveg := 0.2;
            cws_val := 0.5 + (0.5 * value[3][1][1]);
        end if;
        retval := value[1][1][1] * ((value[2][1][1] * kcveg * cws_val) + ((1.0 - value[2][1][1]) * kcsoil * value[3][1][1]));
        --RAISE NOTICE '% - % - % - % - % - % - % --->  % ',value[1][1][1],value[2][1][1],kcveg,cws_val, value[2][1][1],kcsoil,value[3][1][1] , retval;
        IF retval < 0.0 THEN
            retval := NULL;
        end if;
    ELSE
        retval := NULL;
    END IF;
    RETURN retval;

END;
$$ LANGUAGE 'plpgsql' IMMUTABLE;


-- calculate ETa globally
-- input
--  1- eto
--  2- fvc
--  3- etp_sum
--  4- rain_sum
--  5- etp_sum2
--  6- rain_sum2
--  7- landuse
-- parameters
--  kcveg, kcsoil

-- CREATE OR REPLACE FUNCTION postgis.calculate_eta_raster_g(value double precision[][][], pos integer[][], VARIADIC userargs text[])
--     RETURNS double precision
-- AS $$
-- DECLARE
--     kcveg double precision;
--     kcsoil double precision;
--     apply_check integer := userargs[1]::integer;
--     aw_val double precision;
--     cws_val double precision;
--     retval double precision;
-- BEGIN

    --check landcover type
--     IF value[7][1][1] = 10 OR value[7][1][1] = 11 OR value[7][1][1] = 12 OR value[7][1][1] = 20 OR value[7][1][1] = 30 OR
--        value[7][1][1] = 40 THEN
--         -- case Crop
--         kcveg := 1.2;
--         kcsoil := 0.2;
--         IF apply_check = 1 AND value[2][1][1] > 0.6 THEN
--             aw_val := 1.0;
--             cws_val := 1.0;
--         ELSE
--             aw_val := value[4][1][1] / value[3][1][1];
--             cws_val := (0.5 * aw_val) + 0.5;
--         end if;

--     ELSEIF value[7][1][1] = 50 OR value[7][1][1] = 60 OR value[7][1][1] = 61 OR value[7][1][1] = 62 OR value[7][1][1] = 70 OR
--            value[7][1][1] = 71 OR value[7][1][1] = 72 OR value[7][1][1] = 80 OR value[7][1][1] = 81 OR value[7][1][1] = 82 OR
--            value[7][1][1] = 90 OR value[7][1][1] = 100 OR value[7][1][1] = 110 OR value[7][1][1] = 120 OR value[7][1][1] = 121 OR
--            value[7][1][1] = 122 THEN
        --case forest
--         kcveg := 0.7;
--         kcsoil := 0.2;

--         aw_val := value[6][1][1] / value[5][1][1];
--         cws_val := (0.5 * aw_val) + 0.5;

--     ELSEIF value[7][1][1] = 130 THEN
        -- case grasslands
--        kcveg := 1.2;
--         kcsoil := 0.2;

--         aw_val := value[4][1][1] / value[3][1][1];
--         cws_val := (0.5 * aw_val) + 0.5;
--     ELSE
--         RETURN null;
--     end if;
--         retval := value[1][1][1] * ((value[2][1][1] * kcveg * cws_val) + ((1.0 - value[2][1][1]) * kcsoil * aw_val));
    --  RAISE NOTICE '% - % - % - % - % - % - % --->  % ',value[1][1][1],value[2][1][1],kcveg,value[3][1][1], value[2][1][1],kcsoil,value[4][1][1] , retval;
--     RETURN retval;

-- END;--
-- $$ LANGUAGE 'plpgsql' IMMUTABLE;



