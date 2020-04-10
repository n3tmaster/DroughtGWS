CREATE OR REPLACE FUNCTION postgis.calculate_rain_cum(
	month_in integer,
	year_in integer)
    RETURNS raster
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE
AS $BODY$

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

$BODY$;



CREATE OR REPLACE FUNCTION postgis.calculate_pre_rain_cum(
	month_in integer,
	year_in integer)
    RETURNS raster
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE
AS $BODY$

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
 			from postgis.acquisizioni a inner join postgis.pre_rain b using (id_acquisizione)
            where extract('month' from a.dtime) = month_in
            and   extract('year' from a.dtime) = year_in
            and   a.id_imgtype = 18
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

$BODY$;




-- extract avg and std values series
-- INPUT
--  imgtype
--  polygon
-- OUTPUT
--  recordset with doy, year, avg, std
create or replace function postgis.calculate_stat_series(imgtype_in varchar, poly_in geometry)
RETURNS TABLE (
  doy_out int, year_out int, avg_out double precision, std_out double precision, min_out double precision,
  max_out double precision, count_out int,
  q25_out double precision, q75_out double precision
) AS
$$
DECLARE

  rowrecord RECORD;

  ids INT;
  outstat summarystats;

BEGIN
   SELECT id_imgtype INTO ids FROM postgis.imgtypes where imgtype = upper(imgtype_in);

   RAISE NOTICE 'Extract data...%',ids;


   FOR rowrecord IN EXECUTE 'SELECT ST_Clip(ST_Union(rast),$1,true) as drast, extract(doy from dtime) as ddoy, extract(year from dtime) as dyear
        FROM postgis.'||imgtype_in||' INNER JOIN postgis.acquisizioni USING (id_acquisizione)
        WHERE ST_Intersects(rast,$1) and id_imgtype = $2
		GROUP BY ddoy, dyear
		ORDER BY dyear, ddoy' USING poly_in, ids
   LOOP
        outstat    := ST_SummaryStatsAgg(rowrecord.drast, 1, TRUE);


        q25_out    := ST_Quantile(rowrecord.drast,0.25);
        q75_out    := ST_Quantile(rowrecord.drast,0.75);
        count_out  := outstat.count;
        min_out    := outstat.min;
        max_out    := outstat.max;
        avg_out    := outstat.mean;
        std_out    := outstat.stddev;
        doy_out    := rowrecord.ddoy;
        year_out   := rowrecord.dyear;

        RETURN NEXT;
    END LOOP;

END;
$$
language 'plpgsql';



--- NEW VERSION
create or replace function postgis.calculate_stat_series(imgtype_in varchar, poly_in geometry)
RETURNS TABLE (
  doy_out int, year_out int, avg_out double precision, std_out double precision, min_out double precision,
  max_out double precision, count_out int,
  q25_out double precision, q75_out double precision
) AS
$$
DECLARE

  rowrecord RECORD;

  ids INT;
  outstat summarystats;


DECLARE

  rowrecord RECORD;
  rasttest RASTER;
  ids INT;
  outstat summarystats;
  countagg INT;
  ddoy INT;
  dyear INT;

  sqlstrpart1 VARCHAR;
  sqlstrpart2 VARCHAR;

  sqlstrpart01 VARCHAR;
  sqlstrpart02 VARCHAR;

BEGIN
  -- SELECT id_imgtype INTO ids FROM postgis.imgtypes where imgtype = upper(imgtype_in);

   RAISE NOTICE 'Testing area...';

   EXECUTE 'SELECT ST_Clip(ST_Union(rast),$1,true) as drast, extract(doy from dtime) as ddoy, extract(year from dtime) as dyear
        FROM postgis.'||imgtype_in||' INNER JOIN postgis.acquisizioni USING (id_acquisizione)
        WHERE ST_Intersects(rast,$1)
		GROUP BY ddoy, dyear
		ORDER BY dyear, ddoy
		LIMIT 1' USING poly_in INTO rasttest, ddoy, dyear;
	outstat    := ST_SummaryStatsAgg(rasttest, 1, TRUE);

	IF outstat.count = 0 THEN
		RAISE NOTICE 'Area too small, using centroid';
		sqlstrpart1 := 'ST_Centroid(';
  		sqlstrpart2 := ')';
		sqlstrpart01 := 'ST_Clip(';
  		sqlstrpart02 := ',ST_Centroid($1))';
	ELSE
		RAISE NOTICE 'Area : % - %,%,%', outstat.count, outstat.min, outstat.max, outstat.mean;
		sqlstrpart1 := '';
  		sqlstrpart2 := '';
		sqlstrpart01 := 'ST_Clip(';
  		sqlstrpart02 := ',$1,true)';
	END IF;


 RAISE NOTICE 'Extract data...';
   FOR rowrecord IN EXECUTE 'SELECT '||sqlstrpart01||'ST_Union(rast)'||sqlstrpart02||' as drast, extract(doy from dtime) as ddoy, extract(year from dtime) as dyear
        FROM postgis.'||imgtype_in||' INNER JOIN postgis.acquisizioni USING (id_acquisizione)
        WHERE ST_Intersects(rast,'||sqlstrpart1||'$1'||sqlstrpart2||')
		GROUP BY ddoy, dyear
		ORDER BY dyear, ddoy' USING poly_in
   LOOP
        outstat    := ST_SummaryStatsAgg(rowrecord.drast, 1, TRUE);


        q25_out    := ST_Quantile(rowrecord.drast,0.25);
        q75_out    := ST_Quantile(rowrecord.drast,0.75);
        count_out  := outstat.count;
        min_out    := outstat.min;
        max_out    := outstat.max;
        avg_out    := outstat.mean;
        std_out    := outstat.stddev;
        doy_out    := rowrecord.ddoy;
        year_out   := rowrecord.dyear;

        RETURN NEXT;
    END LOOP;

END;




$$
language 'plpgsql';




-- Versione che salta il controllo sul centroide
create or replace function postgis.calculate_stat_series_adv(imgtype_in varchar, poly_in geometry)
RETURNS TABLE (
  doy_out int, year_out int, avg_out double precision, std_out double precision, min_out double precision,
  max_out double precision, count_out int,
  q25_out double precision, q75_out double precision
) AS
$$
DECLARE

  rowrecord RECORD;

  ids INT;
  outstat summarystats;


DECLARE

  rowrecord RECORD;
  rasttest RASTER;
  ids INT;
  outstat summarystats;
  countagg INT;
  ddoy INT;
  dyear INT;

  sqlstrpart1 VARCHAR;
  sqlstrpart2 VARCHAR;

  sqlstrpart01 VARCHAR;
  sqlstrpart02 VARCHAR;

BEGIN
  -- SELECT id_imgtype INTO ids FROM postgis.imgtypes where imgtype = upper(imgtype_in);




		sqlstrpart1 := '';
  		sqlstrpart2 := '';
		sqlstrpart01 := 'ST_Clip(';



 RAISE NOTICE 'Extract data...';
   FOR rowrecord IN EXECUTE 'SELECT '||sqlstrpart01||'ST_Union(rast)'||sqlstrpart02||' as drast, extract(doy from dtime) as ddoy, extract(year from dtime) as dyear
        FROM postgis.'||imgtype_in||' INNER JOIN postgis.acquisizioni USING (id_acquisizione)
        WHERE ST_Intersects(rast,'||sqlstrpart1||'$1'||sqlstrpart2||')
		GROUP BY ddoy, dyear
		ORDER BY dyear, ddoy' USING poly_in
   LOOP
        outstat    := ST_SummaryStatsAgg(rowrecord.drast, 1, TRUE);


        q25_out    := ST_Quantile(rowrecord.drast,0.25);
        q75_out    := ST_Quantile(rowrecord.drast,0.75);
        count_out  := outstat.count;
        min_out    := outstat.min;
        max_out    := outstat.max;
        avg_out    := outstat.mean;
        std_out    := outstat.stddev;
        doy_out    := rowrecord.ddoy;
        year_out   := rowrecord.dyear;

        RETURN NEXT;
    END LOOP;

END;




$$
language 'plpgsql';


-- POLYGONIZE RASTER
-- procedure for polygonizing given raster.
create or replace function postgis.polygonize_raster(rast_in raster)
    RETURNS text AS
$$
DECLARE
    geojson_out text;
BEGIN

    EXECUTE 'select row_to_json(fc)
from (
			select
                        ''FeatureCollection'' as "type",
                        array_to_json(array_agg(f)) as "features"
                        from
						(select ''Feature'' as "type",
        ST_AsGeoJSON(dp.geom, 6) :: json as "geometry",
						( select json_strip_nulls(row_to_json(t))
						  from (select dp.val) t
						)as "properties"
			   FROM
			   ST_DumpAsPolygons( $1) as dp

						) as f
) as fc;

' USING  rast_in INTO geojson_out;

    return(geojson_out);
END;
$$
    language 'plpgsql';




/**
  calculate last element.
  VERSION 2.0

  extract last doy and year of given image type
 */

CREATE OR REPLACE FUNCTION postgis.calculate_last_element(
    imgtype_in character varying)
    RETURNS table (odoy int, oday int, omonth int, oyear int)
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE
AS $BODY$
BEGIN
    -- check the existence of one or more doy for given year
    RAISE NOTICE 'CHECK DOY EXISTENCE';

    RAISE NOTICE 'There are at least one doy';
    SELECT extract(doy from max(dtime)), extract(day from max(dtime)),
           extract(month from max(dtime)), extract(year from max(dtime))
    INTO   odoy, oday, omonth, oyear
    FROM   postgis.acquisizioni INNER JOIN postgis.imgtypes USING (id_imgtype)
    WHERE  imgtype = imgtype_in;

    RETURN NEXT;
END;
$BODY$;

ALTER FUNCTION postgis.calculate_last_element(character varying)
    OWNER TO postgres;






-- FUNCTION: postgis.calculate_last_element(character varying, integer)

-- DROP FUNCTION postgis.calculate_last_element(character varying, integer);
-- versione con tipo immagine e step
CREATE OR REPLACE FUNCTION postgis.calculate_last_element(
    imgtype_in character varying,
    step integer)
    RETURNS TABLE(odoy integer, oday integer, omonth integer, oyear integer)
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE
    ROWS 1000
AS $BODY$
DECLARE
    dt timestamp;
BEGIN
    -- check the existence of one or more doy for given year
    EXECUTE 'SELECT max(dtime) + interval '''||step||' days''
			FROM   postgis.acquisizioni INNER JOIN postgis.imgtypes USING (id_imgtype)
    		WHERE  imgtype = $1'
        USING   imgtype_in
        INTO   dt;

    odoy := extract(doy from dt);
    oday := extract(day from dt);
    omonth := extract(month from dt);
    oyear  := extract(year from dt);

    RETURN NEXT;
END;
$BODY$;

ALTER FUNCTION postgis.calculate_last_element(character varying, integer)
    OWNER TO postgres;




-- FUNCTION: postgis.calculate_last_element(character varying, integer)

-- DROP FUNCTION postgis.calculate_last_element(character varying, integer);
-- versione con tipo immagine e step e tile_referende
-- versione con gestione dei tile reference per migliorare la ricerca dei nuovi dati
CREATE OR REPLACE FUNCTION postgis.calculate_last_element(
    imgtype_in character varying,
    step integer,
    tile_reference character varying)
    RETURNS TABLE(odoy integer, oday integer, omonth integer, oyear integer)
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE
    ROWS 1000
AS $BODY$
DECLARE
    dt timestamp;
BEGIN
    -- check the existence of one or more doy for given year
    EXECUTE 'SELECT max(dtime) + interval '''||step||' days''
			FROM   postgis.acquisizioni INNER JOIN postgis.imgtypes USING (id_imgtype) INNER JOIN postgis.tile_references USING (id_tile_reference)
    		WHERE  imgtype = $1 and tile_ref = $2'
        USING   imgtype_in, tile_reference
        INTO   dt;

    odoy := extract(doy from dt);
    oday := extract(day from dt);
    omonth := extract(month from dt);
    oyear  := extract(year from dt);

    RETURN NEXT;
END;
$BODY$;

ALTER FUNCTION postgis.calculate_last_element(character varying, integer, character varying)
    OWNER TO postgres;




-- FUNCTION: postgis.calculate_last_element_2(character varying, integer)

-- DROP FUNCTION postgis.calculate_last_element_2(character varying, integer);
-- versione con gestione singoli tile di origine
CREATE OR REPLACE FUNCTION postgis.calculate_last_element_2(
    imgtype_in character varying,
    step integer,
    tile_reference_in character varying)
    RETURNS TABLE(odoy integer, oday integer, omonth integer, oyear integer, odoy_next integer, oday_next integer, omonth_next integer, oyear_next integer)
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE
    ROWS 1000
AS $BODY$
BEGIN
    -- check the existence of one or more doy for given year
    RAISE NOTICE 'CHECK DOY EXISTENCE';

    RAISE NOTICE 'There are at least one doy';
    EXECUTE 'SELECT extract(doy from max(dtime)), extract(day from max(dtime)),
           extract(month from max(dtime)), extract(year from max(dtime)),
		   extract(doy from max(dtime + interval '''||step||''' day)), extract(day from max(dtime + interval '''||step||''' day)),
           extract(month from max(dtime + interval '''||step||''' day)), extract(year from max(dtime + interval '''||step||''' day))
    FROM   postgis.acquisizioni
           INNER JOIN postgis.imgtypes USING (id_imgtype)
           INNER JOIN postgis.tile_references USING (id_acquisizione)
    WHERE  imgtype = $1 and tile_ref = $2'
        INTO   odoy, oday, omonth, oyear, odoy_next, oday_next, omonth_next, oyear_next
        USING  imgtype_in,tile_reference_in;


    RETURN NEXT;
END;
$BODY$;

ALTER FUNCTION postgis.calculate_last_element_2(character varying, integer, character varying)
    OWNER TO postgres;

