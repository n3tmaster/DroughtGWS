-- import seasonal
create or replace function postgis.import_seasonal_images(
    calc_year_in integer,
    calc_month_in integer,
    year_in integer,
    month_in integer,
    dataset_in varchar,
    timelapse_in varchar
    )
RETURNS boolean AS
$BODY$
DECLARE
 id_ins INT;
 id_it  INT;
 tile_in RASTER;
 id_tl INT;
 id_rid INT;
BEGIN


    SELECT id_imgtype INTO id_it FROM postgis.imgtypes WHERE imgtype = upper(dataset_in);
    SELECT id_timelapse INTO id_tl FROM postgis.timelapse WHERE timelapse = upper(timelapse_in);

    RAISE NOTICE '% - % - %',dataset_in, month_in, year_in;

    IF EXISTS(SELECT id_acquisizione FROM postgis.acquisizioni INNER JOIN postgis.imgtypes USING (id_imgtype)
               WHERE imgtype = upper(dataset_in) AND extract(month from dtime) = month_in AND extract(year from dtime) = year_in) THEN

		SELECT id_acquisizione INTO id_ins FROM postgis.acquisizioni INNER JOIN postgis.imgtypes USING (id_imgtype)
        WHERE imgtype = upper(dataset_in) AND extract(month from dtime) = month_in AND extract(year from dtime) = year_in;
	ELSE
	    RAISE NOTICE 'There are some tiles, new tiles will be updated';

        INSERT INTO postgis.acquisizioni (dtime, id_imgtype) VALUES (to_timestamp(''||year_in||'-'||month_in||'-01 00:00:00','YYYY-MM-DD HH24:MI:SS'),id_it);
		id_ins := currval('postgis.acquisizioni_id_acquisizione_seq');

    END IF;





    RAISE NOTICE 'OK';

    RAISE NOTICE 'Id: %',id_ins;


    -- Create entry for Seasonal bands

    IF EXISTS (SELECT * FROM pg_tables WHERE tablename in ('spi3_temp','spi3_perc_below_temp','spi3_perc_above_temp','spi3_perc_top_temp','spi3_perc_norm_temp','spi3_perc_bottom_temp')) THEN
        RAISE NOTICE 'Sono dentro';
		INSERT INTO postgis.seasonals (id_acquisizione, id_timelapse, calc_dtime)
        VALUES (id_ins, id_tl,to_timestamp(''||calc_year_in||'-'||calc_month_in||'-01 00:00:00','YYYY-MM-DD HH24:MI:SS'));

        id_rid := currval('postgis.seasonals_rid_seq');

        RAISE NOTICE 'Insert new tiles of image spi3';

        SELECT rast INTO tile_in FROM postgis.spi3_temp;
        UPDATE postgis.seasonals SET spi3 = tile_in WHERE rid = id_rid;

        RAISE NOTICE 'Insert new tiles of image spi3_perc_below';

        SELECT rast INTO tile_in FROM postgis.spi3_perc_below_temp;
        UPDATE postgis.seasonals SET spi3_perc_below = tile_in WHERE rid = id_rid;

        RAISE NOTICE 'Insert new tiles of image spi3_perc_above';

        SELECT rast INTO tile_in FROM postgis.spi3_perc_above_temp;
        UPDATE postgis.seasonals SET spi3_perc_above = tile_in WHERE rid = id_rid;

        RAISE NOTICE 'Insert new tiles of image spi3_perc_top';

        SELECT rast INTO tile_in FROM postgis.spi3_perc_top_temp;
        UPDATE postgis.seasonals SET spi3_perc_top = tile_in WHERE rid = id_rid;

        RAISE NOTICE 'Insert new tiles of image spi3_perc_norm';

        SELECT rast INTO tile_in FROM postgis.spi3_perc_norm_temp;
        UPDATE postgis.seasonals SET spi3_perc_norm = tile_in WHERE rid = id_rid;

        RAISE NOTICE 'Insert new tiles of image spi3_perc_bottom';

        SELECT rast INTO tile_in FROM postgis.spi3_perc_bottom_temp;
        UPDATE postgis.seasonals SET spi3_perc_bottom = tile_in WHERE rid = id_rid;

    ELSE
        RAISE NOTICE 'no table temp exist';
    END IF;

    RAISE NOTICE 'OK';

    RAISE NOTICE 'Cleaning temporary table...';

    drop table postgis.spi3_temp;
    drop table postgis.spi3_perc_below_temp;
    drop table postgis.spi3_perc_above_temp;
    drop table postgis.spi3_perc_top_temp;
    drop table postgis.spi3_perc_norm_temp;
    drop table postgis.spi3_perc_bottom_temp;

    RAISE NOTICE 'OK';

 RETURN TRUE;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE



-- import seasonal
create or replace function postgis.create_monthly_rainfall()
RETURNS boolean AS
$BODY$
DECLARE
 id_ins INT;
 id_it  INT;
 tile_in RASTER;
 id_tl INT;
BEGIN

    FOR tile_in IN SELECT rast FROM postgis.seasonal_temp
    LOOP
        INSERT INTO postgis.seasonals (id_acquisizione, rast, id_timelapse, calc_dtime)
        VALUES (id_ins, tile_in,id_tl,to_timestamp(''||calc_year_in||'-'||calc_month_in||'-01 00:00:00','YYYY-MM-DD HH24:MI:SS'));
    END LOOP;

 RETURN TRUE;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE