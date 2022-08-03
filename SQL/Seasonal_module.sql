
-- Calculate spi3 seasonal pixel value.
-- output value will be calculated taking the higher value between below, norm and above raster
-- in order to remember which value type is taken, output value will be summed to a specific coefficient (1000 for below
-- value, 2000 for norm value and 3000 for above value)
-- First raster: below_raster
-- Second raster: norm_raster
-- Third raster: above_raster
CREATE OR REPLACE FUNCTION postgis.calc_spi3_seasonal_values(value double precision[][][], pos integer[][], VARIADIC userargs text[])
	RETURNS double precision
	AS $$
    DECLARE
	out_value double precision := 0.0;
    coefficient_k integer := 0;

	BEGIN
        --check maximum value between above, norm and below images
        out_value := value[1][1][1];
        coefficient_k := 1000;

        if (out_value < value[2][1][1]) then
            out_value := value[2][1][1];
            coefficient_k := 2000;
        end if;

        if (out_value < value[3][1][1]) then
            out_value := value[3][1][1];
            coefficient_k := 3000;
        end if;

        RETURN (out_value + coefficient_k);

	END;
	$$ LANGUAGE 'plpgsql' IMMUTABLE;



-- This function calculate and extracts SPI3 seasonal forecast
create or replace function postgis.calculate_seasonal_forecast_spi3(
    poly_in geometry,
    year_in INT,
    doy_in INT,
    imgtype_in varchar
    )
RETURNS RASTER AS
$BODY$
DECLARE

 -- percentile rasters
 below_raster RASTER;
 norm_raster RASTER;
 above_raster RASTER;

 -- spi3 forecast
 rast_out RASTER;
 perc_raster RASTER;
 id_in integer;
BEGIN



    select id_acquisizione into id_in
    from postgis.acquisizioni inner join postgis.imgtypes using (id_imgtype)
    where extract(doy from dtime) = doy_in
    and   extract(year from dtime) = year_in
    and   imgtype = upper(imgtype_in);
    RAISE NOTICE 'ID: %',id_in;

    RAISE NOTICE 'get percentile rasters';

    select st_union(spi3_perc_below),
	        st_union(spi3_perc_norm),
            st_union(spi3_perc_above)
	into   below_raster,norm_raster,above_raster
    from   postgis.seasonals
    where  ST_Intersects(spi3_perc_below, poly_in)
    and    id_acquisizione = id_in;



    -- calculate output image
    RAISE NOTICE 'Calculate spi3 seasonal image';

    perc_raster := ST_MapAlgebra(ARRAY[ROW(below_raster,1),ROW(norm_raster,1),ROW(above_raster,1)]::rastbandarg[],
            'postgis.calc_spi3_seasonal_values(double precision[], int[], text[])'::regprocedure,
            '32BF',
            'LAST', null,
            0, 0,null);

    -- reclassing image
    RAISE NOTICE 'reclass image';

    rast_out := ST_Reclass(perc_raster,1,'[1040.0-1060.0):1,[1060.0-1080.0):2,[1080.0-1100.0]:3,[3040.0-3060.0):4,[3060.0-3080.0):5,[3080.0-3100.0]:6','8BUI',7);



    RAISE NOTICE 'clipping image';

    rast_out := ST_Clip(rast_out,poly_in,true);




    RAISE NOTICE 'OK';


 RETURN rast_out;
END;

$BODY$
  LANGUAGE plpgsql VOLATILE



-- calculate_new_element
--
-- calculate the last element stored into db
-- INPUT
-- imgtype_in: name of the image type
-- year_in: year for new element
-- OUTPUT
-- actual doy for given year, 0 if there are no doy for given year
create or replace function postgis.calculate_last_element(
     imgtype_in varchar, year_in int
    )
RETURNS INT AS $$
DECLARE
	doy_out INT;
BEGIN
    -- check the existence of one or more doy for given year
    RAISE NOTICE 'CHECK DOY EXISTENCE FOR YEAR: %', year_in;
    IF EXISTS (SELECT *
                FROM postgis.acquisizioni
                INNER JOIN postgis.imgtypes USING (id_imgtype)
                WHERE imgtype = imgtype_in
                AND   extract(year from dtime) = year_in) THEN

                RAISE NOTICE 'There are at least one doy';
                SELECT extract(doy from max(dtime)) into doy_out
                             FROM   postgis.acquisizioni INNER JOIN postgis.imgtypes USING (id_imgtype)
                             WHERE  imgtype = imgtype_in
                             AND    extract(year from dtime) = year_in;
	ELSE
				RAISE NOTICE 'There are no doy element for given year';
				doy_out := 0;
    END IF;



	RETURN doy_out;
END;
$$
 LANGUAGE plpgsql VOLATILE



-----
VERSIONE NUOVA

-----


-- This function calculate and extracts SPI3 seasonal forecast
create or replace function postgis.calculate_seasonal_forecast_spi3(
    poly_in geometry,
    year_in INT,
    doy_in INT,
    imgtype_in varchar
    )
RETURNS RASTER AS
$BODY$
DECLARE

 -- percentile rasters
 top_raster RASTER;
 bottom_raster RASTER;


 -- spi3 forecast
 rast_out RASTER;
 perc_raster RASTER;
 id_in integer;
BEGIN



    select id_acquisizione into id_in
    from postgis.acquisizioni inner join postgis.imgtypes using (id_imgtype)
    where extract(doy from dtime) = doy_in
    and   extract(year from dtime) = year_in
    and   imgtype = upper(imgtype_in);
    RAISE NOTICE 'ID: %',id_in;

    RAISE NOTICE 'get percentile rasters';

    select st_union(spi3_perc_top),
            st_union(spi3_perc_bottom)
	into   top_raster, bottom_raster
    from   postgis.seasonals
    where  ST_Intersects(spi3_perc_top, poly_in)
    and    id_acquisizione = id_in;



    -- calculate output image
    RAISE NOTICE 'Calculate spi3 seasonal image';

    perc_raster := ST_MapAlgebra(ARRAY[ROW(top_raster,1),ROW(bottom_raster,1)]::rastbandarg[],
            'postgis.calc_spi3_seasonal_values(double precision[], int[], text[])'::regprocedure,
            '32BF',
            'LAST', null,
            0, 0,null);

    -- reclassing image
    RAISE NOTICE 'reclass image';

    rast_out := ST_Reclass(perc_raster,1,'[2040.0-2060.0):1,[2060.0-2080.0):2,[2080.0-2100.0]:3,[1040.0-1060.0):4,[1060.0-1080.0):5,[1080.0-1100.0]:6','8BUI',7);



    RAISE NOTICE 'clipping image';

    rast_out := ST_Clip(rast_out,poly_in,true);




    RAISE NOTICE 'OK';


 RETURN rast_out;
END;

$BODY$
  LANGUAGE plpgsql VOLATILE


-- Calculate spi3 seasonal pixel value.
-- output value will be calculated taking the higher value between below, norm and above raster
-- in order to remember which value type is taken, output value will be summed to a specific coefficient (1000 for below
-- value, 2000 for norm value and 3000 for above value)
-- First raster: below_raster
-- Second raster: norm_raster
-- Third raster: above_raster
CREATE OR REPLACE FUNCTION postgis.calc_spi3_seasonal_values(value double precision[][][], pos integer[][], VARIADIC userargs text[])
	RETURNS double precision
	AS $$
    DECLARE
    val1 double precision := 0.0;
    val2 double precision := 0.0;

	out_value double precision := 0.0;
    coefficient_k integer := 0;
    thismask double precision := 40.0;

	BEGIN
        --check maximum value between above, norm and below images


        if (value[1][1][1] < thismask) then
            val1 := 0.0;
        else
            val1 := value[1][1][1];
        end if;

        if (value[2][1][1] < thismask) then
            val2 := 0.0;
        else
            val2 := value[2][1][1];
        end if;

        out_value := val1;
        coefficient_k := 1000;

        if (out_value < val2) then
            out_value := val2;
            coefficient_k := 2000;
        end if;

        RETURN (out_value + coefficient_k);

	END;
	$$ LANGUAGE 'plpgsql' IMMUTABLE;


