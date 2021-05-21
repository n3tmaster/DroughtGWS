


--prepare spi3 dataset
--it creates new spi3 empty rasters and inserts them into SPI3 table
--referring the coverage of rainfall estimation dataset
--Ref. SPI3 - id_imgtype = 7
create or replace function postgis.prepare_spi3_dataset(poly_in geometry, x_div integer, y_div integer)
RETURNS boolean as
$$
DECLARE
  start_year INT;
  end_year INT;
  end_month INT;
  rowrecord RECORD;
  id_is INT;

  -- raster specs for new data creation
  xwidth INT;
  yheight INT;
  rast_exp RASTER;

  bandtypes VARCHAR:='32BF';
begin


    select ST_Clip(rast , poly_in , true) into  rast_exp
    from   postgis.precipitazioni
    limit 1;

    xwidth := ceil(ST_Width(rast_exp) / x_div);
    yheight := ceil(ST_Height(rast_exp) / y_div);

    RAISE NOTICE 'xwidth: %, yheight: %', xwidth, yheight;

    FOR rowrecord IN
    SELECT extract(month from dtime) as dmonth, extract(year from dtime) as dyear
    			FROM postgis.acquisizioni
   				WHERE id_imgtype = 1
                GROUP BY 1,2
				ORDER BY 2,1 LOOP
        -- checking if this timestamp exists in SPI3 dataset
        IF EXISTS (SELECT * FROM postgis.acquisizioni
                WHERE id_imgtype=7
                AND extract(month from dtime) = rowrecord.dmonth
                AND extract(year from dtime) = rowrecord.dyear) THEN
             RAISE NOTICE 'table exists, it will be skipped';
        ELSE
             RAISE NOTICE 'table does not exist, it will be created';

             INSERT INTO postgis.acquisizioni (dtime, id_imgtype)
             VALUES
             (to_timestamp(''||rowrecord.dmonth||' '||rowrecord.dyear, 'MM YYYY'), 7);

             -- get new id from acquisizioni
             SELECT max(id_acquisizione) into id_is
             FROM   postgis.acquisizioni
             WHERE  id_imgtype = 7;

             RAISE NOTICE 'new ID: %', id_is;

             -- insert new empty raster into SPI3 table
             EXECUTE 'INSERT INTO postgis.spi3 (id_acquisizione, rast) SELECT $1, ST_Tile(ST_AddBand(ST_MakeEmptyRaster($2),''32BF'',-1.0,-999),$3,$4,FALSE)'
             USING id_is, rast_exp, xwidth, yheight;
             RAISE NOTICE 'new SPI3 raster tiles created for %',to_timestamp(''||rowrecord.dmonth||' '||rowrecord.dyear, 'MM YYYY');

        END IF;

    END LOOP;

    RETURN true;

    EXCEPTION WHEN OTHERS THEN RETURN false;
END;
$$
language 'plpgsql';







--prepare spi3 dataset
--it creates new spi3 empty rasters and inserts them into SPI3 table
--referring the coverage of rainfall estimation dataset
--Ref. SPI3 - id_imgtype = 7
-- Version for using supplied polygon
create or replace function postgis.prepare_spi3_dataset(poly_in geometry)
RETURNS boolean as
$$
DECLARE
  start_year INT;
  end_year INT;
  end_month INT;
  rowrecord RECORD;
  id_is INT;

  -- raster specs for new data creation
  xwidth INT;
  yheight INT;
  rast_exp RASTER;

  bandtypes VARCHAR:='32BF';
begin


    select ST_Clip(rast , poly_in , true) into  rast_exp
    from   postgis.precipitazioni
    limit 1;

    RAISE NOTICE 'w: % , h: %',ST_Width(rast_exp), ST_Height(rast_exp);

    FOR rowrecord IN
    SELECT extract(month from dtime) as dmonth, extract(year from dtime) as dyear
    			FROM postgis.acquisizioni
   				WHERE id_imgtype = 1
                GROUP BY 1,2
				ORDER BY 2,1 LOOP


		RAISE NOTICE 'checking for %',to_timestamp(''||rowrecord.dmonth||' '||rowrecord.dyear, 'MM YYYY');
        -- checking if this timestamp exists in SPI3 dataset
        IF EXISTS (SELECT id_acquisizione INTO id_is FROM postgis.acquisizioni
                WHERE id_imgtype=7
                AND extract(month from dtime) = rowrecord.dmonth
                AND extract(year from dtime) = rowrecord.dyear) THEN

             RAISE NOTICE 'table exists, it will be skipped';

             RAISE NOTICE 'ID: %', id_is;

        ELSE
             RAISE NOTICE 'table does not exist, it will be created';

             INSERT INTO postgis.acquisizioni (dtime, id_imgtype)
             VALUES
             (to_timestamp(''||rowrecord.dmonth||' '||rowrecord.dyear, 'MM YYYY'), 7);

             -- get new id from acquisizioni
             SELECT max(id_acquisizione) into id_is
             FROM   postgis.acquisizioni
             WHERE  id_imgtype = 7;


        END IF;

        EXECUTE 'INSERT INTO postgis.spi3 (id_acquisizione, rast) SELECT $1, ST_AddBand(ST_MakeEmptyRaster($2),''32BF'',-1.0,-999)' USING id_is, rast_exp;

        RAISE NOTICE 'spi3 inserted';


    END LOOP;



    RETURN true;


    EXCEPTION WHEN OTHERS THEN RETURN false;
END;
$$
language 'plpgsql';



﻿--prepare spi6 dataset
--it creates new spi6 empty rasters and inserts them into SPI3 table
--referring the coverage of rainfall estimation dataset
--Ref. SPI6 - id_imgtype = 8
create or replace function postgis.prepare_spi6_dataset()
RETURNS boolean as
$$
DECLARE
  start_year INT;
  end_year INT;
  end_month INT;
  rowrecord RECORD;
  id_is INT;

  -- raster specs for new data creation
  xwidth INT;
  yheight INT;
  rast_exp RASTER;
begin


    select ST_Width(rast), ST_Height(rast), rast into xwidth, yheight, rast_exp
    from   postgis.precipitazioni
    limit 1;

    FOR rowrecord IN
    SELECT extract(month from dtime) as dmonth, extract(year from dtime) as dyear
    			FROM postgis.acquisizioni
   				WHERE id_imgtype = 1
                GROUP BY 1,2
				ORDER BY 2,1 LOOP
        -- checking if this timestamp exists in SPI3 dataset
        IF EXISTS (SELECT * FROM postgis.acquisizioni
                WHERE id_imgtype=8
                AND extract(month from dtime) = rowrecord.dmonth
                AND extract(year from dtime) = rowrecord.dyear) THEN
             RAISE NOTICE 'table exists, it will be skipped';
        ELSE
             RAISE NOTICE 'table does not exist, it will be created';

             INSERT INTO postgis.acquisizioni (dtime, id_imgtype)
             VALUES
             (to_timestamp(''||rowrecord.dmonth||' '||rowrecord.dyear, 'MM YYYY'), 8);

             -- get new id from acquisizioni
             SELECT max(id_acquisizione) into id_is
             FROM   postgis.acquisizioni
             WHERE  id_imgtype = 8;

             RAISE NOTICE 'new ID: %', id_is;

             -- insert new empty raster into SPI6 table
             EXECUTE 'INSERT INTO postgis.spi6 (id_acquisizione, rast) VALUES ($1, ST_MakeEmptyRaster($2))' USING id_is, rast_exp;


             RAISE NOTICE 'new SPI6 raster created for %',to_timestamp(''||rowrecord.dmonth||' '||rowrecord.dyear, 'MM YYYY');

        END IF;

    END LOOP;



    RETURN true;


    EXCEPTION WHEN OTHERS THEN RETURN false;
END;
$$
language 'plpgsql';



﻿--prepare spi12 dataset
--it creates new spi6 empty rasters and inserts them into SPI3 table
--referring the coverage of rainfall estimation dataset
--Ref. SPI12 - id_imgtype = 9
create or replace function postgis.prepare_spi12_dataset()
RETURNS boolean as
$$
DECLARE
  start_year INT;
  end_year INT;
  end_month INT;
  rowrecord RECORD;
  id_is INT;

  -- raster specs for new data creation
  xwidth INT;
  yheight INT;
  rast_exp RASTER;
begin


    select ST_Width(rast), ST_Height(rast), rast into xwidth, yheight, rast_exp
    from   postgis.precipitazioni
    limit 1;

    FOR rowrecord IN
    SELECT extract(month from dtime) as dmonth, extract(year from dtime) as dyear
    			FROM postgis.acquisizioni
   				WHERE id_imgtype = 1
                GROUP BY 1,2
				ORDER BY 2,1 LOOP
        -- checking if this timestamp exists in SPI3 dataset
        IF EXISTS (SELECT * FROM postgis.acquisizioni
                WHERE id_imgtype=9
                AND extract(month from dtime) = rowrecord.dmonth
                AND extract(year from dtime) = rowrecord.dyear) THEN
             RAISE NOTICE 'table exists, it will be skipped';
        ELSE
             RAISE NOTICE 'table does not exist, it will be created';

             INSERT INTO postgis.acquisizioni (dtime, id_imgtype)
             VALUES
             (to_timestamp(''||rowrecord.dmonth||' '||rowrecord.dyear, 'MM YYYY'), 9);

             -- get new id from acquisizioni
             SELECT max(id_acquisizione) into id_is
             FROM   postgis.acquisizioni
             WHERE  id_imgtype = 9;

             RAISE NOTICE 'new ID: %', id_is;

             -- insert new empty raster into SPI6 table
             EXECUTE 'INSERT INTO postgis.spi12 (id_acquisizione, rast) VALUES ($1, ST_MakeEmptyRaster($2))' USING id_is, rast_exp;


             RAISE NOTICE 'new SPI12 raster created for %',to_timestamp(''||rowrecord.dmonth||' '||rowrecord.dyear, 'MM YYYY');

        END IF;

    END LOOP;



    RETURN true;


    EXCEPTION WHEN OTHERS THEN RETURN false;
END;
$$
language 'plpgsql';






create or replace function spi_on_pixel(pixel_vector double precision[]) returns double precision as
$$
    library(spei)

	outdata <- spi(pixel_vector, 1)

	return(outdata)
$$
language 'plr';


-- spi_on_vector --
--  calculate SPI with specific step on single pixel vector
--  returns a matrix
--  arg1 : x_coord
--  arg2 : y_coord
--  arg3 : step
--  arg4 : ids_in
create or replace function postgis.spi_on_vector(integer, integer, integer, integer) returns float8[][][] as
$$
    library(SPEI)



    vect_points <- pg.spi.exec(sprintf("SELECT extract(month from dtime) as dmonth,
    extract(year from dtime) as dyear,
    round(sum(st_value(rast, 1, %s, %s, false))::numeric,2) as px_val
    FROM postgis.precipitazioni
    INNER JOIN postgis.acquisizioni USING (id_acquisizione)
    GROUP BY 1,2
	ORDER BY 2,1",arg1,arg2))


    spi_out <- spi(vect_points[,3], arg3)


    outmat <- as.matrix(data.frame(vect_points[,1:2],spi_out$fitted))


	return(outmat)
$$
language 'plr' STABLE;

-- spi_on_matrix --
--  calculate SPI with specific step on vector of matrix
--  returns a matrix
--  arg1 : step
create or replace function postgis.spi_on_matrix(integer) returns float8[][][] as
$$
    library(SPEI)



    mat_points <- pg.spi.exec(sprintf("SELECT extract(month from dtime) as dmonth,
    extract(year from dtime) as dyear,
    ST_DumpValues(sum(rast),1,FALSE) as px_val
    FROM postgis.precipitazioni
    INNER JOIN postgis.acquisizioni USING (id_acquisizione)
    GROUP BY 1,2
	ORDER BY 2,1
	limit 10"))


    spi_out <- spi(mat_points[,3], arg1)

    msg <- spi_out
    pg.thrownotice(msg)

    outmat <- as.matrix(data.frame(vect_points[,1:2],spi_out$fitted))


	return(outmat)
$$
language 'plr' STABLE;


create or replace function postgis.prepare_spi3_data()
return boolean as
$$
﻿DECLARE

	xwidth INT;
    yheight INT;

    x_iterator INT := 1;
    y_iterator INT := 1;

    v_iterator INT := 1;
    v_max INT;



    pixel_vector numeric[];
begin

   PERFORM load_r_typenames();



   select ST_Width(rast), ST_Height(rast) into xwidth, yheight
   from   postgis.precipitazioni
   limit 1;




  RAISE NOTICE 'Getting width and height of rasters...%, %',xwidth,yheight;



   LOOP
   	EXIT WHEN y_iterator = yheight;
    LOOP
    	EXIT WHEN x_iterator = xwidth;
   			RAISE NOTICE 'Processing %, %',x_iterator, y_iterator;

            pixel_vector := NULL;



            pixel_vector := postgis.spi3_on_vector(600, 200);
     --       RAISE NOTICE 'calcolato! %',pixel_vector;

            RAISE NOTICE 'start saving output';

            v_iterator := 1;
            v_max := array_length(pixel_vector, 1);


            LOOP
                EXIT WHEN v_iterator > v_max;

				RAISE NOTICE 'Iteration - % > % ',v_iterator,v_max;



                PERFORM (SELECT ST_SetValue(a.rast,1,x_iterator,y_iterator,pixel_vector[(v_iterator)][3])
                FROM   postgis.spi3 AS a INNER JOIN postgis.acquisizioni AS b USING (id_acquisizione)
                WHERE  b.id_imgtype = 7
                AND    extract(month from dtime) = pixel_vector[(v_iterator)][1]
                AND    extract(year from dtime) = pixel_vector[(v_iterator)][2]);

                v_iterator := v_iterator + 1;


            END LOOP;



      --  RAISE NOTICE 'Ecco vector finale:  %', pixel_vector;

--		pixel_vector := postgis.spi('c(1.2,3.4,3.4)', 3);


        x_iterator := x_iterator + 1;
   	END LOOP;

   	y_iterator := y_iterator + 1;
   END LOOP;

RETURN true;
END;
$$
language 'plpgsql';





﻿-- calculate SPI data for every timestamps at selected step
create or replace function postgis.prepare_spi_data(step_in integer)
RETURNS BOOLEAN as
$$
DECLARE

	xwidth INT;
    yheight INT;

    x_iterator INT := 1;
    y_iterator INT := 1;

    v_iterator INT := 1;
    v_max INT;

    imgtype_in INT;
    pixel_vector numeric[];

    ids INTEGER;
    rast_temp RASTER;
begin

   PERFORM load_r_typenames();



   select ST_Width(rast), ST_Height(rast) into xwidth, yheight
   from   postgis.precipitazioni
   limit 1;




  RAISE NOTICE 'Getting width and height of rasters...%, %, %',xwidth,yheight,step_in;



    LOOP
   	EXIT WHEN y_iterator = yheight;
        LOOP
    	EXIT WHEN x_iterator = xwidth;
   			RAISE NOTICE 'Processing %, %',x_iterator, y_iterator;

            pixel_vector := NULL;


            IF step_in = 3 THEN
                imgtype_in := 7;
            ELSEIF step_in = 6 THEN
                imgtype_in :=8;
            ELSEIF step_in = 12 THEN
                imgtype_in := 9;
            END IF;



            pixel_vector := postgis.spi_on_vector(x_iterator, y_iterator, step_in);
     --       RAISE NOTICE 'calcolato! %',pixel_vector;

            RAISE NOTICE 'start saving output';

            v_iterator := 1;
            v_max := array_length(pixel_vector, 1);


            LOOP
                EXIT WHEN v_iterator > v_max;


                EXECUTE 'SELECT ST_SetValue(a.rast,1,$5,$6,$1), id_acquisizione
                FROM   postgis.spi'||step_in||' AS a INNER JOIN postgis.acquisizioni AS b USING (id_acquisizione)
                WHERE  b.id_imgtype = $4
                AND    extract(month from dtime) = $2
                AND    extract(year from dtime) = $3' INTO rast_temp, ids USING  pixel_vector[(v_iterator)][3],pixel_vector[(v_iterator)][1],
                			pixel_vector[(v_iterator)][2],imgtype_in,
                            x_iterator,y_iterator;



                EXECUTE 'UPDATE postgis.spi'||step_in||'
                 SET rast = $1
                 WHERE id_acquisizione = $2' USING rast_temp, ids;

                --RAISE NOTICE 'Saving IDS: %', ids;

                v_iterator := v_iterator + 1;


            END LOOP;


            RAISE NOTICE 'Done %, %',x_iterator, y_iterator;
            x_iterator := x_iterator + 1;
   	    END LOOP;

        x_iterator := 1;
   	    y_iterator := y_iterator + 1;
    END LOOP;

RETURN true;
END;

$$
language 'plpgsql';



﻿-- calculate SPI data for every timestamps at selected step and selected tile
create or replace function postgis.prepare_spi_data(step_in integer, x_coord integer, y_coord integer, x_coord_start double precision, y_coord_start double precision, x0 integer, y0 integer)
RETURNS BOOLEAN as
$$
DECLARE

	xwidth INT;
    yheight INT;

    x_iterator INT := 1;
    y_iterator INT := 1;

    v_iterator INT := 1;
    v_max INT;

    imgtype_in INT;
    pixel_vector numeric[];

    ids INTEGER;
    rids INTEGER;
    rast_temp RASTER;
begin

   PERFORM load_r_typenames();





    raise NOTICE 'Processing %, %',x_coord, y_coord;

    pixel_vector := NULL;

    IF step_in = 3 THEN
        imgtype_in := 7;
    ELSEIF step_in = 6 THEN
        imgtype_in :=8;
    ELSEIF step_in = 12 THEN
        imgtype_in := 9;
    END IF;




    pixel_vector := postgis.spi_on_vector(x_coord, y_coord, step_in);

    RAISE NOTICE 'vector: %',pixel_vector;

    v_iterator := 1;
    v_max := array_length(pixel_vector, 1);


    LOOP
        EXIT WHEN v_iterator > v_max;
        RAISE NOTICE 'Values: %, %, %', pixel_vector[(v_iterator)][1],pixel_vector[(v_iterator)][2],pixel_vector[(v_iterator)][3];



        EXECUTE 'SELECT ST_SetValue(a.rast,1,$5,$6,$1), id_acquisizione, rid
        FROM   postgis.spi'||step_in||' AS a INNER JOIN postgis.acquisizioni AS b USING (id_acquisizione)
        WHERE  b.id_imgtype = $4
        AND    extract(month from dtime) = $2
        AND    extract(year from dtime) = $3
        AND    ST_UpperLeftX(rast)  = $7
        AND    ST_UpperLeftY(rast)  = $8' INTO rast_temp, ids, rids
        USING  pixel_vector[(v_iterator)][3],
               pixel_vector[(v_iterator)][1],
               pixel_vector[(v_iterator)][2],
               imgtype_in,
               x0,
               y0,
               x_coord_start,
               y_coord_start;


        RAISE NOTICE 'This id : % - %', ids, rids;

        EXECUTE 'UPDATE postgis.spi'||step_in||'
                 SET rast = $1
                 WHERE id_acquisizione = $2
                 AND   rid = $3' USING rast_temp, ids, rids;


        v_iterator := v_iterator + 1;


    END LOOP;



RETURN true;

EXCEPTION WHEN OTHERS THEN RETURN false;
END;

$$
language 'plpgsql';



with foo as
    (select (rast) from postgis.spi3 as a inner join postgis.acquisizioni as b using (id_acquisizione) where extract('year' from b.dtime) = 1985 and   extract('doy' from b.dtime) = 1 )
select dmonth, dyear, ST_DumpValues(mat_sum, 1, FALSE)
FROM foo;


with foo as
    (select extract(month from dtime) as dmonth,
           extract(year from dtime) as dyear,
           ST_Union(ST_Clip(rast, ST_GeomFromText('POLYGON((9.5 44.6, 12.5 44.6, 12.5 42.0, 9.5 42.0, 9.5 44.6))', 4326), true), 'SUM') as mat_sum
    from postgis.precipitazioni
    inner join postgis.acquisizioni using (id_acquisizione)
    group by 1,2
	order by 2,1)
select dmonth, dyear, ST_DumpValues(mat_sum, 1, FALSE)
FROM foo;




-- spi_on_matrix --
--  calculate SPI with specific step on vector of matrix
--  returns a matrix
--  arg1 : step
create or replace function postgis.spi_on_matrix(integer) returns float8[][][] as
$$
    library(SPEI)


    pg.thrownotice("prima della query")

    mat_points <- pg.spi.exec(sprintf("
    WITH foo AS
    (SELECT extract(month from dtime) as dmonth,
           extract(year from dtime) as dyear,
           ST_Union(ST_Clip(rast, ST_GeomFromText('POLYGON((9.5 44.6, 12.5 44.6, 12.5 42.0, 9.5 42.0, 9.5 44.6))', 4326), true), 'SUM') as mat_sum
    FROM postgis.precipitazioni
    INNER JOIN postgis.acquisizioni USING (id_acquisizione)
    GROUP BY 1,2
	ORDER BY 2,1)
    SELECT dmonth,dyear,mat_sum as px_val
    FROM foo"))


    pg.thrownotice(mat_points[200,1])
    pg.thrownotice(mat_points[200,2])
    pg.thrownotice(mat_points[200,3])

    pg.thrownotice("ora calcolo")
    spi_out <- apply(mat_points,MARGIN = c(1,2),FUN = spei, scale=3, na.rm=T)

    pg.thrownotice("Ho calcolato")
    msg <- spi_out
    pg.thrownotice(msg)

    outmat <- as.matrix(data.frame(vect_points[,1:2],spi_out$fitted))


	return(outmat)
$$
language 'plr' STABLE;


spi_out <- apply(mat_points,MARGIN = c(1,2),FUN = spei, scale=3, na.rm=T)


 spi_out <- spi(mat_points[,3], arg1)


vect_points <- pg.spi.exec(sprintf("SELECT extract(month from dtime) as dmonth,
    extract(year from dtime) as dyear,
    round(sum(st_value(rast, 1, %s, %s, false))::numeric,2) as px_val
    FROM postgis.precipitazioni
    INNER JOIN postgis.acquisizioni USING (id_acquisizione)
    GROUP BY 1,2
	ORDER BY 2,1",arg1,arg2))


    spi_out <- spi(vect_points[,3], arg3)


    outmat <- as.matrix(data.frame(vect_points[,1:2],spi_out$fitted))




-- SPI calculator - NEW Version
-- it works with a polygon/extent given , cumulative rainfall will be calculated before run the calculations
-- PLR funcs performs the calculation on each pixel vectors.
-- Results will be stored into new images, finally list of images will be tiled into SPI3 table.

create or replace function postgis.calculate_spi(poly_in geometry, step_in integer)
RETURNS boolean as
$$
DECLARE

  rowrecord RECORD;

  rast_exp RASTER;

  bandtypes VARCHAR:='32BF';
begin


    -- calculate monthly rainfall images

    for rowrecord in
        select extract(month from dtime) as dmonth,
               extract(year from dtime)  as dyear,
               ST_Union(rast,'SUM')      as mat_val
        from postgis.precipitazioni
        inner join postgis.acquisizioni using (id_acquisizione)
        group by 1,2
        order by 2,1 loop


    end loop;
------
    select ST_Clip(rast , poly_in , true) into  rast_exp
    from   postgis.precipitazioni
    limit 1;

    xwidth := ceil(ST_Width(rast_exp) / x_div);
    yheight := ceil(ST_Height(rast_exp) / y_div);

    RAISE NOTICE 'xwidth: %, yheight: %', xwidth, yheight;

    FOR rowrecord IN
    SELECT extract(month from dtime) as dmonth, extract(year from dtime) as dyear
    			FROM postgis.acquisizioni
   				WHERE id_imgtype = 1
                GROUP BY 1,2
				ORDER BY 2,1 LOOP
        -- checking if this timestamp exists in SPI3 dataset
        IF EXISTS (SELECT * FROM postgis.acquisizioni
                WHERE id_imgtype=7
                AND extract(month from dtime) = rowrecord.dmonth
                AND extract(year from dtime) = rowrecord.dyear) THEN
             RAISE NOTICE 'table exists, it will be skipped';
        ELSE
             RAISE NOTICE 'table does not exist, it will be created';

             INSERT INTO postgis.acquisizioni (dtime, id_imgtype)
             VALUES
             (to_timestamp(''||rowrecord.dmonth||' '||rowrecord.dyear, 'MM YYYY'), 7);

             -- get new id from acquisizioni
             SELECT max(id_acquisizione) into id_is
             FROM   postgis.acquisizioni
             WHERE  id_imgtype = 7;

             RAISE NOTICE 'new ID: %', id_is;

             -- insert new empty raster into SPI3 table
             EXECUTE 'INSERT INTO postgis.spi3 (id_acquisizione, rast) SELECT $1, ST_Tile(ST_AddBand(ST_MakeEmptyRaster($2),''32BF'',-1.0,-999),$3,$4,FALSE)'
             USING id_is, rast_exp, xwidth, yheight;
             RAISE NOTICE 'new SPI3 raster tiles created for %',to_timestamp(''||rowrecord.dmonth||' '||rowrecord.dyear, 'MM YYYY');

        END IF;

    END LOOP;

    RETURN true;

    EXCEPTION WHEN OTHERS THEN RETURN false;
END;
$$
language 'plpgsql';



-- spi_on_vector NEW VERSION --
--  calculate SPI with specific step on single pixel vector
--  returns a matrix
--  arg1 : x_coord
--  arg2 : y_coord
--  arg3 : step
--  arg4 : ids_in
create or replace function postgis.spi_on_vector2(integer) returns float8[][][] as
$$
    library(SPEI)



    vect_points <- pg.spi.exec(sprintf("SELECT dmonth, dyear,
    rast as px_val
    FROM postgis.monthly_rainfall
	ORDER BY 2,1",arg1,arg2))


    pg.thrownotice('prima di calcolo')
    pg.thrownotice(vect_points)


    spi_out <- spi(vect_points[,3], arg3)

    pg.thrownotice('dopo calcolo')
    pg.thrownotice(spi_out);

    outmat <- as.matrix(data.frame(vect_points[,1:2],spi_out$fitted))
    pg.thrownotice('dopo trasformazione')

	return(outmat)
$$
language 'plr' STABLE;



﻿-- calculate SPI data for every timestamps at selected step and selected tile NEW VERSION
create or replace function postgis.prepare_spi_data(poly_in geometry)
RETURNS BOOLEAN as
$$
DECLARE

	xwidth INT;
    yheight INT;

    x_iterator INT := 1;
    y_iterator INT := 1;

    v_iterator INT := 1;
    v_max INT;

    imgtype_in INT;
    pixel_vector numeric[];

    ids INTEGER;
    rids INTEGER;
    rast_temp RASTER;
begin

    PERFORM load_r_typenames();

    raise NOTICE 'Processing %, %',x_coord, y_coord;

    pixel_vector := NULL;

    IF step_in = 3 THEN
        imgtype_in := 7;
    ELSEIF step_in = 6 THEN
        imgtype_in :=8;
    ELSEIF step_in = 12 THEN
        imgtype_in := 9;
    END IF;



    SELECT extract(month from dtime) as dmonth, extract(year from dtime) as dyear, ST_Value(ST_Union(rast) as px_val
    FROM   postgis.monthly_rain INNER JOIN postgis.acquisizioni USING (id_acquisizione)
    WHERE  ST_Intersects(rast,ST_GeomFromText(poly_in,4326))
    GROUP BY 1,2
    ORDER BY 2,1

    xwidth := ST_Width

    pixel_vector := postgis.spi_on_vector(x_coord, y_coord, step_in);

    RAISE NOTICE 'vector: %',pixel_vector;

    v_iterator := 1;
    v_max := array_length(pixel_vector, 1);


    LOOP
        EXIT WHEN v_iterator > v_max;
        RAISE NOTICE 'Values: %, %, %', pixel_vector[(v_iterator)][1],pixel_vector[(v_iterator)][2],pixel_vector[(v_iterator)][3];



        EXECUTE 'SELECT ST_SetValue(a.rast,1,$5,$6,$1), id_acquisizione, rid
        FROM   postgis.spi'||step_in||' AS a INNER JOIN postgis.acquisizioni AS b USING (id_acquisizione)
        WHERE  b.id_imgtype = $4
        AND    extract(month from dtime) = $2
        AND    extract(year from dtime) = $3
        AND    ST_UpperLeftX(rast)  = $7
        AND    ST_UpperLeftY(rast)  = $8' INTO rast_temp, ids, rids
        USING  pixel_vector[(v_iterator)][3],
               pixel_vector[(v_iterator)][1],
               pixel_vector[(v_iterator)][2],
               imgtype_in,
               x0,
               y0,
               x_coord_start,
               y_coord_start;


        RAISE NOTICE 'This id : % - %', ids, rids;

        EXECUTE 'UPDATE postgis.spi'||step_in||'
                 SET rast = $1
                 WHERE id_acquisizione = $2
                 AND   rid = $3' USING rast_temp, ids, rids;


        v_iterator := v_iterator + 1;


    END LOOP;



RETURN true;

EXCEPTION WHEN OTHERS THEN RETURN false;
END;

$$
language 'plpgsql';



-- spi_on_vector NEW VERSION --
--  calculate SPI with specific step on single pixel vector
--  returns a matrix
--  arg1 : x_coord
--  arg2 : y_coord
--  arg3 : step
--  arg4 : ids_in
create or replace function postgis.spi_on_vector3(integer) returns float8[][][] as
$$
    library(SPEI)



    vect_points <- pg.spi.exec(sprintf("SELECT extract(month from dtime) as dmonth,extract(year from dtime) as dyear,
    ST_DumpValues(ST_Union(rast),1,FALSE) as px_val
    FROM postgis.monthly_rain INNER JOIN postgis.acquisizioni USING (id_acquisizione)
	GROUP BY 1,2
	ORDER BY 2,1"))


    pg.thrownotice('prima di calcolo')



    spi_out <- apply(vect_points,MARGIN = c(1,2),FUN = spi, scale=arg3, na.rm=T)

    pg.thrownotice('dopo calcolo')
    pg.thrownotice(spi_out);

    outmat <- as.matrix(data.frame(vect_points[,1:2],spi_out$fitted))
    pg.thrownotice('dopo trasformazione')

	return(outmat)
$$
language 'plr' STABLE;




-- spi_on_vector NEW VERSION --
--  calculate SPI with specific step on single pixel vector
--  returns a matrix
--  arg1 : x_coord
--  arg2 : y_coord
--  arg3 : step
--  arg4 : ids_in
create or replace function postgis.spi_on_vector4(integer) returns float8[][][] as
$$
    library(SPEI)



    vect_points <- pg.spi.exec(sprintf("SELECT extract(month from dtime) as dmonth,extract(year from dtime) as dyear,
    ST_DumpValues(ST_Union(rast),1,FALSE) as px_val
    FROM postgis.monthly_rain INNER JOIN postgis.acquisizioni USING (id_acquisizione)
	GROUP BY 1,2
	ORDER BY 2,1"))


    pg.thrownotice('prima di calcolo')


    spi_out <- apply(vect_points,MARGIN = c(1,2),FUN = spi, scale=arg1, na.rm=T)

    pg.thrownotice('dopo calcolo')
    pg.thrownotice(spi_out);

    outmat <- as.matrix(data.frame(vect_points[,1:2],spi_out$fitted))
    pg.thrownotice('dopo trasformazione')

	return(outmat)
$$
language 'plr' STABLE;


matrix(data = NA, nrow = 1, ncol = 1, byrow = FALSE,
       dimnames = NULL)




-- spi_on_vector NEW VERSION --
--  calculate SPI with specific step on single pixel vector
--  returns a matrix
--  arg1 : x_coord
--  arg2 : y_coord
--  arg3 : step
--  arg4 : ids_in
create or replace function postgis.spi_on_vector5(integer) returns float8[][][] as
$$
    library(SPEI)
	load_r_typenames();


    vect_points <- pg.spi.exec(sprintf("SELECT extract(month from dtime) as dmonth,extract(year from dtime) as dyear,
    ST_DumpValues(ST_Union(rast),1,FALSE) as px_val
    FROM postgis.monthly_rain INNER JOIN postgis.acquisizioni USING (id_acquisizione)
	GROUP BY 1,2
	ORDER BY 2,1"))


    pg.thrownotice('fuori dalla query')
    pg.thrownotice(vect_points[,3]);

	pg.thrownotice('dopo conversione')
    pg.thrownotice(pg.spi.factor(vect_points[,3]);



    pg.thrownotice('dopo calcolo')
    pg.thrownotice(spi_out);

    outmat <- as.matrix(data.frame(vect_points[,1:2],spi_out$fitted))
    pg.thrownotice('dopo trasformazione')

	return(outmat)
$$
language 'plr' STABLE;




----
-- VERSIONE 2.0 05/09/2018
--prepare spi3 dataset
--it creates new spi3 empty rasters and inserts them into SPI3 table
--referring the coverage of rainfall estimation dataset
--Ref. SPI3 - id_imgtype = 7
create or replace function postgis.prepare_spi3_dataset(x_div integer, y_div integer)
RETURNS boolean as
$$
DECLARE
  start_year INT;
  end_year INT;
  end_month INT;
  rowrecord RECORD;
  id_is INT;

  -- raster specs for new data creation
  xwidth INT;
  yheight INT;
  rast_exp RASTER;

  bandtypes VARCHAR:='32BF';
begin


    select rast into rast_exp
    from   postgis.precipitazioni
    limit 1;

    xwidth := ceil(ST_Width(rast_exp) / x_div);
    yheight := ceil(ST_Height(rast_exp) / y_div);

    RAISE NOTICE 'xwidth: %, yheight: %', xwidth, yheight;

    FOR rowrecord IN
    SELECT extract(month from dtime) as dmonth, extract(year from dtime) as dyear
    			FROM postgis.acquisizioni
   				WHERE id_imgtype = 1
                GROUP BY 1,2
				ORDER BY 2,1 LOOP
        -- checking if this timestamp exists in SPI3 dataset
        IF EXISTS (SELECT * FROM postgis.acquisizioni
                WHERE id_imgtype=7
                AND extract(month from dtime) = rowrecord.dmonth
                AND extract(year from dtime) = rowrecord.dyear) THEN
             RAISE NOTICE 'table exists, it will be skipped';
        ELSE
             RAISE NOTICE 'table does not exist, it will be created';

             INSERT INTO postgis.acquisizioni (dtime, id_imgtype)
             VALUES
             (to_timestamp(''||rowrecord.dmonth||' '||rowrecord.dyear, 'MM YYYY'), 7);

             -- get new id from acquisizioni
             SELECT max(id_acquisizione) into id_is
             FROM   postgis.acquisizioni
             WHERE  id_imgtype = 7;

             RAISE NOTICE 'new ID: %', id_is;

             -- insert new empty raster into SPI3 table
             EXECUTE 'INSERT INTO postgis.spi3 (id_acquisizione, rast) SELECT $1, ST_Tile(ST_AddBand(ST_MakeEmptyRaster($2),''32BF'',-1.0,-999),$3,$4,FALSE)'
             USING id_is, rast_exp, xwidth, yheight;
             RAISE NOTICE 'new SPI3 raster tiles created for %',to_timestamp(''||rowrecord.dmonth||' '||rowrecord.dyear, 'MM YYYY');

        END IF;

    END LOOP;

    RETURN true;

    EXCEPTION WHEN OTHERS THEN RETURN false;
END;
$$
language 'plpgsql';

-- VERSIONE 2.0 - 14 Settembre 2018
-- INPUT : View monthly_rainfall
create or replace function postgis.calculate_spei_matrix(monthly_rainfall) returns setof spi3 as
$$
    library(SPEI)
	load_r_typenames();


    pg.thrownotice('Extracting data...')

    vect_points <- pg.spi.exec(sprintf("month,year,
    ST_AsGDALRaster(rast,'R',ARRAY['ASCII']) as vals
    FROM postgis.monthly_rainfall
	ORDER BY 1,2"))


    pg.thrownotice('prima di calcolo')


    spi_out <- apply(vect_points,MARGIN = c(1,2),FUN = spi, scale=arg1, na.rm=T)

    pg.thrownotice('dopo calcolo')
    pg.thrownotice(spi_out);

    outmat <- as.matrix(data.frame(vect_points[,1:2],spi_out$fitted))
    pg.thrownotice('dopo trasformazione')


    pg.thrownotice('fuori dalla query')
    pg.thrownotice(vect_points[,3]);

	pg.thrownotice('dopo conversione')
    pg.thrownotice(pg.spi.factor(vect_points[,3]);



    pg.thrownotice('dopo calcolo')
    pg.thrownotice(spi_out);

    outmat <- as.matrix(data.frame(vect_points[,1:2],spi_out$fitted))
    pg.thrownotice('dopo trasformazione')

	return(outmat)
$$
language 'plr' STABLE;

	spi_out <- spi(as.numeric(as.matrix(unlist(mat_points[,3]))),arg1)


-- VERSIONE 2.0 - 14 Settembre 2018
-- INPUT : View monthly_rainfall
create or replace function postgis.create_rain_exploded() returns boolean as
$$
DECLARE
	width_rast INT;
	height_rast INT;
	n_columns INT;

	x_count INT;
	y_count INT;
	i_raster RECORD;
	icount INT;
begin


    select ST_Width(rast), ST_Height(rast) into width_rast, height_rast
	from   postgis.monthly_rainfall
	limit 1;

	n_columns := width_rast * height_rast;

	RAISE NOTICE 'Raster size: % - % , %',width_rast, height_rast, n_columns;

	RAISE NOTICE 'creating temp_table';
	create table postgis.temp_spi (dmonth int, dyear int);

	FOR icount IN 1..n_columns LOOP
	    EXECUTE 'alter table postgis.temp_spi add column series.'||icount||'double precision';
	END LOOP;

	RAISE NOTICE 'exploding data';


	FOR i_raster IN SELECT dmonth, dyear, ST_DumpValues(rast, 1, false) as vals
					FROM postgis.monthly_rainfall
					ORDER BY dyear, dmonth LOOP
		icount:=1;
		EXECUTE 'insert into postgis.temp_spi (dmonth, dyear) values $1 , $2' USING i_raster.dmonth, i_raster.dyear;

		RAISE NOTICE 'calculating %, %',i_raster.dmonth, i_raster.dyear;
		FOR ycount IN 1..height_rast LOOP
			FOR xcount IN 1..width_rast LOOP
				EXECUTE 'update postgis.temp_spi set series.'||icount||'=$1 where dmonth=$2 and dyear=$3'
				USING i_raster.vals[ycount][xcount],i_raster.dmonth,i_raster.dyear;

				icount := icount + 1;
			END LOOP;
		END LOOP;
    END LOOP;



	return(TRUE);


END
$$
language 'plpgsql' STABLE;

-- VERSIONE 3.0 (Edmondo ti voglio bene ) - con ricostruzione data.frame
-- FUNCTION: postgis.spi_on_matrix(integer)

-- DROP FUNCTION postgis.spi_on_matrix(integer);

create or replace function postgis.spi_on_matrix(
	integer)
    RETURNS setof spitemp
    LANGUAGE 'plr'

    COST 100
    VOLATILE
as $BODY$




    library(SPEI)
	library(tidyr)
	library(magrittr)
	library('stringr')
    library(parallel)
	require("RPostgreSQL")

	pg.thrownotice('Performing query...')
	conn <- dbConnect(dbDriver("PostgreSQL"), dbname="gisdb", host="149.139.16.84", port="5432", user="satserv" , password="ss!2017pwd")
	wh_rast <- dbGetQuery(conn, "select ST_Width(drast) as w, ST_Height(drast) as h from postgis.monthly_rainfall limit 1")


	mat_points <- dbGetQuery(conn, "select dmonth, dyear, ST_DumpValues(drast, 1, false) as pxval from postgis.monthly_rainfall order by 2,1 limit 30")

	pg.thrownotice(wh_rast$w)
	pg.thrownotice(wh_rast$h)
	wh <- wh_rast$w * wh_rast$h
	pg.thrownotice(wh)
	pg.thrownotice('Converting matrix values...')
	mat_points$pxval <- str_replace_all(mat_points$pxval, "[[}]]", "")
	mat_points$pxval <- str_replace_all(mat_points$pxval, "[[{]]", "")
	mat_points$pxval <- str_replace_all(mat_points$pxval, "c", "")
	mat_points$pxval <- str_replace_all(mat_points$pxval, "[[(]]", "")
    mat_points$pxval <- str_replace_all(mat_points$pxval, "[[)]]", "")

    numCores <- detectCores()
    numCores <- trunc((numCores / 2)+1)

    pg.thrownotice(numCores)
    mat_points <- separate(mat_points, pxval, into=paste('v', 1:wh, sep=''), sep='[[,]]' , extra='merge', fill='right', remove=TRUE)

	mat_points2 <- apply(mat_points,2,as.numeric)
	mat_points2 <- as.data.frame(mat_points2)

	pg.thrownotice('Calculating SPI...')


    spi_out <- mclapply(mat_points2[3:ncol(mat_points2)], FUN = spi, scale=arg3, mc.cores=(numCores))




	pg.thrownotice('Componing spi_out matrix')
	outmat <- data.frame(mat_points2[,1:2],spi_out$fitted)

    pg.thrownotice('OK')

	for(icount in 0:(wh_rast$h - 1)){
		outmat <- unite(outmat, UQ(paste('pxval',(icount+1),'')),paste('v', (1+(wh_rast$w * icount)):(wh_rast$w + (wh_rast$w * icount)), sep=''), sep=',',remove=TRUE )
		outmat[,icount+3] <- paste('{',outmat[,icount+3],'}',sep='')
	}


	outmat <- unite(outmat, pxval,3:(wh_rast$h+2), sep=',',remove=TRUE )



 	outmat$pxval <- paste('{',outmat$pxval,'}',sep='')

	return(outmat)








$BODY$;

ALTER function postgis.spi_on_matrix(integer)
    OWNER TO postgres;





-- save data from spitemp table into spi table
-- input
-- step : step for spi procedure, 3,6 or 12
-- output
-- boolean: true is ok
create or replace function postgis.save_spi_data(step integer)
RETURNS boolean as
$$
DECLARE
  w INT;
  h INT;
  rowrecord RECORD;
  spitype VARCHAR;
  imgtype_ins INT;
  id_is INT;
  rastfrom RASTER;
  rasttemp RASTER;



  bandtypes VARCHAR:='32BF';
begin


    select ST_Width(drast),
		    ST_Height(drast),
			drast
	into w, h, rastfrom
	from postgis.monthly_rainfall LIMIT 1;

    spitype := 'SPI'||step;
    SELECT id_imgtype INTO imgtype_ins FROM postgis.imgtypes WHERE imgtype = spitype;

    RAISE NOTICE 'Processing % (%) %, %',spitype,imgtype_ins,w,h;


 --   RAISE NOTICE 'Deleting old SPI3...';
 --   EXECUTE 'DELETE FROM postgis.'||spitype||'';
 --   EXECUTE 'DELETE FROM postgis.acquisizioni WHERE id_imgtype = (SELECT id_imgtype FROM postgis.imgtypes WHERE imgtype=''$1'')'
 --   USING spitype;


    FOR rowrecord IN
    SELECT dmonth,  dyear, pxval
    			FROM postgis.spitemp
				ORDER BY 2,1 LOOP



        RAISE NOTICE 'Insert acquisizione';
        INSERT INTO postgis.acquisizioni (dtime, id_imgtype)
        VALUES (to_timestamp(''||rowrecord.dmonth||' '||rowrecord.dyear, 'MM YYYY'), imgtype_ins);

        SELECT max(id_acquisizione) into id_is
        FROM   postgis.acquisizioni
        WHERE  id_imgtype = 7;

        RAISE NOTICE 'new ID: %', id_is;

        RAISE NOTICE 'creating raster from R result';
        rasttemp := ST_AddBand(ST_MakeEmptyRaster(rastfrom), '32BF', 0.0, NULL);
        RAISE NOTICE 'rastfrom- band: %, width: %, height: %, pixeltype: %',
		ST_NumBands(rastfrom), ST_Width(rastfrom), ST_Height(rastfrom), ST_BandPixelType(rastfrom,1);
		RAISE NOTICE 'rasttemp- band: %, width: %, height: %, pixeltype: %',
		ST_NumBands(rasttemp), ST_Width(rasttemp), ST_Height(rasttemp), ST_BandPixelType(rasttemp,1);

		--RAISE NOTICE 'rowrecord: %',rowrecord.pxval::double precision[][];

        EXECUTE 'INSERT INTO postgis.spi'||step||' (id_acquisizione, rast) VALUES ($1, ST_Tile($2,512,512))'
        USING id_is, ST_SetValues(rasttemp, 1, 1,1, rowrecord.pxval::double precision[][]);


	END LOOP;


    RETURN true;

    EXCEPTION WHEN OTHERS THEN RETURN false;
END;


$$
language 'plpgsql';


-- VERSIONE 4.0 - nuova funzione MCLApply
create or replace function postgis.spi_on_matrix(
	integer)
    RETURNS setof spitemp
    LANGUAGE 'plr'

    COST 100
    VOLATILE
as $BODY$




    library(SPEI)
	library(tidyr)
	library(magrittr)
	library('stringr')
    library(parallel)
	require("RPostgreSQL")

	pg.thrownotice('Performing query...')
	conn <- dbConnect(dbDriver("PostgreSQL"), dbname="gisdb", host="149.139.16.84", port="5432", user="satserv" , password="ss!2017pwd")
	wh_rast <- dbGetQuery(conn, "select ST_Width(ST_Clip(drast,ST_GeomFromText('POLYGON((9.5 44.6,12.5 44.6,12.5 42,9.5 42,9.5 44.6))',4326),true)) as w, ST_Height(ST_Clip(drast,ST_GeomFromText('POLYGON((9.5 44.6,12.5 44.6,12.5 42,9.5 42,9.5 44.6))',4326),true)) as h from postgis.monthly_rainfall limit 1")


	mat_points <- dbGetQuery(conn, "select dmonth, dyear, ST_DumpValues(ST_Clip(drast,ST_GeomFromText('POLYGON((9.5 44.6,12.5 44.6,12.5 42,9.5 42,9.5 44.6))',4326),true), 1, false) as pxval from postgis.monthly_rainfall order by 2,1")

	pg.thrownotice(wh_rast$w)
	pg.thrownotice(wh_rast$h)
	wh <- wh_rast$w * wh_rast$h
	pg.thrownotice(wh)
	pg.thrownotice('Converting matrix values...')
	mat_points$pxval <- str_replace_all(mat_points$pxval, "[[}]]", "")
	mat_points$pxval <- str_replace_all(mat_points$pxval, "[[{]]", "")
	mat_points$pxval <- str_replace_all(mat_points$pxval, "c", "")
	mat_points$pxval <- str_replace_all(mat_points$pxval, "[[(]]", "")
    mat_points$pxval <- str_replace_all(mat_points$pxval, "[[)]]", "")

    numCores <- detectCores()
    numCores <- trunc((numCores / 2)+1)

    pg.thrownotice(numCores)
    mat_points <- separate(mat_points, pxval, into=paste('v', 1:wh, sep=''), sep='[[,]]' , extra='merge', fill='right', remove=TRUE)

	mat_points2 <- apply(mat_points,2,as.numeric)
	mat_points2 <- as.data.frame(mat_points2)

	pg.thrownotice('Calculating SPI...')

    spi_out <- mclapply(mat_points2[3:ncol(mat_points2)],
    						function(x,arg1) {
								y <- spi(x,arg1)
							    return(y$fitted)
							}, mc.cores=(numCores))





	pg.thrownotice('Componing spi_out matrix')
	outmat <- data.frame(mat_points2[,1:2],spi_out)
    names(outmat)[3:ncol(outmat)] <- names(spi_out)

    pg.thrownotice('Change NA values with NULL')
    outmat[is.na(outmat)]<-'NULL'
    pg.thrownotice('OK')

	for(icount in 0:(wh_rast$h - 1)){
		outmat <- unite(outmat, UQ(paste('pxval',(icount+1),'')),paste('v', (1+(wh_rast$w * icount)):(wh_rast$w + (wh_rast$w * icount)), sep=''), sep=',',remove=TRUE )
		outmat[,icount+3] <- paste('{',outmat[,icount+3],'}',sep='')
	}


	outmat <- unite(outmat, pxval,3:(wh_rast$h+2), sep=',',remove=TRUE )



 	outmat$pxval <- paste('{',outmat$pxval,'}',sep='')


	return(outmat)








$BODY$;

ALTER function postgis.spi_on_matrix(integer)
    OWNER TO postgres;








-- VERSIONE 5.0 - nuova funzione MCLApply e ricomposizione dei dati
create or replace function postgis.spi_on_matrix(
	integer)
    RETURNS setof spitemp
    LANGUAGE 'plr'

    COST 100
    VOLATILE
as $BODY$


       library(SPEI)
	library(tidyr)
	library(magrittr)
	library('stringr')
    library(parallel)
	require("RPostgreSQL")

	pg.thrownotice('Performing query...')
	conn <- dbConnect(dbDriver("PostgreSQL"), dbname="gisdb", host="149.139.16.84", port="5432", user="satserv" , password="ss!2017pwd")


	mat_points <- dbGetQuery(conn, "select dmonth, dyear, w, h, pxval from postgis.extract_rainfall_dump('',0)")

	w <- mat_points$w[1]
	h <- mat_points$h[1]
	pg.thrownotice(w)
	pg.thrownotice(h)
	wh <- w * h

	pg.thrownotice(wh)
	pg.thrownotice('Converting matrix values...')

    mat_points$w <- NULL
	mat_points$h <- NULL

	mat_points$pxval <- str_replace_all(mat_points$pxval, "[[{]]", "")
    mat_points$pxval <- str_replace_all(mat_points$pxval, "[[}]]", "")

    numCores <- detectCores()
    numCores <- trunc((numCores / 2))

    pg.thrownotice(numCores)
    mat_points <- separate(mat_points, pxval, into=paste('v', 1:wh, sep=''), sep='[[,]]' ,  fill='right', remove=TRUE)

	mat_points2 <- apply(mat_points,2,as.numeric)
	mat_points2 <- as.data.frame(mat_points2)

	pg.thrownotice('Calculating SPI...')

	spi_out <- simplify2array(
                mclapply(mat_points2[3:ncol(mat_points2)],
                    function(x) {
                      y <- spi(x,3)
					  y$fitted[is.na(y$fitted)] <- 'NULL'
                      return(y$fitted)
                    }, mc.cores=(numCores))
                )



	pg.thrownotice('Componing spi_out matrix')
	outmat <- data.frame(mat_points2[,1:2],spi_out[,,])

    pg.thrownotice('composing output')



	for(icount in 0:(h - 1)){
		outmat <- unite(outmat, UQ(paste('pxval',(icount+1),'')),paste('v', (1+(w * icount)):(w + (w * icount)), sep=''), sep=',',remove=TRUE )
		outmat[,icount+3] <- paste('{',outmat[,icount+3],'}',sep='')
	}

    pg.thrownotice('OK')
	outmat <- unite(outmat, pxval,3:(h+2), sep=',',remove=TRUE )



 	outmat$pxval <- paste('{',outmat$pxval,'}',sep='')
    pg.thrownotice('complete')

	return(outmat)



$BODY$;

ALTER function postgis.spi_on_matrix(integer)
    OWNER TO postgres;






-- generate rainfall data as dump values
create or replace function postgis.extract_rainfall_dump(polyin varchar, limitin int)
RETURNS table (dmonth int, dyear int, w int, h int, pxval varchar) as
$$
DECLARE

  limitstr VARCHAR := '';
  polystr VARCHAR :=  '';
  wherestr VARCHAR := '';
  clipstr VARCHAR := 'rast';
  dm int := 0;
  dy int := 0;
  dw int := 0;
  dh int := 0;
  rowrecord RECORD;
  sqlstr   VARCHAR := '';

  bandtypes VARCHAR:='32BF';
begin

  if char_length(polyin) > 0 and char_length(polyin) is not null then
    wherestr := ' AND ';
    polystr  := ' ST_Intersects(rast,ST_GeomFromText('''||polyin||''',4326)) ';
    clipstr  := 'ST_Clip(rast,ST_GeomFromText('''||polyin||''',4326),true) ';
  end if;

  if limitin > 0 then

    limitstr := ' LIMIT '||limitin||' ';


  end if;

  sqlstr := 'SELECT extract(month from dtime) as dmonth, extract(year from dtime) as dyear,'||
  			'ST_Width(ST_Union('||clipstr||', ''SUM''::text)) as w, ST_Height(ST_Union('||clipstr||', ''SUM''::text)) as h '||
            'from precipitazioni inner join acquisizioni using(id_acquisizione) '||
            'where id_imgtype = 1 '||wherestr||polystr||
            'GROUP BY dmonth,dyear '||
            'LIMIT 1';

  raise NOTICE 'SQL STRING 1 : %',sqlstr;

  EXECUTE sqlstr INTO dm, dy, dw, dh;

  RAISE NOTICE 'width : %, height: %',dw,dh;

  sqlstr := 'SELECT extract(month from dtime) as dmonth, extract(year from dtime) as dyear, $1 as wout, $2 as hout,'||
            'ST_DumpValues(ST_Union('||clipstr||', ''SUM''::text),1,false) as pxval '||
            'from precipitazioni inner join acquisizioni using(id_acquisizione) '||
            'where id_imgtype = 1 '||wherestr||polystr||
            'GROUP BY dmonth,dyear,wout,hout '||
            'ORDER BY dyear,dmonth '||limitstr;

  RAISE NOTICE 'SQL STRING 2: %',sqlstr;

   FOR rowrecord IN EXECUTE sqlstr USING dw,dh
   LOOP
        dmonth := rowrecord.dmonth;
		dyear  := rowrecord.dyear;
		w      := rowrecord.wout;
		h      := rowrecord.hout;
		pxval  := rowrecord.pxval;

        RETURN NEXT;
    END LOOP;
END;
$$
language 'plpgsql';



-------

-- VERSIONE 5.0 - nuova funzione MCLApply e ricomposizione dei dati
create or replace function postgis.spi_calculation(
	integer)
    RETURNS setof spitemp
    LANGUAGE 'plr'

    COST 100
    VOLATILE
as $BODY$



       library(SPEI)
	library(tidyr)
	library(magrittr)
	library('stringr')
    library(parallel)
	require("RPostgreSQL")

	pg.thrownotice('Performing query...')
	conn <- dbConnect(dbDriver("PostgreSQL"), dbname="gisdb", host="149.139.16.84", port="5432", user="satserv" , password="ss!2017pwd")


	mat_points <- dbGetQuery(conn, "select dmonth, dyear, w, h, pxval from postgis.extract_rainfall_dump('POLYGON((9.5 44.6,12.5 44.6,12.5 42,9.5 42,9.5 44.6))',20)")

	w <- mat_points$w[1]
	h <- mat_points$h[1]
	pg.thrownotice(w)
	pg.thrownotice(h)
	wh <- w * h

	pg.thrownotice(wh)
	pg.thrownotice('Converting matrix values...')
	spistep <- arg1
    mat_points$w <- NULL
	mat_points$h <- NULL

	mat_points$pxval <- str_replace_all(mat_points$pxval, "[[{]]", "")
    mat_points$pxval <- str_replace_all(mat_points$pxval, "[[}]]", "")

    numCores <- detectCores()
    numCores <- trunc((numCores / 2))

    pg.thrownotice(numCores)
    mat_points <- separate(mat_points, pxval, into=paste('v', 1:wh, sep=''), sep='[[,]]' ,  fill='right', remove=TRUE)

	mat_points2 <- apply(mat_points,2,as.numeric)
	mat_points2 <- as.data.frame(mat_points2)

	pg.thrownotice('Calculating SPI...')

	spi_out <- simplify2array(
                mclapply(mat_points2[3:ncol(mat_points2)],
                    function(x,spistep) {
                      y <- spi(x,spistep)
					  y$fitted[is.na(y$fitted)] <- 'NULL'
                      return(y$fitted)
                    }, spistep,mc.cores=(numCores))
                )



	pg.thrownotice('Componing spi_out matrix')
	outmat <- data.frame(mat_points2[,1:2],spi_out[,,])

    pg.thrownotice('composing output')



	for(icount in 0:(h - 1)){
		outmat <- unite(outmat, UQ(paste('pxval',(icount+1),'')),paste('v', (1+(w * icount)):(w + (w * icount)), sep=''), sep=',',remove=TRUE )
		outmat[,icount+3] <- paste('{',outmat[,icount+3],'}',sep='')
	}

    pg.thrownotice('OK')
	outmat <- unite(outmat, pxval,3:(h+2), sep=',',remove=TRUE )



 	outmat$pxval <- paste('{',outmat$pxval,'}',sep='')
    pg.thrownotice('complete')

	return(outmat)


$BODY$;

ALTER function postgis.spi_calculation(integer)
    OWNER TO postgres;


----


-- VERSIONE 6.0 - nuova funzione MCLApply e ricomposizione dei dati
create or replace function postgis.spi_calculation(
	integer)
    RETURNS setof spitemp
    LANGUAGE 'plr'

    COST 100
    VOLATILE
as $BODY$



       library(SPEI)
	library(tidyr)
	library(magrittr)
	library('stringr')
    library(parallel)
	require("RPostgreSQL")

	pg.thrownotice('Performing query...')
	conn <- dbConnect(dbDriver("PostgreSQL"), dbname="gisdb", host="149.139.16.84", port="5432", user="satserv" , password="ss!2017pwd")


	mat_points <- dbGetQuery(conn, "select dmonth, dyear, w, h, pxval from postgis.extract_rainfall_dump('POLYGON((9.5 44.6,12.5 44.6,12.5 42,9.5 42,9.5 44.6))',20)")

	w <- mat_points$w[1]
	h <- mat_points$h[1]
	pg.thrownotice(w)
	pg.thrownotice(h)
	wh <- w * h

	pg.thrownotice(wh)

	pg.thrownotice('Define recomp_matrix func')
	recomp_matrix <- function(mat_in,h_in,w_in){


		for(icount in 0:(h_in - 1)){
			mat_in <- unite(mat_in, UQ(paste('pxval',(icount+1),'')),paste('v', (1+(w_in * icount)):(w_in + (w_in * icount)), sep=''), sep=',',remove=TRUE )
			mat_in[,icount+3] <- paste('{',mat_in[,icount+3],'}',sep='')
		}
		mat_in <- unite(mat_in, pxval,3:(h_in+2), sep=',',remove=TRUE )
		mat_in$pxval <- paste('{',mat_in$pxval,'}',sep='')
		return(mat_in)
	}
	pg.thrownotice('OK')

	pg.thrownotice('Converting matrix values...')
	spistep <- arg1
    mat_points$w <- NULL
	mat_points$h <- NULL

	mat_points$pxval <- str_replace_all(mat_points$pxval, "[[{]]", "")
    mat_points$pxval <- str_replace_all(mat_points$pxval, "[[}]]", "")

    numCores <- detectCores()
    numCores <- trunc((numCores / 2))

    pg.thrownotice(numCores)
    mat_points <- separate(mat_points, pxval, into=paste('v', 1:wh, sep=''), sep='[[,]]' ,  fill='right', remove=TRUE)

	mat_points2 <- apply(mat_points,2,as.numeric)
	mat_points2 <- as.data.frame(mat_points2)

	pg.thrownotice('Calculating SPI...')

	spi_out <- simplify2array(
                mclapply(mat_points2[3:ncol(mat_points2)],
                    function(x,spistep) {
                      y <- spi(x,spistep)
					  y$fitted[is.na(y$fitted)] <- 'NULL'
                      return(y$fitted)
                    }, spistep,mc.cores=(numCores))
                )


	pg.thrownotice('Componing spi_out matrix')
    outmat <- data.frame(mat_points2[,1:2],spi_out[,,])

	pg.thrownotice('composing output')


   spi_out <- mclapply(outmat,
		function(x,hi,wi) {
		  hi
					   wi
		  y <- recomp_matrix(x,hi,wi)
		  return(y)
		}, h,w,mc.cores=(numCores))


    pg.thrownotice('Closing connection')

    dbDisconnect(conn)

    pg.thrownotice('complete')

	return(outmat)


$BODY$;

ALTER function postgis.spi_calculation(integer)
    OWNER TO postgres;




-- nuova versione per calcolo dell'ultimo giorno del mese
create or replace function postgis.save_spi_data(step integer)
RETURNS boolean as
$$
DECLARE
  w INT;
  h INT;
  rowrecord RECORD;
  spitype VARCHAR;
  imgtype_ins INT;
  id_is INT;
  rastfrom RASTER;
  rasttemp RASTER;

  newdtime timestamp;

  bandtypes VARCHAR:='32BF';
begin


    select ST_Width(drast),
		    ST_Height(drast),
			drast
	into w, h, rastfrom
	from postgis.monthly_rainfall LIMIT 1;

    spitype := 'SPI'||step;
    SELECT id_imgtype INTO imgtype_ins FROM postgis.imgtypes WHERE imgtype = spitype;

    RAISE NOTICE 'Processing % (%) %, %',spitype,imgtype_ins,w,h;


 --   RAISE NOTICE 'Deleting old SPI3...';
 --   EXECUTE 'DELETE FROM postgis.'||spitype||'';
 --   EXECUTE 'DELETE FROM postgis.acquisizioni WHERE id_imgtype = (SELECT id_imgtype FROM postgis.imgtypes WHERE imgtype=''$1'')'
 --   USING spitype;


    FOR rowrecord IN
    SELECT dmonth,  dyear, pxval
    			FROM postgis.spitemp
				ORDER BY 2,1 LOOP

        newdtime := to_timestamp(''||rowrecord.dmonth||' '||rowrecord.dyear, 'MM YYYY');
		RAISE NOTICE 'DTIME: %',((newdtime + interval '1 month') - interval '1 day');

        RAISE NOTICE 'Insert acquisizione';


        INSERT INTO postgis.acquisizioni (dtime, id_imgtype)
        VALUES (((newdtime + interval '1 month') - interval '1 day'), imgtype_ins);

        SELECT max(id_acquisizione) into id_is
        FROM   postgis.acquisizioni
        WHERE  id_imgtype = imgtype_ins;

        RAISE NOTICE 'new ID: %', id_is;

        RAISE NOTICE 'creating raster from R result';
        rasttemp := ST_AddBand(ST_MakeEmptyRaster(rastfrom), '32BF', 0.0, NULL);
        RAISE NOTICE 'rastfrom- band: %, width: %, height: %, pixeltype: %',
		ST_NumBands(rastfrom), ST_Width(rastfrom), ST_Height(rastfrom), ST_BandPixelType(rastfrom,1);
		RAISE NOTICE 'rasttemp- band: %, width: %, height: %, pixeltype: %',
		ST_NumBands(rasttemp), ST_Width(rasttemp), ST_Height(rasttemp), ST_BandPixelType(rasttemp,1);

		--RAISE NOTICE 'rowrecord: %',rowrecord.pxval::double precision[][];

        EXECUTE 'INSERT INTO postgis.spi'||step||' (id_acquisizione, rast) VALUES ($1, ST_Tile($2,512,512))'
        USING id_is, ST_SetValues(rasttemp, 1, 1,1, rowrecord.pxval::double precision[][]);


	END LOOP;


    RETURN true;

    EXCEPTION WHEN OTHERS THEN RETURN false;
END;

$$
language 'plpgsql';





----Prepare SPI input metadata for multi-thread operations
---- INPUT
----  threads number
-- OUTPUT
-- RECORDSET with : ULX , ULY , WIDTH , HEIGHT of every tile
create or replace function postgis.prepare_spi_metadata(nthreads integer)
RETURNS table (x0 double precision, y0 double precision, x1 double precision, y1 double precision, w_part_out int, h_part_out int, scalex_out double precision, scaley_out double precision) as
$$
DECLARE



  rast_demo RASTER;

  w INT;
  h INT;
  w_part INT;
  h_part INT;

  cornercoord RECORD;
  tilerecord RECORD;

begin
	raise NOTICE 'Get information on input extent';

	SELECT ST_Width(rast), ST_Height(rast), rast
	INTO    w, h, rast_demo
	FROM   postgis.precipitazioni INNER JOIN postgis.acquisizioni USING (id_acquisizione)
	LIMIT 1;

	w_part := ROUND (w / ROUND(nthreads / 2));
	h_part := ROUND (h / ROUND(nthreads / 2));
	RAISE NOTICE 'W: %, H: %',w,h;
	RAISE NOTICE 'W_part: %, H_part: %',w_part,h_part;

	FOR tilerecord IN SELECT ST_Tile(rast_demo, 1, w_part, h_part) as rast_tile
					   LOOP
			cornercoord :=  ST_RasterToWorldCoord(tilerecord.rast_tile, 1, 1);
			x0 := cornercoord.longitude;
			y0 := cornercoord.latitude;
			cornercoord :=  ST_RasterToWorldCoord(tilerecord.rast_tile, (st_width(tilerecord.rast_tile)+1), (st_height(tilerecord.rast_tile)+1));
			x1 := cornercoord.longitude;
			y1 := cornercoord.latitude;
			w_part_out := st_width(tilerecord.rast_tile);
			h_part_out := st_height(tilerecord.rast_tile);
            scalex_out := st_scalex(tilerecord.rast_tile);
            scaley_out := st_scaley(tilerecord.rast_tile);

			RETURN NEXT;
	END LOOP;




END;
$$
language 'plpgsql';


------

-- VERSIONE 7.0 - nuova funzione MCLApply e ricomposizione dei dati
create or replace function postgis.spi_mc_calculation(
	integer, character, integer, numeric, numeric, integer, integer,numeric,numeric)
    RETURNS setof spitemp
    LANGUAGE 'plr'

    COST 100
    VOLATILE
as $BODY$




       library(SPEI)
	library(tidyr)
	library(magrittr)
	library('stringr')
    library(parallel)
	require("RPostgreSQL")

	pg.thrownotice('Performing query...')
	conn <- dbConnect(dbDriver("PostgreSQL"), dbname="gisdb", host="149.139.16.84", port="5432", user="satserv" , password="ss!2017pwd")


	mat_points <- dbGetQuery(conn, arg2)

	w <- mat_points$w[1]
	h <- mat_points$h[1]
	pg.thrownotice(w)
	pg.thrownotice(h)
	wh <- w * h

	pg.thrownotice(wh)
	pg.thrownotice('Converting matrix values...')
	spistep <- arg1
    mat_points$w <- NULL
	mat_points$h <- NULL

	mat_points$pxval <- str_replace_all(mat_points$pxval, "[[{]]", "")
    mat_points$pxval <- str_replace_all(mat_points$pxval, "[[}]]", "")


    numCores <- arg3

    pg.thrownotice(numCores)
    mat_points <- separate(mat_points, pxval, into=paste('v', 1:wh, sep=''), sep='[[,]]' ,  fill='right', remove=TRUE)

	mat_points2 <- apply(mat_points,2,as.numeric)
	mat_points2 <- as.data.frame(mat_points2)

	pg.thrownotice('Calculating SPI...')

	spi_out <- simplify2array(
                mclapply(mat_points2[3:ncol(mat_points2)],
                    function(x,spistep) {
                      y <- spi(x,spistep)
					  y$fitted[is.na(y$fitted)] <- 'NULL'
                      return(y$fitted)
                    }, spistep,mc.cores=(numCores))
                )



	pg.thrownotice('Componing spi_out matrix')
	outmat <- data.frame(mat_points2[,1:2],spi_out[,,])

    pg.thrownotice('composing output')



	for(icount in 0:(h - 1)){
		outmat <- unite(outmat, UQ(paste('pxval',(icount+1),'')),paste('v', (1+(w * icount)):(w + (w * icount)), sep=''), sep=',',remove=TRUE )
		outmat[,icount+3] <- paste('{',outmat[,icount+3],'}',sep='')
	}

    pg.thrownotice('OK')
	outmat <- unite(outmat, pxval,3:(h+2), sep=',',remove=TRUE )



 	outmat$pxval <- paste('{',outmat$pxval,'}',sep='')
    pg.thrownotice('add metadata')
	outmat$dxo <- arg4
    outmat$dyo <- arg5
    outmat$dw <- arg6
    outmat$dh <- arg7
    outmat$dscalex <- arg8
    outmat$dscaley <- arg9
	pg.thrownotice('complete')

	return(outmat)




$BODY$;

ALTER function postgis.spi_calculation(integer)
    OWNER TO postgres;



-- nuova versione per calcolo dell'ultimo giorno del mese
create or replace function postgis.save_spi_data2(step integer)
RETURNS boolean as
$$
DECLARE
  w INT;
  h INT;
  rowrecord RECORD;
  spitype VARCHAR;
  imgtype_ins INT;
  id_is INT;

  rasttemp RASTER;

  newdtime timestamp;

  bandtypes VARCHAR:='32BF';
begin



    spitype := 'SPI'||step;
    select id_imgtype into imgtype_ins from postgis.imgtypes where imgtype = spitype;

    raise NOTICE 'Processing % (%) %, %',spitype,imgtype_ins,w,h;


 --   RAISE NOTICE 'Deleting old SPI3...';
 --   EXECUTE 'DELETE FROM postgis.'||spitype||'';
 --   EXECUTE 'DELETE FROM postgis.acquisizioni WHERE id_imgtype = (SELECT id_imgtype FROM postgis.imgtypes WHERE imgtype=''$1'')'
 --   USING spitype;


    FOR rowrecord IN
    SELECT dmonth,  dyear, pxval, dxo, dyo, dw, dh, dscalex, dscaley
    			FROM postgis.spitemp
				ORDER BY 2,1 LOOP

        newdtime := to_timestamp(''||rowrecord.dmonth||' '||rowrecord.dyear, 'MM YYYY');
		RAISE NOTICE 'DTIME: %',((newdtime + interval '1 month') - interval '1 day');



        IF NOT EXISTS (SELECT id_acquisizione  FROM postgis.acquisizioni WHERE id_imgtype = imgtype_ins
                    AND dtime = ((newdtime + interval '1 month') - interval '1 day')) THEN
            RAISE NOTICE 'Not exists - add new acquisizione';

            INSERT INTO postgis.acquisizioni (dtime, id_imgtype)
            VALUES (((newdtime + interval '1 month') - interval '1 day'), imgtype_ins);


        END IF;

        SELECT id_acquisizione INTO id_is  FROM postgis.acquisizioni WHERE id_imgtype = imgtype_ins
                    AND dtime = ((newdtime + interval '1 month') - interval '1 day');

        RAISE NOTICE 'new ID: %', id_is;

        RAISE NOTICE 'creating raster from R result - (% %) - % x %',rowrecord.dxo,rowrecord.dyo,rowrecord.dw,rowrecord.dh;
        rasttemp := ST_AddBand(ST_MakeEmptyRaster(rowrecord.dw, rowrecord.dh, rowrecord.dxo, rowrecord.dyo, rowrecord.dscalex,rowrecord.dscaley,0,0,4326), '32BF', 0.0, NULL);

		RAISE NOTICE 'rasttemp- band: %, width: %, height: %, pixeltype: %, srid: %',
		ST_NumBands(rasttemp), ST_Width(rasttemp), ST_Height(rasttemp), ST_BandPixelType(rasttemp,1),ST_SRID(rasttemp);
        RAISE NOTICE 'rasttemp- metadata: %',
		ST_metadata(rasttemp);
		--RAISE NOTICE 'rowrecord: %',rowrecord.pxval::double precision[][];

        EXECUTE 'INSERT INTO postgis.spi'||step||' (id_acquisizione, rast) VALUES ($1, $2)'
        USING id_is, ST_SetValues(rasttemp, 1, 1,1, rowrecord.pxval::double precision[][]);


	END LOOP;


    RETURN true;

    EXCEPTION WHEN OTHERS THEN RETURN false;
END;

$$
language 'plpgsql';



-- FUNCTION: postgis.spi_calculation(integer)

-- DROP FUNCTION postgis.spi_calculation(integer);
-- VERSIONE 8
create or replace function postgis.spi_calculation(
	integer, integer)
    RETURNS SETOF spitemp
    LANGUAGE 'plr'

    COST 100
    VOLATILE
    ROWS 1000
as $BODY$

    library(SPEI)
	library(tidyr)
	library(magrittr)
	library('stringr')
    library(parallel)
	require("RPostgreSQL")

	pg.thrownotice('Performing query...')
	conn <- dbConnect(dbDriver("PostgreSQL"), dbname="gisdb", host="149.139.16.84", port="5432", user="satserv" , password="ss!2017pwd")

	mat_points <- dbGetQuery(conn, "select dmonth, dyear, w, h, pxval from postgis.extract_rainfall_dump('',0)")

	w <- mat_points$w[1]
	h <- mat_points$h[1]
	pg.thrownotice(w)
	pg.thrownotice(h)
	wh <- w * h

	pg.thrownotice(wh)
	pg.thrownotice('Converting matrix values...')
	spistep <- arg1
    mat_points$w <- NULL
	mat_points$h <- NULL

	mat_points$pxval <- str_replace_all(mat_points$pxval, "[[{]]", "")
    mat_points$pxval <- str_replace_all(mat_points$pxval, "[[}]]", "")


    numCores <- arg2

    pg.thrownotice(numCores)
    mat_points <- separate(mat_points, pxval, into=paste('v', 1:wh, sep=''), sep='[[,]]' ,  fill='right', remove=TRUE)

	mat_points2 <- apply(mat_points,2,as.numeric)
	mat_points2 <- as.data.frame(mat_points2)

	pg.thrownotice('Calculating SPI...')

	spi_out <- simplify2array(
                mclapply(mat_points2[3:ncol(mat_points2)],
                    function(x,spistep) {
                      y <- spi(x,spistep)
					  y$fitted[is.na(y$fitted)] <- 'NULL'
                      return(y$fitted)
                    }, spistep,mc.cores=(numCores))
                )

	pg.thrownotice('Componing spi_out matrix')
	outmat <- data.frame(mat_points2[,1:2],spi_out[,,])

    pg.thrownotice('composing output')

	for(icount in 0:(h - 1)){
		outmat <- unite(outmat, UQ(paste('pxval',(icount+1),'')),paste('v', (1+(w * icount)):(w + (w * icount)), sep=''), sep=',',remove=TRUE )
		outmat[,icount+3] <- paste('{',outmat[,icount+3],'}',sep='')
	}

    pg.thrownotice('OK')
	outmat <- unite(outmat, pxval,3:(h+2), sep=',',remove=TRUE )

 	outmat$pxval <- paste('{',outmat$pxval,'}',sep='')
    pg.thrownotice('complete')

	return(outmat)

$BODY$;

ALTER function postgis.spi_calculation(integer)
    OWNER TO postgres;




-- generate rainfall data as dump values
-- VERSION 2.0 - gestione simultanea preliminari e definitive
create or replace function postgis.extract_rainfall_dump(polyin varchar, limitin int)
RETURNS table (dmonth int, dyear int, w int, h int, pxval varchar) as
$$
DECLARE

  limitstr VARCHAR := '';
  polystr VARCHAR :=  '';
  wherestr VARCHAR := '';
  clipstr VARCHAR := 'rast';
  dm int := 0;
  dy int := 0;
  dw int := 0;
  dh int := 0;
  rowrecord RECORD;
  sqlstr   VARCHAR := '';

  to_thisdata timestamp;
  bandtypes VARCHAR:='32BF';
begin

  if char_length(polyin) > 0 and char_length(polyin) is not null then
    wherestr := ' AND ';
    polystr  := ' ST_Intersects(rast,ST_GeomFromText('''||polyin||''',4326)) ';
    clipstr  := 'ST_Clip(rast,ST_GeomFromText('''||polyin||''',4326),true) ';
  end if;

  if limitin > 0 then

    limitstr := ' LIMIT '||limitin||' ';


  end if;


  SELECT date_trunc('month', current_date) INTO to_thisdata
  FROM postgis.acquisizioni where id_imgtype = 18;

  RAISE NOTICE 'Get last usable timestamp from preliminary data: %',to_thisdata;

  sqlstr := 'SELECT extract(month from dtime) as dmonth, extract(year from dtime) as dyear,'||
  			'ST_Width(ST_Union('||clipstr||', ''SUM''::text)) as w, ST_Height(ST_Union('||clipstr||', ''SUM''::text)) as h '||
            'from precipitazioni inner join acquisizioni using(id_acquisizione) '||
            'where id_imgtype = 1 '||wherestr||polystr||
            'GROUP BY dmonth,dyear '||
            'LIMIT 1';

  raise NOTICE 'SQL STRING 1 : %',sqlstr;

  EXECUTE sqlstr INTO dm, dy, dw, dh;

  RAISE NOTICE 'width : %, height: %',dw,dh;

  sqlstr := 'SELECT extract(month from dtime) as dmonth, extract(year from dtime) as dyear, $1 as wout, $2 as hout,'||
            'ST_DumpValues(ST_Union('||clipstr||', ''SUM''::text),1,false) as pxval '||
            'from precipitazioni inner join acquisizioni using(id_acquisizione) '||
            'where id_imgtype = 1 '||wherestr||polystr||
            'GROUP BY dmonth,dyear,wout,hout '||
            'UNION '||
            'SELECT extract(month from dtime) as dmonth, extract(year from dtime) as dyear, $1 as wout, $2 as hout,'||
            'ST_DumpValues(ST_Union('||clipstr||', ''SUM''::text),1,false) as pxval '||
            'from pre_rains inner join acquisizioni using(id_acquisizione) '||
            'where id_imgtype = 18 '||wherestr||polystr||' AND dtime < $3'
            'GROUP BY dmonth,dyear,wout,hout '||
            'ORDER BY dyear,dmonth '||limitstr;

  RAISE NOTICE 'SQL STRING 2: %',sqlstr;

   FOR rowrecord IN EXECUTE sqlstr USING dw,dh,to_thisdata
   LOOP
        dmonth := rowrecord.dmonth;
		dyear  := rowrecord.dyear;
		w      := rowrecord.wout;
		h      := rowrecord.hout;
		pxval  := rowrecord.pxval;

        RETURN NEXT;
    END LOOP;
END;
$$
language 'plpgsql';




-- import seasonal images -
-- VERSION 2.0
CREATE OR REPLACE FUNCTION postgis.import_seasonal_images(
	calc_year_in integer,
	calc_month_in integer,
	year_in integer,
	month_in integer,
	dataset_in character varying,
	timelapse_in character varying)
    RETURNS boolean
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE
AS $BODY$
DECLARE
 id_ins INT;
 id_it  INT;
 tile_in RASTER;
 id_tl INT;
 id_rid INT;

 upperleftx double precision;
 upperlefty double precision;

 pixelh double precision;
 pixelw double precision;
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

        INSERT INTO postgis.acquisizioni (dtime, id_imgtype) VALUES ((to_timestamp(''||year_in||'-'||month_in||'-01 00:00:00','YYYY-MM-DD HH24:MI:SS') + interval '1 month' - interval '1 day'),id_it);
		id_ins := currval('postgis.acquisizioni_id_acquisizione_seq');

    END IF;

    RAISE NOTICE 'OK';

    RAISE NOTICE 'Id: %',id_ins;

    -- Create entry for Seasonal bands

    IF EXISTS (SELECT * FROM pg_tables WHERE tablename in ('spi3_temp','spi3_perc_below_temp','spi3_perc_above_temp','spi3_perc_top_temp','spi3_perc_norm_temp','spi3_perc_bottom_temp')) THEN
        RAISE NOTICE 'Sono dentro';
		INSERT INTO postgis.seasonals (id_acquisizione, id_timelapse, calc_dtime)
        VALUES (id_ins, id_tl,(to_timestamp(''||calc_year_in||'-'||calc_month_in||'-01 00:00:00','YYYY-MM-DD HH24:MI:SS') + interval '1 month' - interval '1 day'));

        id_rid := currval('postgis.seasonals_rid_seq');

        RAISE NOTICE 'Insert new tiles of image spi3';

        SELECT rast INTO tile_in FROM postgis.spi3_temp;

   --     upperleftx := ST_UpperLeftX(tile_in);
   --     upperlefty := ST_UpperLeftY(tile_in);
   --     pixelh := ST_PixelHeight(tile_in);
   --     pixelw := ST_PixelWidth(tile_in);
   --     upperleftx := upperleftx - (pixelw);
   --     upperlefty := upperlefty - (pixelh * 4);
   --     tile_in := ST_SetUpperLeft(tile_in, upperleftx, upperlefty);

        UPDATE postgis.seasonals SET spi3 = tile_in WHERE rid = id_rid;

        RAISE NOTICE 'Insert new tiles of image spi3_perc_below';

        SELECT rast INTO tile_in FROM postgis.spi3_perc_below_temp;

    --    upperleftx := ST_UpperLeftX(tile_in);
    --    upperlefty := ST_UpperLeftY(tile_in);
    --    pixelh := ST_PixelHeight(tile_in);
    --    pixelw := ST_PixelWidth(tile_in);
    --    upperleftx := upperleftx - (pixelw);
    --    upperlefty := upperlefty - (pixelh * 4);
    --    tile_in := ST_SetUpperLeft(tile_in, upperleftx, upperlefty);


        UPDATE postgis.seasonals SET spi3_perc_below = tile_in WHERE rid = id_rid;

        RAISE NOTICE 'Insert new tiles of image spi3_perc_above';

        SELECT rast INTO tile_in FROM postgis.spi3_perc_above_temp;

    --    upperleftx := ST_UpperLeftX(tile_in);
    --    upperlefty := ST_UpperLeftY(tile_in);
    --    pixelh := ST_PixelHeight(tile_in);
    --    pixelw := ST_PixelWidth(tile_in);
    --    upperleftx := upperleftx - (pixelw);
    --    upperlefty := upperlefty - (pixelh * 4);
    --    tile_in := ST_SetUpperLeft(tile_in, upperleftx, upperlefty);


        UPDATE postgis.seasonals SET spi3_perc_above = tile_in WHERE rid = id_rid;

        RAISE NOTICE 'Insert new tiles of image spi3_perc_top';

        SELECT rast INTO tile_in FROM postgis.spi3_perc_top_temp;

    --    upperleftx := ST_UpperLeftX(tile_in);
    --    upperlefty := ST_UpperLeftY(tile_in);
    --    pixelh := ST_PixelHeight(tile_in);
    --    pixelw := ST_PixelWidth(tile_in);
    --    upperleftx := upperleftx - (pixelw);
    --    upperlefty := upperlefty - (pixelh * 4);
    --    tile_in := ST_SetUpperLeft(tile_in, upperleftx, upperlefty);


        UPDATE postgis.seasonals SET spi3_perc_top = tile_in WHERE rid = id_rid;

        RAISE NOTICE 'Insert new tiles of image spi3_perc_norm';

        SELECT rast INTO tile_in FROM postgis.spi3_perc_norm_temp;

    --    upperleftx := ST_UpperLeftX(tile_in);
    --    upperlefty := ST_UpperLeftY(tile_in);
    --    pixelh := ST_PixelHeight(tile_in);
    --    pixelw := ST_PixelWidth(tile_in);
    --    upperleftx := upperleftx - (pixelw);
    --    upperlefty := upperlefty - (pixelh * 4);
    --    tile_in := ST_SetUpperLeft(tile_in, upperleftx, upperlefty);

        UPDATE postgis.seasonals SET spi3_perc_norm = tile_in WHERE rid = id_rid;

        RAISE NOTICE 'Insert new tiles of image spi3_perc_bottom';

        SELECT rast INTO tile_in FROM postgis.spi3_perc_bottom_temp;

    --    upperleftx := ST_UpperLeftX(tile_in);
    --    upperlefty := ST_UpperLeftY(tile_in);
    --    pixelh := ST_PixelHeight(tile_in);
    --    pixelw := ST_PixelWidth(tile_in);
    --
    --    upperlefty := upperlefty - (pixelh * 4);
    --    upperleftx := upperleftx - (pixelw);
    --    tile_in := ST_SetUpperLeft(tile_in, upperleftx, upperlefty);

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
$BODY$;

