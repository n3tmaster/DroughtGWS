--prepare spi3 dataset
--it creates new spi3 empty rasters and inserts them into SPI3 table
--referring the coverage of rainfall estimation dataset
--Ref. SPI3 - id_imgtype = 7
create or replace function postgis.prepare_spi3_dataset()
RETURNS boolean AS
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
BEGIN


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

             EXECUTE 'INSERT INTO postgis.spi3 (id_acquisizione, rast) SELECT $1, ST_Tile(ST_AddBand(ST_MakeEmptyRaster($2),''32BF'',-1.0,-999),200,200,FALSE)' USING id_is, rast_exp;


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
RETURNS boolean AS
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
BEGIN


    select ST_Clip(rast , poly_in , true) into  rast_exp
    from   postgis.precipitazioni
    limit 1;

    FOR rowrecord IN
    SELECT extract(month from dtime) as dmonth, extract(year from dtime) as dyear
    			FROM postgis.acquisizioni
   				WHERE id_imgtype = 1
                GROUP BY 1,2
				ORDER BY 2,1 LOOP
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



             -- insert new empty raster into SPI3 table


        END IF;

        EXECUTE 'INSERT INTO postgis.spi3 (id_acquisizione, rast) SELECT $1, ST_AddBand(ST_MakeEmptyRaster($2),''32BF'',-1.0,-999)' USING id_is, rast_exp;

        RAISE NOTICE 'new SPI3 raster tiles created for %',to_timestamp(''||rowrecord.dmonth||' '||rowrecord.dyear, 'MM YYYY');


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
RETURNS boolean AS
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
BEGIN


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
RETURNS boolean AS
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
BEGIN


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






CREATE or REPLACE FUNCTION spi_on_pixel(pixel_vector double precision[]) returns double precision AS
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
CREATE or REPLACE FUNCTION postgis.spi_on_vector(integer, integer, integer, integer) returns float8[][][] AS
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
CREATE or REPLACE FUNCTION postgis.spi_on_matrix(integer) returns float8[][][] AS
$$
    library(SPEI)



    mat_points <- pg.spi.exec(sprintf("SELECT extract(month from dtime) as dmonth,
    extract(year from dtime) as dyear,
    ST_DumpValues(sum(rast),1,FALSE) as px_val
    FROM postgis.precipitazioni
    INNER JOIN postgis.acquisizioni USING (id_acquisizione)
    GROUP BY 1,2
	ORDER BY 2,1"))


    spi_out <- spi(mat_points[,3], arg1)

    msg <- spi_out
    pg.thrownotice(msg)

    outmat <- as.matrix(data.frame(vect_points[,1:2],spi_out$fitted))


	return(outmat)
$$
language 'plr' STABLE;


create or replace function postgis.prepare_spi3_data()
RETURN BOOLEAN as
$$
﻿DECLARE

	xwidth INT;
    yheight INT;

    x_iterator INT := 1;
    y_iterator INT := 1;

    v_iterator INT := 1;
    v_max INT;



    pixel_vector numeric[];
BEGIN

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
create or replace function postgis.prepare_spi_data(step_in INTEGER)
RETURNS BOOLEAN AS
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
BEGIN

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
create or replace function postgis.prepare_spi_data(step_in INTEGER, x_coord INTEGER, y_coord INTEGER, x_coord_start DOUBLE PRECISION, y_coord_start DOUBLE PRECISION)
RETURNS BOOLEAN AS
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
BEGIN

   PERFORM load_r_typenames();





    RAISE NOTICE 'Processing %, %',x_coord, y_coord;

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
               x_coord,
               y_coord,
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
END;

$$
language 'plpgsql';



WITH foo AS
    (select (rast) from postgis.spi3 as a inner join postgis.acquisizioni as b using (id_acquisizione) where extract('year' from b.dtime) = 1985 and   extract('doy' from b.dtime) = 1 )
SELECT dmonth, dyear, ST_DumpValues(mat_sum, 1, FALSE)
FROM foo;


WITH foo AS
    (SELECT extract(month from dtime) as dmonth,
           extract(year from dtime) as dyear,
           ST_Union(ST_Clip(rast, ST_GeomFromText('POLYGON((9.5 44.6, 12.5 44.6, 12.5 42.0, 9.5 42.0, 9.5 44.6))', 4326), true), 'SUM') as mat_sum
    FROM postgis.precipitazioni
    INNER JOIN postgis.acquisizioni USING (id_acquisizione)
    GROUP BY 1,2
	ORDER BY 2,1)
SELECT dmonth, dyear, ST_DumpValues(mat_sum, 1, FALSE)
FROM foo;




-- spi_on_matrix --
--  calculate SPI with specific step on vector of matrix
--  returns a matrix
--  arg1 : step
CREATE or REPLACE FUNCTION postgis.spi_on_matrix(integer) returns float8[][][] AS
$$
    library(SPEI)




    mat_points <- pg.spi.exec(sprintf("
    WITH foo AS
    (SELECT extract(month from dtime) as dmonth,
           extract(year from dtime) as dyear,
           ST_Union(ST_Clip(rast, ST_GeomFromText('POLYGON((9.5 44.6, 12.5 44.6, 12.5 42.0, 9.5 42.0, 9.5 44.6))', 4326), true), 'SUM') as mat_sum
    FROM postgis.precipitazioni
    INNER JOIN postgis.acquisizioni USING (id_acquisizione)
    GROUP BY 1,2
	ORDER BY 2,1)
    SELECT dmonth, dyear, ST_DumpValues(mat_sum, 1, FALSE)
    FROM foo"))


    spi_out <- spi(mat_points[,3], arg1)

    msg <- spi_out
    pg.thrownotice(msg)

    outmat <- as.matrix(data.frame(vect_points[,1:2],spi_out$fitted))


	return(outmat)
$$
language 'plr' STABLE;