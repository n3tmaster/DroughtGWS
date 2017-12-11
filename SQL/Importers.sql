-- import all rainfall data library
create or replace function postgis.import_rainfall_images(
    year_start integer,
    year_end integer,
    month_start integer,
    month_end integer
    )
RETURNS boolean AS
$BODY$
DECLARE
 mcount INT;
 ycount INT;
 id_ins INT;
BEGIN

 ycount := year_start;
 mcount := month_start;

 LOOP
 	if ycount <= year_end then
    	LOOP
            if (mcount <= 12 AND ycount < year_end) OR (mcount <= month_end AND ycount = year_end) then
        		RAISE NOTICE 'Inserisco acquisizione : % - %: %', ycount, mcount,''||ycount||'-'||mcount||'-01 00:00:00';

                if mcount < 10 then
                	insert into postgis.acquisizioni (dtime, id_imgtype) values (to_timestamp(''||ycount||'-0'||mcount||'-01 00:00:00','YYYY-MM-DD HH24:MI:SS'),1);
            	else
                 	insert into postgis.acquisizioni (dtime, id_imgtype) values (to_timestamp(''||ycount||'-'||mcount||'-01 00:00:00','YYYY-MM-DD HH24:MI:SS'),1);
                end if;
                RAISE NOTICE 'OK';

                select max(id_acquisizione) into id_ins
                from   postgis.acquisizioni;

                RAISE NOTICE 'New id: %',id_ins;

                if mcount < 10 then

                	EXECUTE format('
                	insert into postgis.precipitazioni (id_acquisizione, rast)
                	select '||id_ins||', rast
                	from   %I','prec_'||ycount||'_0'||mcount);
                else
                	EXECUTE format('
                	insert into postgis.precipitazioni (id_acquisizione, rast)
                	select '||id_ins||', rast
                	from   %I','prec_'||ycount||'_'||mcount);
                end if;


                RAISE NOTICE 'OK';

            else
                mcount := month_start;
            	EXIT;
            end if;

        	mcount := mcount + 1;
        END LOOP;
    else
        EXIT;
    end if;

    ycount := ycount + 1;
 END LOOP;

 RETURN TRUE;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE


-- delete temporary rainfall table
﻿create or replace function postgis.clean_temp_rainfall_tables(
    year_start integer,
    year_end integer,
    month_start integer,
    month_end integer
    )
RETURNS boolean AS
$BODY$
DECLARE
 mcount INT;
 ycount INT;
 id_ins INT;
BEGIN

 ycount := year_start;
 mcount := month_start;

 LOOP
 	if ycount <= year_end then
    	LOOP
            if (mcount <= 12 AND ycount < year_end) OR (mcount <= month_end AND ycount = year_end) then
        		RAISE NOTICE 'Drop table : % - %: %', ycount, mcount,''||ycount||'-'||mcount||'-01 00:00:00';


                if mcount < 10 then

                	EXECUTE format('
                	drop table %I','prec_'||ycount||'_0'||mcount);
                else
                	EXECUTE format('
                	drop table %I','prec_'||ycount||'_'||mcount);
                end if;


                RAISE NOTICE 'OK';

            else
                mcount := month_start;
            	EXIT;
            end if;

        	mcount := mcount + 1;
        END LOOP;
    else
        EXIT;
    end if;

    ycount := ycount + 1;
 END LOOP;

 RETURN TRUE;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE



-- import LST data into DB model
-- INPUT
--   year_start
--   year_end
--   gg_start
--   gg_end
--
-- OUTPUT
--   Boolean : TRUE al is ok, FALSE error
create or replace function postgis.import_lst_images(
    year_start integer,
    year_end integer,
    gg_start integer,
    gg_end integer
    )
RETURNS boolean AS
$BODY$
DECLARE
 mcount INT;
 ycount INT;
 id_ins INT;
BEGIN

 ycount := year_start;
 mcount := gg_start;

 LOOP
 	if ycount <= year_end then
    	LOOP
            if (mcount <= gg_end) then
        		RAISE NOTICE 'Inserisco acquisizione : % - %: %', ycount, mcount,''||ycount||'-'||mcount||' 00:00:00';

                insert into postgis.acquisizioni (dtime, id_imgtype) values (to_timestamp(''||ycount||'-'||mcount||' 00:00:00','YYYY-DDD HH24:MI:SS'),5);

                RAISE NOTICE 'OK';

                select max(id_acquisizione) into id_ins
                from   postgis.acquisizioni;

                RAISE NOTICE 'New id: %',id_ins;

                IF EXISTS (SELECT * FROM pg_tables WHERE tablename = 'lst_'||ycount||'_'||mcount) THEN
                    RAISE NOTICE 'Found';

                    EXECUTE format('
                	    insert into postgis.lst (id_acquisizione, rast)
                	    select '||id_ins||', rast
                	    from   %I','lst_'||ycount||'_'||mcount);
                ELSE
                    RAISE NOTICE 'Not Found, Skipping';

                    EXECUTE format('delete from postgis.acquisizioni where id_acquisizione='||id_ins||' ');

                END IF;

                RAISE NOTICE 'OK';

            else
                mcount := gg_start;
            	EXIT;
            end if;

        	mcount := mcount + 8;
        END LOOP;
    else
        EXIT;
    end if;

    ycount := ycount + 1;
 END LOOP;

 RETURN TRUE;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE


-- import NDVI data into DB model
-- INPUT
--   year_start
--   year_end
--   gg_start
--   gg_end
--
-- OUTPUT
--   Boolean : TRUE al is ok, FALSE error
create or replace function postgis.import_ndvi_images(
    year_start integer,
    year_end integer,
    gg_start integer,
    gg_end integer
    )
RETURNS boolean AS
$BODY$
DECLARE
 mcount INT;
 ycount INT;
 id_ins INT;
BEGIN

 ycount := year_start;
 mcount := gg_start;

 LOOP
 	if ycount <= year_end then
    	LOOP
            if (mcount <= gg_end) then
        		RAISE NOTICE 'Inserisco acquisizione : % - %: %', ycount, mcount,''||ycount||'-'||mcount||' 00:00:00';

                insert into postgis.acquisizioni (dtime, id_imgtype) values (to_timestamp(''||ycount||'-'||mcount||' 00:00:00','YYYY-DDD HH24:MI:SS'),2);

                RAISE NOTICE 'OK';

                select max(id_acquisizione) into id_ins
                from   postgis.acquisizioni;

                RAISE NOTICE 'New id: %',id_ins;

                IF EXISTS (SELECT * FROM pg_tables WHERE tablename = 'ndvi_'||ycount||'_'||mcount) THEN
                    RAISE NOTICE 'Found';

                    EXECUTE format('
                	    insert into postgis.ndvi (id_acquisizione, rast)
                	    select '||id_ins||', rast
                	    from   %I','ndvi_'||ycount||'_'||mcount);
                ELSE
                    RAISE NOTICE 'Not Found, Skipping';

                    EXECUTE format('delete from postgis.acquisizioni where id_acquisizione='||id_ins||' ');

                END IF;

                RAISE NOTICE 'OK';

            else
                mcount := gg_start;
            	EXIT;
            end if;

        	mcount := mcount + 16;
        END LOOP;
    else
        EXIT;
    end if;

    ycount := ycount + 1;
 END LOOP;

 RETURN TRUE;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE


-- import EVI data into DB model
-- INPUT
--   year_start
--   year_end
--   gg_start
--   gg_end
--
-- OUTPUT
--   Boolean : TRUE al is ok, FALSE error
create or replace function postgis.import_evi_images(
    year_start integer,
    year_end integer,
    gg_start integer,
    gg_end integer
    )
RETURNS boolean AS
$BODY$
DECLARE
 mcount INT;
 ycount INT;
 id_ins INT;
BEGIN

 ycount := year_start;
 mcount := gg_start;

 LOOP
 	if ycount <= year_end then
    	LOOP
            if (mcount <= gg_end) then
        		RAISE NOTICE 'Inserisco acquisizione : % - %: %', ycount, mcount,''||ycount||'-'||mcount||' 00:00:00';

                insert into postgis.acquisizioni (dtime, id_imgtype) values (to_timestamp(''||ycount||'-'||mcount||' 00:00:00','YYYY-DDD HH24:MI:SS'),3);

                RAISE NOTICE 'OK';

                select max(id_acquisizione) into id_ins
                from   postgis.acquisizioni;

                RAISE NOTICE 'New id: %',id_ins;

                IF EXISTS (SELECT * FROM pg_tables WHERE tablename = 'evi_'||ycount||'_'||mcount) THEN
                    RAISE NOTICE 'Found';

                    EXECUTE format('
                	    insert into postgis.evi (id_acquisizione, rast)
                	    select '||id_ins||', rast
                	    from   %I','evi_'||ycount||'_'||mcount);
                ELSE
                    RAISE NOTICE 'Not Found, Skipping';

                    EXECUTE format('delete from postgis.acquisizioni where id_acquisizione='||id_ins||' ');

                END IF;

                RAISE NOTICE 'OK';

            else
                mcount := gg_start;
            	EXIT;
            end if;

        	mcount := mcount + 16;
        END LOOP;
    else
        EXIT;
    end if;

    ycount := ycount + 1;
 END LOOP;

 RETURN TRUE;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE


-- delete temporary EVI tables
--
-- INPUT
--   year_start
--   year_end
--   gg_start
--   gg_end
--
-- OUTPUT
--   Boolean : TRUE al is ok, FALSE error
﻿create or replace function postgis.clean_temp_evi_tables(
    year_start integer,
    year_end integer,
    gg_start integer,
    gg_end integer
    )
RETURNS boolean AS
$BODY$
DECLARE
 mcount INT;
 ycount INT;
 id_ins INT;
BEGIN

 ycount := year_start;
 mcount := gg_start;

 LOOP
 	if ycount <= year_end then
    	LOOP
            if (mcount <= gg_end) then
        		RAISE NOTICE 'Drop table : % - %: %', ycount, mcount,''||ycount||'-'||mcount||'-01 00:00:00';

                IF EXISTS (SELECT * FROM pg_tables WHERE tablename = 'evi_'||ycount||'_'||mcount) THEN

                	EXECUTE format('
                	drop table %I','evi_'||ycount||'_'||mcount);

                END IF;

                RAISE NOTICE 'OK';

            else
                mcount := 1;
            	EXIT;
            end if;

        	mcount := mcount + 16;
        END LOOP;
    else
        EXIT;
    end if;

    ycount := ycount + 1;
 END LOOP;

 RETURN TRUE;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE

-- delete temporary NDVI tables
--
-- INPUT
--   year_start
--   year_end
--   gg_start
--   gg_end
--
-- OUTPUT
--   Boolean : TRUE al is ok, FALSE error
﻿create or replace function postgis.clean_temp_ndvi_tables(
    year_start integer,
    year_end integer,
    gg_start integer,
    gg_end integer
    )
RETURNS boolean AS
$BODY$
DECLARE
 mcount INT;
 ycount INT;
 id_ins INT;
BEGIN

 ycount := year_start;
 mcount := gg_start;

 LOOP
 	if ycount <= year_end then
    	LOOP
            if (mcount <= gg_end) then
        		RAISE NOTICE 'Drop table : % - %: %', ycount, mcount,''||ycount||'-'||mcount||'-01 00:00:00';

                IF EXISTS (SELECT * FROM pg_tables WHERE tablename = 'ndvi_'||ycount||'_'||mcount) THEN

                	EXECUTE format('
                	drop table %I','ndvi_'||ycount||'_'||mcount);

                END IF;

                RAISE NOTICE 'OK';

            else
                mcount := 1;
            	EXIT;
            end if;

        	mcount := mcount + 16;
        END LOOP;
    else
        EXIT;
    end if;

    ycount := ycount + 1;
 END LOOP;

 RETURN TRUE;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE

-- delete temporary LST tables
--
-- INPUT
--   year_start
--   year_end
--   gg_start
--   gg_end
--
-- OUTPUT
--   Boolean : TRUE al is ok, FALSE error
﻿create or replace function postgis.clean_temp_lst_tables(
    year_start integer,
    year_end integer,
    gg_start integer,
    gg_end integer
    )
RETURNS boolean AS
$BODY$
DECLARE
 mcount INT;
 ycount INT;
 id_ins INT;
BEGIN

 ycount := year_start;
 mcount := gg_start;

 LOOP
 	if ycount <= year_end then
    	LOOP
            if (mcount <= gg_end) then
        		RAISE NOTICE 'Drop table : % - %: %', ycount, mcount,''||ycount||'-'||mcount||'-01 00:00:00';

                IF EXISTS (SELECT * FROM pg_tables WHERE tablename = 'lst_'||ycount||'_'||mcount) THEN

                	EXECUTE format('
                	drop table %I','lst_'||ycount||'_'||mcount);

                END IF;

                RAISE NOTICE 'OK';

            else
                mcount := 1;
            	EXIT;
            end if;

        	mcount := mcount + 8;
        END LOOP;
    else
        EXIT;
    end if;

    ycount := ycount + 1;
 END LOOP;

 RETURN TRUE;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE



  ﻿-- import all rainfall data library from chirps image
create or replace function postgis.import_chirps(
    year_start int
    )
RETURNS boolean AS
$BODY$
DECLARE
   doy_start INT;
   doy_end INT;

   rowrecord RECORD;
BEGIN

 RAISE NOTICE 'Checking last DOY for input year';

 select extract(doy from dtime) into doy_start
 from postgis.acquisizioni
 where extract(year from dtime) = year_start
 and   id_imgtype = 1;

 IF doy_start = null THEN
 	doy_start := 1;
 END IF;

 RAISE NOTICE 'starting from doy: %',doy_start;

 RAISE NOTICE 'Getting number of bands';

 select st_numbands(rast) into doy_end
 from   postgis.rain_temp
 limit 1;

 RAISE NOTICE 'ending doy: %',doy_end;

 FOR i IN doy_start..doy_end LOOP
 	RAISE NOTICE 'processing % image',i;

    INSERT INTO postgis.acquisizioni (dtime, id_imgtype)
    VALUES (to_timestamp(to_char(year_start,'9999')||' '||to_char(i,'999'),'YYYY DDD'),1);

 END LOOP;

 COMMIT;

 RAISE NOTICE 'Importing...';
 FOR rowrecord IN SELECT id_acquisizione, extract(doy from dtime) as doy_in
 	FROM postgis.acquisizioni
    WHERE extract(year from dtime) = year_start
    AND   extract(doy from dtime) BETWEEN doy_start AND doy_end
    AND   id_imgtype = 1
    ORDER BY dtime LOOP

   RAISE NOTICE 'doy: % - %',rowrecord.id_acquisione, rowrecord.doy_in;

   INSERT INTO postgis.precipitazioni (id_acquisizione, rast)
   SELECT rowrecord.id_acquisizione, ST_Band(rast, rowrecord.doy_in)
   FROM rain_temp;

   COMMIT;

 END LOOP;

 DROP TABLE rain_temp;

 RETURN TRUE;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE


