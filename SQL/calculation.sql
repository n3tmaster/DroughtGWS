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

-- calculate spi conditions over region from given country, month and year

CREATE OR REPLACE FUNCTION postgis.calc_spi_stats_over_region(
    month_in integer,
    year_in integer,
    spi_type character varying,
    spi_reclass character varying,
    country_in character varying)
    RETURNS TABLE(region_name_o character varying, hazard_class_o double precision, count_o integer, perc_o double precision)
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$

DECLARE
    spirast RASTER;

    rowrecord RECORD;
    rowrecorda RECORD;

    bandtype varchar := '32BSI';
    dtot bigint;

    m20 integer;
    m15 integer;
    m10 integer;
    m0 integer;
    p10 integer;
    p15 integer;
    p20 integer;

BEGIN
    RAISE NOTICE 'Extract spi raster';

    FOR rowrecorda IN SELECT regione, reg_boundaries.the_geom
                      FROM reg_boundaries INNER JOIN eu_boundaries ON eu_boundaries.gid = reg_boundaries.id_eu_boundaries
                      WHERE lower(name_engl) = country_in
                      ORDER BY regione
        LOOP
            EXECUTE 'SELECT postgis.ST_Reclass(postgis.ST_Union(rast),1,$4,$5) '||
                    'FROM postgis.'||spi_type||' INNER JOIN postgis.acquisizioni USING (id_acquisizione) ' ||
                    'WHERE extract(year from dtime) = $2 ' ||
                    'AND extract(month from dtime) = $3 ' ||
                    'AND postgis.ST_Intersects(rast,$1) '
                USING rowrecorda.the_geom, year_in, month_in, spi_reclass, bandtype INTO spirast;

            spirast := ST_Clip(spirast,rowrecorda.the_geom,ST_BandNoDataValue(spirast),true);

            m20 := postgis.ST_ValueCount(spirast,1,true,-20.0);
            m15 := postgis.ST_ValueCount(spirast,1,true,-15.0);
            m10 := postgis.ST_ValueCount(spirast,1,true,-10.0);
            m0 := postgis.ST_ValueCount(spirast,1,true,0.0);
            p10 := postgis.ST_ValueCount(spirast,1,true,10.0);
            p15 := postgis.ST_ValueCount(spirast,1,true,15.0);
            p20 := postgis.ST_ValueCount(spirast,1,true,20.0);
            dtot := m20 + m15 + m10 + m0 + p10 + p15 + p20;

            RAISE NOTICE '% - % % % % % % % - %',rowrecorda.regione,m20,m15,m10,m0,p10,p15,p20,dtot;

            region_name_o := rowrecorda.regione;
            hazard_class_o := -20.0;
            count_o := m20;
            perc_o := ROUND((m20 * 100.0)/dtot, 2);

            RETURN NEXT;

            region_name_o := rowrecorda.regione;
            hazard_class_o := -15.0;
            count_o := m15;
            perc_o := ROUND((m15 * 100.0)/dtot, 2);

            RETURN NEXT;

            region_name_o := rowrecorda.regione;
            hazard_class_o := -10.0;
            count_o := m10;
            perc_o := ROUND((m10 * 100.0)/dtot, 2);

            RETURN NEXT;

            region_name_o := rowrecorda.regione;
            hazard_class_o := 0.0;
            count_o := m0;
            perc_o := ROUND((m0 * 100.0)/dtot, 2);

            RETURN NEXT;

            region_name_o := rowrecorda.regione;
            hazard_class_o := 10.0;
            count_o := p10;
            perc_o := ROUND((p10 * 100.0)/dtot, 2);

            RETURN NEXT;

            region_name_o := rowrecorda.regione;
            hazard_class_o := 15.0;
            count_o := p15;
            perc_o := ROUND((p15 * 100.0)/dtot, 2);

            RETURN NEXT;

            region_name_o := rowrecorda.regione;
            hazard_class_o := 20.0;
            count_o := p20;
            perc_o := ROUND((p20 * 100.0)/dtot, 2);

            RETURN NEXT;

        END LOOP;

END;
$BODY$;

CREATE OR REPLACE FUNCTION postgis.calc_spi_stats_over_region4shp(
    month_in integer,
    year_in integer,
    spi_type character varying,
    spi_reclass character varying,
    country_in character varying)
    RETURNS TABLE(the_geom_o geometry, region_name_o character varying, hazard_class_o double precision, count_o integer, perc_o double precision)
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$

DECLARE
    spirast RASTER;

    rowrecord RECORD;
    rowrecorda RECORD;

    bandtype varchar := '32BSI';
    dtot bigint;

    m20 integer;
    m15 integer;
    m10 integer;
    m0 integer;
    p10 integer;
    p15 integer;
    p20 integer;

BEGIN
    RAISE NOTICE 'Extract spi raster';

    FOR rowrecorda IN SELECT regione, reg_boundaries.the_geom as the_geom FROM postgis.reg_boundaries
                                                                                   INNER JOIN postgis.eu_boundaries ON eu_boundaries.gid = reg_boundaries.id_eu_boundaries
                      WHERE lower(name_engl) = country_in
                      ORDER BY regione
        LOOP
            EXECUTE 'SELECT postgis.ST_Reclass(postgis.ST_Union(rast),1,$4,$5) '||
                    'FROM postgis.'||spi_type||' INNER JOIN postgis.acquisizioni USING (id_acquisizione) ' ||
                    'WHERE extract(year from dtime) = $2 ' ||
                    'AND extract(month from dtime) = $3 ' ||
                    'AND postgis.ST_Intersects(rast,$1) '
                USING rowrecorda.the_geom, year_in, month_in, spi_reclass, bandtype INTO spirast;

            spirast := ST_Clip(spirast,rowrecorda.the_geom,ST_BandNoDataValue(spirast),true);

            m20 := postgis.ST_ValueCount(spirast,1,true,-20.0);
            m15 := postgis.ST_ValueCount(spirast,1,true,-15.0);
            m10 := postgis.ST_ValueCount(spirast,1,true,-10.0);
            m0 := postgis.ST_ValueCount(spirast,1,true,0.0);
            p10 := postgis.ST_ValueCount(spirast,1,true,10.0);
            p15 := postgis.ST_ValueCount(spirast,1,true,15.0);
            p20 := postgis.ST_ValueCount(spirast,1,true,20.0);
            dtot := m20 + m15 + m10 + m0 + p10 + p15 + p20;

            RAISE NOTICE '% - % % % % % % % - %',rowrecorda.regione,m20,m15,m10,m0,p10,p15,p20,dtot;

            the_geom_o := rowrecorda.the_geom;
            region_name_o := rowrecorda.regione;
            hazard_class_o := -20.0;
            count_o := m20;
            perc_o := ROUND((m20 * 100.0)/dtot, 2);

            RETURN NEXT;

            the_geom_o := rowrecorda.the_geom;
            region_name_o := rowrecorda.regione;
            hazard_class_o := -15.0;
            count_o := m15;
            perc_o := ROUND((m15 * 100.0)/dtot, 2);

            RETURN NEXT;

            the_geom_o := rowrecorda.the_geom;
            region_name_o := rowrecorda.regione;
            hazard_class_o := -10.0;
            count_o := m10;
            perc_o := ROUND((m10 * 100.0)/dtot, 2);

            RETURN NEXT;

            the_geom_o := rowrecorda.the_geom;
            region_name_o := rowrecorda.regione;
            hazard_class_o := 0.0;
            count_o := m0;
            perc_o := ROUND((m0 * 100.0)/dtot, 2);

            RETURN NEXT;

            the_geom_o := rowrecorda.the_geom;
            region_name_o := rowrecorda.regione;
            hazard_class_o := 10.0;
            count_o := p10;
            perc_o := ROUND((p10 * 100.0)/dtot, 2);

            RETURN NEXT;

            the_geom_o := rowrecorda.the_geom;
            region_name_o := rowrecorda.regione;
            hazard_class_o := 15.0;
            count_o := p15;
            perc_o := ROUND((p15 * 100.0)/dtot, 2);

            RETURN NEXT;

            the_geom_o := rowrecorda.the_geom;
            region_name_o := rowrecorda.regione;
            hazard_class_o := 20.0;
            count_o := p20;
            perc_o := ROUND((p20 * 100.0)/dtot, 2);

            RETURN NEXT;

        END LOOP;

END;
$BODY$;


CREATE OR REPLACE FUNCTION postgis.calc_spi_stats_over_eu(
    month_in integer,
    year_in integer,
    spi_type character varying,
    spi_reclass character varying)
    RETURNS TABLE(region_name_o character varying, hazard_class_o double precision, count_o integer, perc_o double precision)
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$

DECLARE
    spirast RASTER;

    rowrecord RECORD;
    rowrecorda RECORD;

    bandtype varchar := '32BSI';
    dtot bigint;

    m20 integer;
    m15 integer;
    m10 integer;
    m0 integer;
    p10 integer;
    p15 integer;
    p20 integer;

BEGIN
    RAISE NOTICE 'Extract spi raster';

    FOR rowrecorda IN SELECT name_engl as regione, the_geom FROM postgis.eu_boundaries ORDER BY name_engl
        LOOP
            EXECUTE 'SELECT postgis.ST_Reclass(postgis.ST_Union(rast),1,$4,$5) '||
                    'FROM postgis.'||spi_type||' INNER JOIN postgis.acquisizioni USING (id_acquisizione) ' ||
                    'WHERE extract(year from dtime) = $2 ' ||
                    'AND extract(month from dtime) = $3 ' ||
                    'AND postgis.ST_Intersects(rast,$1) '
                USING rowrecorda.the_geom, year_in, month_in, spi_reclass, bandtype INTO spirast;

            spirast := ST_Clip(spirast,rowrecorda.the_geom,ST_BandNoDataValue(spirast),true);

            m20 := postgis.ST_ValueCount(spirast,1,true,-20.0);
            m15 := postgis.ST_ValueCount(spirast,1,true,-15.0);
            m10 := postgis.ST_ValueCount(spirast,1,true,-10.0);
            m0 := postgis.ST_ValueCount(spirast,1,true,0.0);
            p10 := postgis.ST_ValueCount(spirast,1,true,10.0);
            p15 := postgis.ST_ValueCount(spirast,1,true,15.0);
            p20 := postgis.ST_ValueCount(spirast,1,true,20.0);
            dtot := m20 + m15 + m10 + m0 + p10 + p15 + p20;

            RAISE NOTICE '% - % % % % % % % - %',rowrecorda.regione,m20,m15,m10,m0,p10,p15,p20,dtot;

            region_name_o := rowrecorda.regione;
            hazard_class_o := -20.0;
            count_o := m20;
            perc_o := ROUND((m20 * 100.0)/dtot, 2);

            RETURN NEXT;

            region_name_o := rowrecorda.regione;
            hazard_class_o := -15.0;
            count_o := m15;
            perc_o := ROUND((m15 * 100.0)/dtot, 2);

            RETURN NEXT;

            region_name_o := rowrecorda.regione;
            hazard_class_o := -10.0;
            count_o := m10;
            perc_o := ROUND((m10 * 100.0)/dtot, 2);

            RETURN NEXT;

            region_name_o := rowrecorda.regione;
            hazard_class_o := 0.0;
            count_o := m0;
            perc_o := ROUND((m0 * 100.0)/dtot, 2);

            RETURN NEXT;

            region_name_o := rowrecorda.regione;
            hazard_class_o := 10.0;
            count_o := p10;
            perc_o := ROUND((p10 * 100.0)/dtot, 2);

            RETURN NEXT;

            region_name_o := rowrecorda.regione;
            hazard_class_o := 15.0;
            count_o := p15;
            perc_o := ROUND((p15 * 100.0)/dtot, 2);

            RETURN NEXT;

            region_name_o := rowrecorda.regione;
            hazard_class_o := 20.0;
            count_o := p20;
            perc_o := ROUND((p20 * 100.0)/dtot, 2);

            RETURN NEXT;

        END LOOP;

END;
$BODY$;


--calculate spi conditions over eu countries for given time interval
CREATE OR REPLACE FUNCTION postgis.calc_spi_stats_over_eu(
    month_in integer,
    year_in integer,
    month2_in integer,
    year2_in integer,
    spi_type character varying,
    spi_reclass character varying,
    country_in character varying)
    RETURNS TABLE(province_name_o character varying, year_o integer, month_o integer, hazard_class_o double precision, count_o integer, perc_o double precision)
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$

DECLARE
    spirast RASTER;

    rowrecord RECORD;
    rowrecorda RECORD;

    bandtype varchar := '32BSI';
    dtot bigint;

    myear integer;
    mmonth integer;
    m20 integer;
    m15 integer;
    m10 integer;
    m0 integer;
    p10 integer;
    p15 integer;
    p20 integer;
    dtime1 timestamp;
    dtime2 timestamp;

BEGIN

    dtime1 := to_timestamp(''||year_in||' '||month_in||' 01','YYYY MM DD');
    dtime2 := to_timestamp(''||year2_in||' '||(month2_in)||' 01','YYYY MM DD');

    RAISE NOTICE 'Extract spi raster from % to %',dtime1, dtime2;

    FOR rowrecorda IN SELECT name_engl as den_uts, the_geom FROM postgis.eu_boundaries WHERE LOWER(name_engl) = country_in
        LOOP
            FOR rowrecord IN EXECUTE 'SELECT extract(year from dtime) as yo, extract(month from dtime) as mo, '||
                                     'postgis.ST_Reclass(postgis.ST_Union(rast),1,$4,$5) as ro '||
                                     'FROM postgis.'||spi_type||' INNER JOIN postgis.acquisizioni USING (id_acquisizione) ' ||
                                     'WHERE dtime BETWEEN $2 AND $3 ' ||
                                     'AND postgis.ST_Intersects(rast,$1) '||
                                     'GROUP BY 1,2 '||
                                     'ORDER BY 1,2'
                USING rowrecorda.the_geom, dtime1, dtime2, spi_reclass, bandtype LOOP

                    spirast := ST_Clip(rowrecord.ro,rowrecorda.the_geom,ST_BandNoDataValue(rowrecord.ro),true);

                    m20 := postgis.ST_ValueCount(spirast,1,true,-20.0);
                    m15 := postgis.ST_ValueCount(spirast,1,true,-15.0);
                    m10 := postgis.ST_ValueCount(spirast,1,true,-10.0);
                    m0 := postgis.ST_ValueCount(spirast,1,true,0.0);
                    p10 := postgis.ST_ValueCount(spirast,1,true,10.0);
                    p15 := postgis.ST_ValueCount(spirast,1,true,15.0);
                    p20 := postgis.ST_ValueCount(spirast,1,true,20.0);
                    dtot := m20 + m15 + m10 + m0 + p10 + p15 + p20;

                    RAISE NOTICE '% - % % - % % % % % % % - %',rowrecorda.den_uts,rowrecord.yo,rowrecord.mo,m20,m15,m10,m0,p10,p15,p20,dtot;

                    province_name_o := rowrecorda.den_uts;
                    year_o := rowrecord.yo;
                    month_o := rowrecord.mo;
                    hazard_class_o := -20.0;
                    count_o := m20;
                    perc_o := ROUND((m20 * 100.0)/dtot, 2);

                    RETURN NEXT;

                    province_name_o := rowrecorda.den_uts;
                    year_o := rowrecord.yo;
                    month_o := rowrecord.mo;
                    hazard_class_o := -15.0;
                    count_o := m15;
                    perc_o := ROUND((m15 * 100.0)/dtot, 2);

                    RETURN NEXT;

                    province_name_o := rowrecorda.den_uts;
                    year_o := rowrecord.yo;
                    month_o := rowrecord.mo;
                    hazard_class_o := -10.0;
                    count_o := m10;
                    perc_o := ROUND((m10 * 100.0)/dtot, 2);

                    RETURN NEXT;

                    province_name_o := rowrecorda.den_uts;
                    year_o := rowrecord.yo;
                    month_o := rowrecord.mo;
                    hazard_class_o := 0.0;
                    count_o := m0;
                    perc_o := ROUND((m0 * 100.0)/dtot, 2);

                    RETURN NEXT;

                    province_name_o := rowrecorda.den_uts;
                    year_o := rowrecord.yo;
                    month_o := rowrecord.mo;
                    hazard_class_o := 10.0;
                    count_o := p10;
                    perc_o := ROUND((p10 * 100.0)/dtot, 2);

                    RETURN NEXT;

                    province_name_o := rowrecorda.den_uts;
                    year_o := rowrecord.yo;
                    month_o := rowrecord.mo;
                    hazard_class_o := 15.0;
                    count_o := p15;
                    perc_o := ROUND((p15 * 100.0)/dtot, 2);

                    RETURN NEXT;

                    province_name_o := rowrecorda.den_uts;
                    year_o := rowrecord.yo;
                    month_o := rowrecord.mo;
                    hazard_class_o := 20.0;
                    count_o := p20;
                    perc_o := ROUND((p20 * 100.0)/dtot, 2);

                    RETURN NEXT;


                END LOOP;

        END LOOP;

END;
$BODY$;

-- calculate spi conditions over regions from given country and time interval
CREATE OR REPLACE FUNCTION postgis.calc_spi_stats_over_region(
    month_in integer,
    year_in integer,
    month2_in integer,
    year2_in integer,
    spi_type character varying,
    spi_reclass character varying,
    country_in character varying,
    region_in character varying)
    RETURNS TABLE(region_name_o character varying, year_o integer, month_o integer, hazard_class_o double precision, count_o integer, perc_o double precision)
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$

DECLARE
    spirast RASTER;

    rowrecord RECORD;
    rowrecorda RECORD;

    bandtype varchar := '32BSI';
    dtot bigint;


    m20 integer;
    m15 integer;
    m10 integer;
    m0 integer;
    p10 integer;
    p15 integer;
    p20 integer;
    dtime1 timestamp;
    dtime2 timestamp;

BEGIN

    dtime1 := to_timestamp(''||year_in||' '||month_in||' 01','YYYY MM DD');
    dtime2 := to_timestamp(''||year2_in||' '||(month2_in)||' 01','YYYY MM DD');

    RAISE NOTICE 'Extract spi raster from % to %',dtime1, dtime2;

    FOR rowrecorda IN SELECT regione, reg_boundaries.the_geom
                      FROM reg_boundaries INNER JOIN eu_boundaries ON eu_boundaries.gid = reg_boundaries.id_eu_boundaries
                      WHERE lower(name_engl) = country_in AND lower(regione) = region_in
                      ORDER BY regione
        LOOP
            FOR rowrecord IN EXECUTE 'SELECT extract(year from dtime) as yo, extract(month from dtime) as mo, '||
                                     'postgis.ST_Reclass(postgis.ST_Union(rast),1,$4,$5) as ro '||
                                     'FROM postgis.'||spi_type||' INNER JOIN postgis.acquisizioni USING (id_acquisizione) ' ||
                                     'WHERE dtime BETWEEN $2 AND $3 ' ||
                                     'AND postgis.ST_Intersects(rast,$1) '||
                                     'GROUP BY 1,2 '||
                                     'ORDER BY 1,2'
                USING rowrecorda.the_geom, dtime1, dtime2, spi_reclass, bandtype LOOP

                    spirast := ST_Clip(rowrecord.ro,rowrecorda.the_geom,ST_BandNoDataValue(rowrecord.ro),true);

                    m20 := postgis.ST_ValueCount(spirast,1,true,-20.0);
                    m15 := postgis.ST_ValueCount(spirast,1,true,-15.0);
                    m10 := postgis.ST_ValueCount(spirast,1,true,-10.0);
                    m0 := postgis.ST_ValueCount(spirast,1,true,0.0);
                    p10 := postgis.ST_ValueCount(spirast,1,true,10.0);
                    p15 := postgis.ST_ValueCount(spirast,1,true,15.0);
                    p20 := postgis.ST_ValueCount(spirast,1,true,20.0);
                    dtot := m20 + m15 + m10 + m0 + p10 + p15 + p20;

                    RAISE NOTICE '% - % % - % % % % % % % - %',rowrecorda.regione,rowrecord.yo,rowrecord.mo,m20,m15,m10,m0,p10,p15,p20,dtot;

                    region_name_o := rowrecorda.regione;
                    year_o := rowrecord.yo;
                    month_o := rowrecord.mo;
                    hazard_class_o := -20.0;
                    count_o := m20;
                    perc_o := ROUND((m20 * 100.0)/dtot, 2);

                    RETURN NEXT;

                    region_name_o := rowrecorda.regione;
                    year_o := rowrecord.yo;
                    month_o := rowrecord.mo;
                    hazard_class_o := -15.0;
                    count_o := m15;
                    perc_o := ROUND((m15 * 100.0)/dtot, 2);

                    RETURN NEXT;

                    region_name_o := rowrecorda.regione;
                    year_o := rowrecord.yo;
                    month_o := rowrecord.mo;
                    hazard_class_o := -10.0;
                    count_o := m10;
                    perc_o := ROUND((m10 * 100.0)/dtot, 2);

                    RETURN NEXT;

                    region_name_o := rowrecorda.regione;
                    year_o := rowrecord.yo;
                    month_o := rowrecord.mo;
                    hazard_class_o := 0.0;
                    count_o := m0;
                    perc_o := ROUND((m0 * 100.0)/dtot, 2);

                    RETURN NEXT;

                    region_name_o := rowrecorda.regione;
                    year_o := rowrecord.yo;
                    month_o := rowrecord.mo;
                    hazard_class_o := 10.0;
                    count_o := p10;
                    perc_o := ROUND((p10 * 100.0)/dtot, 2);

                    RETURN NEXT;

                    region_name_o := rowrecorda.regione;
                    year_o := rowrecord.yo;
                    month_o := rowrecord.mo;
                    hazard_class_o := 15.0;
                    count_o := p15;
                    perc_o := ROUND((p15 * 100.0)/dtot, 2);

                    RETURN NEXT;

                    region_name_o := rowrecorda.regione;
                    year_o := rowrecord.yo;
                    month_o := rowrecord.mo;
                    hazard_class_o := 20.0;
                    count_o := p20;
                    perc_o := ROUND((p20 * 100.0)/dtot, 2);

                    RETURN NEXT;


                END LOOP;

        END LOOP;

END;
$BODY$;


-- calculate spi stats over provinces with given period and region
CREATE OR REPLACE FUNCTION postgis.calc_spi_stats_over_prov(
    month_in integer,
    year_in integer,
    month2_in integer,
    year2_in integer,
    spi_type character varying,
    region_in character varying,
    spi_reclass character varying)
    RETURNS TABLE(province_name_o character varying, year_o integer, month_o integer, hazard_class_o double precision, count_o integer, perc_o double precision)
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$

DECLARE
    spirast RASTER;

    rowrecord RECORD;
    rowrecorda RECORD;

    bandtype varchar := '32BSI';
    dtot bigint;

    myear integer;
    mmonth integer;
    m20 integer;
    m15 integer;
    m10 integer;
    m0 integer;
    p10 integer;
    p15 integer;
    p20 integer;
    dtime1 timestamp;
    dtime2 timestamp;

BEGIN

    dtime1 := to_timestamp(''||year_in||' '||month_in||' 01','YYYY MM DD');
    dtime2 := to_timestamp(''||year2_in||' '||(month2_in)||' 01','YYYY MM DD');

    RAISE NOTICE 'Extract spi raster from % to %',dtime1, dtime2;

    FOR rowrecorda IN SELECT den_uts, the_geom FROM postgis.provinces WHERE LOWER(regione) = region_in ORDER BY den_uts
        LOOP
            FOR rowrecord IN EXECUTE 'SELECT extract(year from dtime) as yo, extract(month from dtime) as mo, '||
                                     'postgis.ST_Reclass(postgis.ST_Union(rast),1,$4,$5) as ro '||
                                     'FROM postgis.'||spi_type||' INNER JOIN postgis.acquisizioni USING (id_acquisizione) ' ||
                                     'WHERE dtime BETWEEN $2 AND $3 ' ||
                                     'AND postgis.ST_Intersects(rast,$1) '||
                                     'GROUP BY 1,2 '||
                                     'ORDER BY 1,2'
                USING rowrecorda.the_geom, dtime1, dtime2, spi_reclass, bandtype LOOP

                    spirast := ST_Clip(rowrecord.ro,rowrecorda.the_geom,ST_BandNoDataValue(rowrecord.ro),true);

                    m20 := postgis.ST_ValueCount(spirast,1,true,-20.0);
                    m15 := postgis.ST_ValueCount(spirast,1,true,-15.0);
                    m10 := postgis.ST_ValueCount(spirast,1,true,-10.0);
                    m0 := postgis.ST_ValueCount(spirast,1,true,0.0);
                    p10 := postgis.ST_ValueCount(spirast,1,true,10.0);
                    p15 := postgis.ST_ValueCount(spirast,1,true,15.0);
                    p20 := postgis.ST_ValueCount(spirast,1,true,20.0);
                    dtot := m20 + m15 + m10 + m0 + p10 + p15 + p20;

                    RAISE NOTICE '% - % % - % % % % % % % - %',rowrecorda.den_uts,rowrecord.yo,rowrecord.mo,m20,m15,m10,m0,p10,p15,p20,dtot;

                    province_name_o := rowrecorda.den_uts;
                    year_o := rowrecord.yo;
                    month_o := rowrecord.mo;
                    hazard_class_o := -20.0;
                    count_o := m20;
                    perc_o := ROUND((m20 * 100.0)/dtot, 2);

                    RETURN NEXT;

                    province_name_o := rowrecorda.den_uts;
                    year_o := rowrecord.yo;
                    month_o := rowrecord.mo;
                    hazard_class_o := -15.0;
                    count_o := m15;
                    perc_o := ROUND((m15 * 100.0)/dtot, 2);

                    RETURN NEXT;

                    province_name_o := rowrecorda.den_uts;
                    year_o := rowrecord.yo;
                    month_o := rowrecord.mo;
                    hazard_class_o := -10.0;
                    count_o := m10;
                    perc_o := ROUND((m10 * 100.0)/dtot, 2);

                    RETURN NEXT;

                    province_name_o := rowrecorda.den_uts;
                    year_o := rowrecord.yo;
                    month_o := rowrecord.mo;
                    hazard_class_o := 0.0;
                    count_o := m0;
                    perc_o := ROUND((m0 * 100.0)/dtot, 2);

                    RETURN NEXT;

                    province_name_o := rowrecorda.den_uts;
                    year_o := rowrecord.yo;
                    month_o := rowrecord.mo;
                    hazard_class_o := 10.0;
                    count_o := p10;
                    perc_o := ROUND((p10 * 100.0)/dtot, 2);

                    RETURN NEXT;

                    province_name_o := rowrecorda.den_uts;
                    year_o := rowrecord.yo;
                    month_o := rowrecord.mo;
                    hazard_class_o := 15.0;
                    count_o := p15;
                    perc_o := ROUND((p15 * 100.0)/dtot, 2);

                    RETURN NEXT;

                    province_name_o := rowrecorda.den_uts;
                    year_o := rowrecord.yo;
                    month_o := rowrecord.mo;
                    hazard_class_o := 20.0;
                    count_o := p20;
                    perc_o := ROUND((p20 * 100.0)/dtot, 2);

                    RETURN NEXT;


                END LOOP;

        END LOOP;

END;
$BODY$;


-- calculate spi stats over province for given month and year
CREATE OR REPLACE FUNCTION postgis.calc_spi_stats_over_prov(
    month_in integer,
    year_in integer,
    spi_type character varying,
    region_in character varying,
    spi_reclass character varying)
    RETURNS TABLE(province_name_o character varying, hazard_class_o double precision, count_o integer, perc_o double precision)
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$

DECLARE
    spirast RASTER;

    rowrecord RECORD;
    rowrecorda RECORD;

    bandtype varchar := '32BSI';
    dtot bigint;

    m20 integer;
    m15 integer;
    m10 integer;
    m0 integer;
    p10 integer;
    p15 integer;
    p20 integer;

BEGIN
    RAISE NOTICE 'Extract spi raster';

    FOR rowrecorda IN SELECT den_uts, the_geom FROM postgis.provinces WHERE LOWER(regione) = region_in ORDER BY den_uts
        LOOP
            EXECUTE 'SELECT postgis.ST_Reclass(postgis.ST_Union(rast),1,$4,$5) '||
                    'FROM postgis.'||spi_type||' INNER JOIN postgis.acquisizioni USING (id_acquisizione) ' ||
                    'WHERE extract(year from dtime) = $2 ' ||
                    'AND extract(month from dtime) = $3 ' ||
                    'AND postgis.ST_Intersects(rast,$1) '
                USING rowrecorda.the_geom, year_in, month_in, spi_reclass, bandtype INTO spirast;

            spirast := ST_Clip(spirast,rowrecorda.the_geom,ST_BandNoDataValue(spirast),true);

            m20 := postgis.ST_ValueCount(spirast,1,true,-20.0);
            m15 := postgis.ST_ValueCount(spirast,1,true,-15.0);
            m10 := postgis.ST_ValueCount(spirast,1,true,-10.0);
            m0 := postgis.ST_ValueCount(spirast,1,true,0.0);
            p10 := postgis.ST_ValueCount(spirast,1,true,10.0);
            p15 := postgis.ST_ValueCount(spirast,1,true,15.0);
            p20 := postgis.ST_ValueCount(spirast,1,true,20.0);
            dtot := m20 + m15 + m10 + m0 + p10 + p15 + p20;

            RAISE NOTICE '% - % % % % % % % - %',rowrecorda.den_uts,m20,m15,m10,m0,p10,p15,p20,dtot;

            province_name_o := rowrecorda.den_uts;
            hazard_class_o := -20.0;
            count_o := m20;
            perc_o := ROUND((m20 * 100.0)/dtot, 2);

            RETURN NEXT;

            province_name_o := rowrecorda.den_uts;
            hazard_class_o := -15.0;
            count_o := m15;
            perc_o := ROUND((m15 * 100.0)/dtot, 2);

            RETURN NEXT;

            province_name_o := rowrecorda.den_uts;
            hazard_class_o := -10.0;
            count_o := m10;
            perc_o := ROUND((m10 * 100.0)/dtot, 2);

            RETURN NEXT;

            province_name_o := rowrecorda.den_uts;
            hazard_class_o := 0.0;
            count_o := m0;
            perc_o := ROUND((m0 * 100.0)/dtot, 2);

            RETURN NEXT;

            province_name_o := rowrecorda.den_uts;
            hazard_class_o := 10.0;
            count_o := p10;
            perc_o := ROUND((p10 * 100.0)/dtot, 2);

            RETURN NEXT;

            province_name_o := rowrecorda.den_uts;
            hazard_class_o := 15.0;
            count_o := p15;
            perc_o := ROUND((p15 * 100.0)/dtot, 2);

            RETURN NEXT;

            province_name_o := rowrecorda.den_uts;
            hazard_class_o := 20.0;
            count_o := p20;
            perc_o := ROUND((p20 * 100.0)/dtot, 2);

            RETURN NEXT;

        END LOOP;

END;
$BODY$;

--calculate spi stats from region

CREATE OR REPLACE FUNCTION postgis.calc_spi_stats_from_region(
    month_in integer,
    year_in integer,
    poly_in geometry,
    spi_type varchar,
    spi_reclass varchar
)
    RETURNS TABLE (hazard_class_o double precision, count_o integer, perc_o double precision)
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE
AS $BODY$

DECLARE
    spirast RASTER;

    rowrecord RECORD;

    bandtype varchar := '32BSI';
    dstat summarystats;
BEGIN
    RAISE NOTICE 'Extract spi raster';



    EXECUTE 'SELECT postgis.ST_Reclass(postgis.st_clip(postgis.ST_Union(rast),$1,true),1,$4,$5) '||
            'FROM postgis.'||spi_type||' INNER JOIN postgis.acquisizioni USING (id_acquisizione) ' ||
            'WHERE extract(year from dtime) = $2 ' ||
            'AND extract(month from dtime) = $3 ' ||
            'AND postgis.ST_Intersects(rast,$1) '
        USING poly_in, year_in, month_in, spi_reclass, bandtype INTO spirast;

    dstat := postgis.st_summarystatsagg(spirast,1,true);
    RAISE NOTICE 'number of pixels: %',dstat.count;

    FOR rowrecord IN SELECT (pvc).value as val, (pvc).count as c
                     FROM (SELECT postgis.ST_ValueCount(spirast) as pvc) as foo
        LOOP
            hazard_class_o := rowrecord.val;
            count_o := rowrecord.c;
            perc_o := (rowrecord.c * 100.0)/dstat.count;

            RETURN NEXT;

        end loop;


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





-- Generic extraction procedure
-- INPUT
--    image type
--    dtime
--    polygon
CREATE OR REPLACE FUNCTION postgis.extract_image(
	imgtype_in varchar,
	dtime_in timestamp,
	polygon_in geometry)
    RETURNS raster
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE
AS $BODY$

DECLARE
 rast_out RASTER;

BEGIN
 RAISE NOTICE 'Extracting image';

 RAISE NOTICE 'Calculating cum raster';

  EXECUTE 'SELECT ST_Clip(ST_Union(rast),$1,true) ' ||
        'FROM postgis.'||imgtype_in||' INNER JOIN postgis.acquisizioni USING (id_acquisizione) '||
        'WHERE dtime = $2 '||
        'AND   ST_Intersects(rast,$1) '
  INTO rast_out USING polygon_in,dtime_in;

 RAISE NOTICE 'done.';
 RETURN rast_out;
END;

$BODY$;


--calculate last element and fill from box
CREATE OR REPLACE FUNCTION postgis.calculate_last_element_and_fill_from_box(
    imgtype_in character varying,
    tbl_name character varying,
    rast_field character varying,
    step integer,
    gap integer,
    box_in geometry)
    RETURNS TABLE(odoy_next integer, oday_next integer, omonth_next integer, oyear_next integer)
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
DECLARE
    max_dtime timestamp;
    stop_dtime timestamp;
    n_days integer;
    i integer;
BEGIN
    -- check the existence of one or more doy for given year

    EXECUTE 'SELECT max(dtime), extract(day from ((current_timestamp - interval '''||gap||''' day)- max(dtime)))
			FROM postgis.acquisizioni INNER JOIN postgis.imgtypes USING (id_imgtype)
			WHERE imgtype = $1
			AND   id_acquisizione IN (SELECT id_acquisizione
									  FROM postgis.'||tbl_name||'
									  WHERE ST_Intersects(wind,$2))'
        INTO   max_dtime, n_days
        USING  imgtype_in, box_in;

    RAISE NOTICE  'Max Dtime: %',max_dtime;
    RAISE NOTICE  'n days to present: %',n_days;
    IF n_days > 0 THEN
        FOR i IN 1..n_days LOOP
                EXECUTE 'SELECT $1 + interval '''||step||''' day'
                    INTO max_dtime
                    USING max_dtime;

                RAISE NOTICE  '% dtime : %', i, max_dtime;

                odoy_next := extract(doy from max_dtime);
                oday_next := extract(day from max_dtime);
                omonth_next := extract(month from max_dtime);
                oyear_next := extract(year from max_dtime);

                RETURN NEXT;
            END LOOP;

    ELSE
        RAISE NOTICE 'no data';
    END IF;
END;
$BODY$;


-- extract spi values as pixel polygon collection
CREATE OR REPLACE FUNCTION postgis.spi_to_vector(

    spi_type_in varchar,
    bounds_in geometry,
    dtime1 timestamp,
    dtime2 timestamp,
    spi_reclass character varying)
    RETURNS TABLE(year_o integer, month_o integer, the_geom_o geometry,
                  val_o double precision)
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
DECLARE

    rowrecord_spi RECORD;
    rowrecord RECORD;

    bandtype varchar := '32BSI';
--
BEGIN

    RAISE NOTICE 'get spi raster';

    FOR rowrecord_spi IN EXECUTE 'SELECT extract(year from dtime) as yo, extract(month from dtime) as mo, '||
                                 'ST_Clip(ST_Reclass(postgis.ST_Union(rast),1,$4,$5),$1,true) as ro '||
                                 'FROM postgis.'||spi_type_in||' INNER JOIN postgis.acquisizioni USING (id_acquisizione) ' ||
                                 'WHERE dtime BETWEEN $2 AND $3 ' ||
                                 'AND postgis.ST_Intersects(rast,$1) '||
                                 'GROUP BY 1,2 '||
                                 'ORDER BY 1,2'
        USING bounds_in, dtime1, dtime2, spi_reclass, bandtype
        LOOP
            RAISE NOTICE 'Elaborating % % - % %',rowrecord_spi.yo, rowrecord_spi.mo,
                st_width(rowrecord_spi.ro), st_height(rowrecord_spi.ro);


            for rowrecord in SELECT ST_Intersection(bounds_in,(foo).gv.geom)  as geom,
                                    (foo).gv.val as val,
                                    (foo).gv.x as pixelx,
                                    (foo).gv.y as pixely
                             FROM (SELECT postgis.ST_PixelAsPolygons(rowrecord_spi.ro,1,true) as gv  ) as foo
                             WHERE  (foo).gv.val between -30 and 30
                loop

                    the_geom_o := rowrecord.geom;
                    val_o := rowrecord.val;
                    year_o := rowrecord_spi.yo;
                    month_o := rowrecord_spi.mo;
                    return next;
                end loop;

        END LOOP;



END;
$BODY$;

CREATE OR REPLACE FUNCTION postgis.calc_spi_stats_over_landcover(
    month_in integer,
    year_in integer,
    month2_in integer,
    year2_in integer,
    spi_type_in character varying,
    bounds_in geometry,
    spi_reclass character varying)
    RETURNS TABLE(year_oo integer, month_oo integer,
                  cci_class_o integer, spi_class_o double precision,
                  n_occurrences integer)
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
DECLARE

    rowrecord_spi RECORD;
    dtime_now timestamp;
    dtime1 timestamp;
    dtime2 timestamp;
BEGIN
    --todo: da togliere quando si creera servizio con concorrenza
    DELETE FROM postgis.spi_cci_temp;
    dtime_now := current_timestamp;
    RAISE NOTICE 'begin procedure at %',dtime_now;
    dtime1 := to_timestamp(''||year_in||' '||month_in||' 01','YYYY MM DD');
    dtime2 := to_timestamp(''||year2_in||' '||(month2_in + 1)||' 01','YYYY MM DD');

    RAISE NOTICE 'run SPI to Vector func and save results';
    INSERT INTO postgis.spi_cci_temp
    (spi_geom, spi_year, spi_month,spi_class,cci_class,dtime)
    select the_geom_o, year_o, month_o, val_o,null,dtime_now
    from postgis.spi_to_vector(
            spi_type_in, bounds_in,
            dtime1,dtime2, spi_reclass);

    RAISE NOTICE 'start cycle for calculate stats';

    FOR rowrecord_spi IN
        SELECT bb.dn,aa.spi_class,aa.spi_month, aa.spi_year,  count(*) as npix
        FROM postgis.spi_cci_temp as aa  join postgis.landcover_cci as bb
                                              ON ST_intersects(aa.spi_geom,bb.the_geom)
        WHERE dtime = dtime_now
        GROUP BY 1,2,3,4
        ORDER BY 4,3,1,2
        LOOP

            cci_class_o := rowrecord_spi.dn;
            spi_class_o := rowrecord_spi.spi_class;
            year_oo := rowrecord_spi.spi_year;
            month_oo := rowrecord_spi.spi_month;
            n_occurrences := rowrecord_spi.npix;

            RETURN NEXT;

        END LOOP;

END;
$BODY$;

-- calculate LST stats over regions
--  INPUT
--   begin month and year
--   end month and year
--   country name
--  RETURNS
--   eu_name , year, month, pixel count, mean, standard deviation, min and max



CREATE OR REPLACE FUNCTION postgis.calc_lst_stats_over_regions(
    month_in integer,
    year_in integer,
    month2_in integer,
    year2_in integer,
    country_in varchar)
    RETURNS TABLE(region_name_o character varying, year_o integer, month_o integer, day_o integer,
                  min_o double precision, max_o double precision, mean_o double precision,
                  stddev_o double precision)
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$

DECLARE
    spirast RASTER;
    spistats summarystats;

    rowrecord RECORD;
    rowrecorda RECORD;

    dtime1 timestamp;
    dtime2 timestamp;

BEGIN
    RAISE NOTICE 'Extract spi raster';

    dtime1 := to_timestamp(''||year_in||' '||month_in||' 01','YYYY MM DD');
    dtime2 := to_timestamp(''||year2_in||' '||(month2_in)||' 01','YYYY MM DD');


    FOR rowrecorda IN SELECT regione, reg_boundaries.the_geom as geom
                      FROM reg_boundaries INNER JOIN eu_boundaries ON eu_boundaries.gid = reg_boundaries.id_eu_boundaries
                      WHERE lower(name_engl) = country_in
                      ORDER BY regione
        LOOP
            FOR rowrecord IN EXECUTE 'SELECT postgis.ST_Union(rast) as ro, extract(year from dtime) as yo,
			extract(month from dtime) as mo, extract(day from dtime) as do '||
                                     'FROM postgis.lst INNER JOIN postgis.acquisizioni USING (id_acquisizione) ' ||
                                     'WHERE dtime between $2 and $3 ' ||
                                     'AND postgis.ST_Intersects(rast,$1) group by 2,3,4 order by 2,3,4'
                USING rowrecorda.geom, dtime1, dtime2 LOOP


                    RAISE NOTICE 'clipping image % % %',rowrecord.yo,rowrecord.mo,rowrecord.do;
                    spirast := ST_Clip(rowrecord.ro,rowrecorda.geom,ST_BandNoDataValue(spirast),true);
                    RAISE NOTICE 'calculate stats image';
                    spistats := ST_SummaryStatsAgg(spirast,1,true);

                    region_name_o:=rowrecorda.regione;
                    year_o := rowrecord.yo;
                    month_o := rowrecord.mo;
                    day_o := rowrecord.do;
                    min_o:=(spistats.min * 0.02) - 273.15;
                    max_o:=(spistats.max * 0.02) - 273.15;
                    mean_o:=(spistats.mean * 0.02) - 273.15;
                    stddev_o:=spistats.stddev;


                    RETURN NEXT;
                END LOOP;
        END LOOP;

END;
$BODY$;

CREATE OR REPLACE FUNCTION postgis.calc_lst_stats_over_eu(
    month_in integer,
    year_in integer,
    month2_in integer,
    year2_in integer)
    RETURNS TABLE(region_name_o character varying, year_o integer, month_o integer, day_o integer, min_o double precision, max_o double precision, mean_o double precision, stddev_o double precision)
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$

DECLARE
    spirast RASTER;
    spistats summarystats;

    rowrecord RECORD;
    rowrecorda RECORD;
    yearo integer;
    montho integer;
    dayo integer;

    dtime1 timestamp;
    dtime2 timestamp;

BEGIN
    RAISE NOTICE 'Extract spi raster';

    dtime1 := to_timestamp(''||year_in||' '||month_in||' 01','YYYY MM DD');
    dtime2 := to_timestamp(''||year2_in||' '||(month2_in)||' 01','YYYY MM DD');

    FOR rowrecorda IN SELECT name_engl as regione, the_geom FROM postgis.eu_boundaries ORDER BY name_engl
        LOOP
            FOR rowrecord IN EXECUTE 'SELECT postgis.ST_Union(rast) as ro, extract(year from dtime) as yo,
			extract(month from dtime) as mo, extract(day from dtime) as do '||
                                     'FROM postgis.lst INNER JOIN postgis.acquisizioni USING (id_acquisizione) ' ||
                                     'WHERE dtime between $2 and $3 ' ||
                                     'AND postgis.ST_Intersects(rast,$1) group by 2,3,4 order by 2,3,4'
                USING rowrecorda.the_geom, dtime1, dtime2 LOOP

                    RAISE NOTICE 'clipping image % % %',rowrecord.yo,rowrecord.mo,rowrecord.do;
                    spirast := ST_Clip(rowrecord.ro,rowrecorda.the_geom,ST_BandNoDataValue(spirast),true);
                    RAISE NOTICE 'calculate stats image';
                    spistats := ST_SummaryStatsAgg(spirast,1,true);

                    region_name_o:=rowrecorda.regione;
                    year_o := rowrecord.yo;
                    month_o := rowrecord.mo;
                    day_o := rowrecord.do;
                    min_o:=(spistats.min * 0.02) - 273.15;
                    max_o:=(spistats.max * 0.02) - 273.15;
                    stddev_o:=spistats.stddev;
                    mean_o:=(spistats.mean * 0.02) - 273.15;


                    RETURN NEXT;
                END LOOP;
        END LOOP;

END;
$BODY$;