-- Create VHI image from VCI and TCI
-- Input : dtime_in - timestamp - reference date for VHI calculation
-- Output : VHI raster
create or replace function postgis.calculate_vhi(
    dtime_in timestamp, store boolean
    )
RETURNS RASTER AS
$BODY$
DECLARE


 tci RASTER;
 tci2 RASTER;
 vci RASTER;
 vhi RASTER;

 imgtype_in INT;
 tcitype_in INT;
 vcitype_in INT;
 id_acquisizione_in INT;

 doy_in INT := extract('doy' from dtime_in);
 year_in INT := extract('year' from dtime_in);



BEGIN
 RAISE NOTICE 'Calculating VHI raster for doy: % and year: %',doy_in,year_in;



 select id_imgtype into imgtype_in
 from   postgis.imgtypes
 where  imgtype = 'VHI';

 select id_imgtype into tcitype_in
 from   postgis.imgtypes
 where  imgtype = 'TCI';

 select id_imgtype into vcitype_in
 from   postgis.imgtypes
 where  imgtype = 'VCI';

 RAISE NOTICE 'ImgType : %',imgtype_in;

 RAISE NOTICE 'Get VCI';
 select b.rast into vci
 from postgis.acquisizioni a inner join postgis.vci b using (id_acquisizione)
 where  extract('year' from a.dtime) = year_in
 and   extract('doy' from a.dtime) = doy_in
 and   a.id_imgtype = vcitype_in;

 RAISE NOTICE 'Get TCI';
 select b.rast into tci
 from postgis.acquisizioni a inner join postgis.tci b using (id_acquisizione)
 where  extract('year' from a.dtime) = year_in
 and   extract('doy' from a.dtime) = doy_in
 and   a.id_imgtype = tcitype_in;

 RAISE NOTICE 'Get TCI_2';
 select b.rast into tci2
 from postgis.acquisizioni a inner join postgis.tci b using (id_acquisizione)
 where  extract('year' from a.dtime) = year_in
 and   extract('doy' from a.dtime) = (doy_in + 8)
 and   a.id_imgtype = tcitype_in;


 vhi := st_addband(st_makeemptyraster(st_band(vci,1)),'32BF'::text);


 RAISE NOTICE 'Calculate TCI average...';

 tci := ST_MapAlgebra(ARRAY[ROW(tci,1), ROW(tci2,1)]::rastbandarg[],
                            'ST_Mean4ma(double precision[], int[], text[])'::regprocedure,
                           	'32BF', 'LAST', null, 0, 0, null);


 RAISE NOTICE 'Calculate VHI raster...';
 vhi := ST_MapAlgebra(ARRAY[ROW(vci,1), ROW(tci,1)]::rastbandarg[],
                            'calculate_vhi_raster(double precision[], int[], text[])'::regprocedure,
                           	'32BF', 'LAST', null, 0, 0, null);



 IF store = TRUE THEN
    RAISE NOTICE 'Check if it exists...';
    id_acquisizione_in := -1;
    select id_acquisizione into id_acquisizione_in
               from postgis.acquisizioni
               where extract('doy' from dtime) = doy_in
               and    extract('year' from dtime) = year_in
               and   id_imgtype = imgtype_in;



    IF id_acquisizione_in <> -1 THEN
        RAISE NOTICE 'deleting old VHI...';

        delete from postgis.vhi
        where  id_acquisizione = id_acquisizione_in;

        delete from postgis.acquisizioni
        where  id_acquisizione = id_acquisizione_in;

        RAISE NOTICE 'done';

    END IF;

    RAISE NOTICE 'create new acquisition...';
    insert into postgis.acquisizioni (dtime, id_imgtype)
    values (dtime_in, imgtype_in);


    select id_acquisizione into id_acquisizione_in
    from   postgis.acquisizioni
    where  dtime = dtime_in
    and    id_imgtype = imgtype_in;

    RAISE NOTICE 'save VHI...';
    insert into postgis.vhi (id_acquisizione, rast)
    values (id_acquisizione_in, vhi);
    RAISE NOTICE 'done';

 END IF;

 RAISE NOTICE 'done.';
 RETURN vhi;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE


-- calculate vhi

CREATE OR REPLACE FUNCTION postgis.calculate_vhi_raster(value double precision[][][], pos integer[][], VARIADIC userargs text[])
	RETURNS double precision
	AS $$
	BEGIN


        -- (VCI * 0.5) + (TCI * 0.5)

	    RETURN ((value[1][1][1] * 0.5) + (value[2][1][1] * 0.5));


	END;
	$$ LANGUAGE 'plpgsql' IMMUTABLE;


