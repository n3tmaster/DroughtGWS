CREATE TABLE acquisizioni
(
    id_acquisizione INTEGER PRIMARY KEY NOT NULL,
    dtime TIMESTAMP,
    id_imgtype INTEGER
);
CREATE TABLE evi
(
    rid INTEGER PRIMARY KEY NOT NULL,
    id_acquisizione INTEGER,
    rast RASTER
);
CREATE TABLE imgtypes
(
    id_imgtype INTEGER PRIMARY KEY NOT NULL,
    imgtype VARCHAR
);
CREATE TABLE lst
(
    rid INTEGER PRIMARY KEY NOT NULL,
    id_acquisizione INTEGER,
    rast RASTER
);
CREATE TABLE ndvi
(
    rid INTEGER PRIMARY KEY NOT NULL,
    id_acquisizione INTEGER,
    rast RASTER
);
CREATE TABLE precipitazioni
(
    rid INTEGER PRIMARY KEY NOT NULL,
    rast RASTER,
    id_acquisizione INTEGER
);
CREATE TABLE spatial_ref_sys
(
    srid INTEGER PRIMARY KEY NOT NULL,
    auth_name VARCHAR(256),
    auth_srid INTEGER,
    srtext VARCHAR(2048),
    proj4text VARCHAR(2048)
);
CREATE TABLE spi
(
    rid INTEGER PRIMARY KEY NOT NULL,
    id_acquisizione INTEGER,
    rast RASTER
);
CREATE TABLE tci
(
    rid INTEGER PRIMARY KEY NOT NULL,
    rast RASTER,
    id_acquisizione INTEGER
);
ALTER TABLE acquisizioni ADD FOREIGN KEY (id_imgtype) REFERENCES imgtypes (id_imgtype);
CREATE UNIQUE INDEX acquisizione_unique ON acquisizioni (id_imgtype, dtime);
ALTER TABLE evi ADD FOREIGN KEY (id_acquisizione) REFERENCES acquisizioni (id_acquisizione);
ALTER TABLE lst ADD FOREIGN KEY (id_acquisizione) REFERENCES acquisizioni (id_acquisizione);
ALTER TABLE ndvi ADD FOREIGN KEY (id_acquisizione) REFERENCES acquisizioni (id_acquisizione);
ALTER TABLE precipitazioni ADD FOREIGN KEY (id_acquisizione) REFERENCES acquisizioni (id_acquisizione);
ALTER TABLE spi ADD FOREIGN KEY (id_acquisizione) REFERENCES acquisizioni (id_acquisizione);
ALTER TABLE tci ADD FOREIGN KEY (id_acquisizione) REFERENCES acquisizioni (id_acquisizione);



-- VIEWS for data aggregation (eg monthly rainfall estimation)
CREATE VIEW postgis.monthly_rainfall as
        SELECT extract(month from dtime) as dmonth,
               extract(year from dtime)  as dyear,
               ST_Union(rast,'SUM')      as drast
        FROM postgis.precipitazioni
        INNER JOIN postgis.acquisizioni USING (id_acquisizione)
        GROUP BY 1,2
        ORDER BY 2,1;

create view monthly_rainfall as
select extract(month from dtime) as dmonth, extract(year from dtime) as dyear,
ST_Union(rast, 'SUM') as drast
FROM postgis.precipitazioni
GROUP BY 1,2
ORDER BY 2,1


-- Types for temporary raster data management
create type postgis.minmaxrast as (minrast RASTER, maxrast RASTER, originrast RASTER, zero_values RASTER, perc_threshold double precision);


-- Spatial indexes
CREATE INDEX vci_raster_idx ON postgis.vci USING gist( ST_ConvexHull(rast) );


CREATE TABLE postgis.spitemp
(
    dmonth integer,
    dyear integer,
    pxval character varying,
    dxo double precision,
    dyo double precision,
    dw integer,
    dh integer,
    dscalex double precision,
    dscaley double precision
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE postgis.spitemp
    OWNER to postgres;


-- Table: postgis.precipitazioni

-- DROP TABLE postgis.pre_rains;

CREATE TABLE postgis.pre_rains
(
    rid serial,
    rast raster,
    id_acquisizione integer,
    CONSTRAINT pre_rains_pkey PRIMARY KEY (rid),
    CONSTRAINT pre_rains_id_acquisizione_fkey FOREIGN KEY (id_acquisizione)
        REFERENCES postgis.acquisizioni (id_acquisizione) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE postgis.pre_rains
    OWNER to postgres;

-- Index: precipitazioni_idx

-- DROP INDEX postgis.precipitazioni_idx;

CREATE INDEX pre_rains_idx
    ON postgis.pre_rains USING btree
    (id_acquisizione)
    TABLESPACE pg_default;

-- Index: precipitazioni_idx2

-- DROP INDEX postgis.precipitazioni_idx2;

CREATE INDEX pre_rains_idx2
    ON postgis.pre_rains USING btree
    (rid, id_acquisizione)
    TABLESPACE pg_default;

-- Index: precipitazioni_raster_idx

-- DROP INDEX postgis.precipitazioni_raster_idx;

CREATE INDEX pre_rains_raster_idx
    ON postgis.pre_rains USING gist
    (st_convexhull(rast))
    TABLESPACE pg_default;

ALTER TABLE postgis.pre_rains
    CLUSTER ON pre_rains_raster_idx;