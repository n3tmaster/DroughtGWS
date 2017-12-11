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