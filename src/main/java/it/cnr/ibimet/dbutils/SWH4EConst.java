/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.cnr.ibimet.dbutils;

/**
 * Costanti utilizzate dal progetto per la configurazione della webApp
 * @author lerocchi
 */
public interface SWH4EConst {

    //costanti per la multilingua
    String LINGUA_IT = "it";
    String LINGUA_EN = "en";
    String LINGUA_FR = "fr";

    //Costanti per la gestione dei dati RASTER

    //costanti per la gestione delle tipologie di poligoni
    String POLYGON_T = "POLYGON";
    String MULTIPOLYGON_T = "MULTIPOLYGON";
    String LINE_T = "LINE";
    String MULTILINESTRING="MULTILINESTRING";
    String POINT_T = "POINT";
    String MULTIPOINT_T = "MULTIPOINT";


    //Costanti dati strutturali
    String LIMITI_FRASCATI_DOC = "frascati_doc";




    //Costanti per la mappa
    double CENTER_X=-15.5676173;
    double CENTER_Y=12.9126752;
    int ZOOMFACTOR=9;
    int PRECISION = 5;




    //Costanti per i grafici
    String TITOLO_TEMP= "Temperature";
    String TITOLO_PIOGGIA = "Pioggia";
    String TITOLO_CO2= "CO2";
    String TITOLO_UMIDITA="Umidita";
    String TITOLO_OZONO="Ozono";
    String EDDY_STANDARD="EDDY";
    long NP_DIGITAL_VALUE=-1000;


    //Costanti per la gestione albero-nodi-select
    String DATI_STRUTTURALI = "DATI STRUTTURALI";
    String STAZIONI="STAZIONI METEO";

    String FRASCATI_DOC = "Zona Frascati DOC";
    String VIGNETI = "Vigneti";
    String STAZIONI_FISSE="Aziende agricole";

    //costanti tipologia layer
    String FRASCATI_DOC_TYPE=POLYGON_T;
    String VIGNETI_TYPE=POLYGON_T;
    String STAZ_FISSE_TYPE=POINT_T;

    //Costanti per la gestione albero-nodi-query
    String GET_FRASCATI = "select ST_AsKML(a.the_geom,8),a.nome_completo,b.colore,b.opacita "
            + " from frascati_doc a, legende b"
            + " where a.id_legenda=b.id_legenda";

    String GET_VIGNETI ="select ST_AsKML(a.the_geom,8),a.varieta,b.colore,b.opacita "
            + " from vigneti a, legende b"
            + " where a.id_legenda=b.id_legenda";

    String GET_STAZ_FISSE = "select ST_AsKML(b.the_geom,4),a.tair,a.co2,a.rad,a.o3, a.data " +
            "from dati_stazioni_fisse a, mobile_stations b " +
            "where  a.id_mobile_station=? " +
            "and    a.id_mobile_station=b.id_mobile_station " +
            "order by data desc limit 1";

    String GET_LAYER_TYPE = "select srid, type from geometry_columns where f_table_name=?";

    //Costanti per la gestione delle tabelle
    String TBL_FRASCATI_DOC = "frascati_doc";
    String TBL_VIGNETI = "vigneti";



    //Costanti gestione tipologia legende
    String LEGEND_TYPE_ALL_FEATURE = "ALL"; //indica di fare una query tra legends e la tabella spaziale e di usare solo il colore
    String LEGEND_TYPE_NONE = "NONE";       //indica di non usare legende
    String LEGEND_TYPE_SINGLE_COLOR = "SINGLE_COLOR"; //unico colore
    String LEGEND_TYPE_UNIQUE_VALUE = "UNIQUE_VALUE"; //su singoli valori
    String LEGEND_TYPE_CLASSBREAK_VALUE = "CLASS_BREAK"; //class break

    //Costanti per PostgreSQL
    String THE_GEOM_COLUMN = "the_geom";
    String GID = "gid";
    String DATA_TYPE_INTEGER="integer";
    String DATA_TYPE_NUMERIC="numeric";
    String DATA_TYPE_DOUBLE_PRECISION = "double precision";
    String DATA_TYPE_TIMESTAMP = "timestamp without time zone";
    String DATA_TYPE_TIMESTAMP_WT = "timestamp with time zone";

    String DATA_TYPE_STRING = "character varying";
    String TIME_STAMP_YES = "1";
    String TIME_STAMP_NO = "0";
    String VECTOR_TYPE_YES = "1";
    String VECTOR_TYPE_NO = "0";


    //Costanti per i moduli della piattaforma
    String MODULE_MOBILES = "MOBILES";

    //Costanti per la gestione delle meta-informazioni del db

    String DB_ENTITY = "ENTITY"; //entita generiche
    String DB_STATION = "STATION";
    String DB_DATA = "NORMAL";
    String DB_GEO = "GEO";


    //costanti per chiamate servlet
    String MACROTIPO_FISSE = "F";
    String MACROTIPO_MOBILI = "M";
    String TIPO_M = "M";
    String TIPO_F = "T";
    String TIPO_E = "E";

    int DOMINIO_URBAN = 1;
    int DOMINIO_AGROMETEO = 2;
    int DOMINIO_FOTOVOLTAICO = 3;


    String BLOCK_STR = "block;";
    String NONE_STR = "none;";

    int TILE_LENGTH = 30;

    //email list
    //TODO: Da cambiare appena possibile
    String EMAILFROM = "lerocchi@gmail.com";
    String EMAILTO = "l.rocchi@ibimet.cnr.it";

    String GTIFF = "GTIFF";
    String PNG = "PNG";
    String AAIGrid = "AAIGrid";


    //LEGENDS
    String TCI_LEGEND =
            "0 171 58 36 255\n"+
            " 1 171 58 36 255\n"+
            " 2 171 58 36 255\n"+
            " 3 171 58 36 255\n"+
            " 4 171 58 36 255\n"+
            " 5 171 58 36 255\n"+
            " 6 219 122 37 255\n" +
            " 7 219 122 37 255\n" +
            " 8 219 122 37 255\n" +
            " 9 219 122 37 255\n" +
            " 10 219 122 37 255\n" +
            " 11 219 122 37 255\n" +
            " 12 240 180 17 255\n" +
            " 13 240 180 17 255\n" +
            " 14 240 180 17 255\n" +
            " 15 240 180 17 255\n" +
            " 16 240 180 17 255\n" +
            " 17 240 180 17 255\n" +
            " 18 240 180 17 255\n" +
            " 19 240 180 17 255\n" +
            " 20 240 180 17 255\n" +
            " 21 240 180 17 255\n" +
            " 22 240 180 17 255\n" +
            " 23 240 180 17 255\n" +
            " 24 252 240 3 255\n" +
            " 25 252 240 3 255\n" +
            " 26 252 240 3 255\n" +
            " 27 252 240 3 255\n" +
            " 28 252 240 3 255\n" +
            " 29 252 240 3 255\n" +
            " 30 252 240 3 255\n" +
            " 31 252 240 3 255\n" +
            " 32 252 240 3 255\n" +
            " 33 252 240 3 255\n" +
            " 34 252 240 3 255\n" +
            " 35 252 240 3 255\n" +
            " 36 255 255 190 255\n" +
            " 37 255 255 190 255\n" +
            " 38 255 255 190 255\n" +
            " 39 255 255 190 255\n" +
            " 40 255 255 190 255\n" +
            " 41 255 255 190 255\n" +
            " 42 255 255 190 255\n" +
            " 43 255 255 190 255\n" +
            " 44 255 255 190 255\n" +
            " 45 255 255 190 255\n" +
            " 46 255 255 190 255\n" +
            " 47 255 255 190 255\n" +
            " 48 163 255 115 255\n" +
            " 49 163 255 115 255\n" +
            " 50 163 255 115 255\n" +
            " 51 163 255 115 255\n" +
            " 52 163 255 115 255\n" +
            " 53 163 255 115 255\n" +
            " 54 163 255 115 255\n" +
            " 55 163 255 115 255\n" +
            " 56 163 255 115 255\n" +
            " 57 163 255 115 255\n" +
            " 58 163 255 115 255\n" +
            " 59 163 255 115 255\n" +
            " 60 27 168 124 255\n" +
            " 61 27 168 124 255\n" +
            " 62 27 168 124 255\n" +
            " 63 27 168 124 255\n" +
            " 64 27 168 124 255\n" +
            " 65 27 168 124 255\n" +
            " 66 27 168 124 255\n" +
            " 67 27 168 124 255\n" +
            " 68 27 168 124 255\n" +
            " 69 27 168 124 255\n" +
            " 70 27 168 124 255\n" +
            " 71 27 168 124 255\n" +
            " 72 24 117 140 255\n" +
            " 73 24 117 140 255\n" +
            " 74 24 117 140 255\n" +
            " 75 24 117 140 255\n" +
            " 76 24 117 140 255\n" +
            " 77 24 117 140 255\n" +
            " 78 24 117 140 255\n" +
            " 79 24 117 140 255\n" +
            " 80 24 117 140 255\n" +
            " 81 24 117 140 255\n" +
            " 82 24 117 140 255\n" +
            " 83 24 117 140 255\n" +
            " 84 11 44 122 255\n" +
            " 85 11 44 122 255\n" +
            " 86 11 44 122 255\n" +
            " 87 11 44 122 255\n" +
            " 88 11 44 122 255\n" +
            " 89 11 44 122 255\n" +
            " 90 11 44 122 255\n" +
            " 91 11 44 122 255\n" +
            " 92 11 44 122 255\n" +
            " 93 11 44 122 255\n" +
            " 94 11 44 122 255\n" +
            " 95 11 44 122 255\n" +
            " 96 11 44 122 255\n" +
            " 97 11 44 122 255\n" +
            " 98 11 44 122 255\n" +
            " 99 11 44 122 255\n" +
            " 100 11 44 122 255\n" +
            " nv 191 191 191 0";


    String VCI_LEGEND =
            "0 171 58 36 255\n"+
                    " 1 171 58 36 255\n"+
                    " 2 171 58 36 255\n"+
                    " 3 171 58 36 255\n"+
                    " 4 171 58 36 255\n"+
                    " 5 171 58 36 255\n"+
                    " 6 219 122 37 255\n" +
                    " 7 219 122 37 255\n" +
                    " 8 219 122 37 255\n" +
                    " 9 219 122 37 255\n" +
                    " 10 219 122 37 255\n" +
                    " 11 219 122 37 255\n" +
                    " 12 240 180 17 255\n" +
                    " 13 240 180 17 255\n" +
                    " 14 240 180 17 255\n" +
                    " 15 240 180 17 255\n" +
                    " 16 240 180 17 255\n" +
                    " 17 240 180 17 255\n" +
                    " 18 240 180 17 255\n" +
                    " 19 240 180 17 255\n" +
                    " 20 240 180 17 255\n" +
                    " 21 240 180 17 255\n" +
                    " 22 240 180 17 255\n" +
                    " 23 240 180 17 255\n" +
                    " 24 252 240 3 255\n" +
                    " 25 252 240 3 255\n" +
                    " 26 252 240 3 255\n" +
                    " 27 252 240 3 255\n" +
                    " 28 252 240 3 255\n" +
                    " 29 252 240 3 255\n" +
                    " 30 252 240 3 255\n" +
                    " 31 252 240 3 255\n" +
                    " 32 252 240 3 255\n" +
                    " 33 252 240 3 255\n" +
                    " 34 252 240 3 255\n" +
                    " 35 252 240 3 255\n" +
                    " 36 255 255 190 255\n" +
                    " 37 255 255 190 255\n" +
                    " 38 255 255 190 255\n" +
                    " 39 255 255 190 255\n" +
                    " 40 255 255 190 255\n" +
                    " 41 255 255 190 255\n" +
                    " 42 255 255 190 255\n" +
                    " 43 255 255 190 255\n" +
                    " 44 255 255 190 255\n" +
                    " 45 255 255 190 255\n" +
                    " 46 255 255 190 255\n" +
                    " 47 255 255 190 255\n" +
                    " 48 163 255 115 255\n" +
                    " 49 163 255 115 255\n" +
                    " 50 163 255 115 255\n" +
                    " 51 163 255 115 255\n" +
                    " 52 163 255 115 255\n" +
                    " 53 163 255 115 255\n" +
                    " 54 163 255 115 255\n" +
                    " 55 163 255 115 255\n" +
                    " 56 163 255 115 255\n" +
                    " 57 163 255 115 255\n" +
                    " 58 163 255 115 255\n" +
                    " 59 163 255 115 255\n" +
                    " 60 27 168 124 255\n" +
                    " 61 27 168 124 255\n" +
                    " 62 27 168 124 255\n" +
                    " 63 27 168 124 255\n" +
                    " 64 27 168 124 255\n" +
                    " 65 27 168 124 255\n" +
                    " 66 27 168 124 255\n" +
                    " 67 27 168 124 255\n" +
                    " 68 27 168 124 255\n" +
                    " 69 27 168 124 255\n" +
                    " 70 27 168 124 255\n" +
                    " 71 27 168 124 255\n" +
                    " 72 24 117 140 255\n" +
                    " 73 24 117 140 255\n" +
                    " 74 24 117 140 255\n" +
                    " 75 24 117 140 255\n" +
                    " 76 24 117 140 255\n" +
                    " 77 24 117 140 255\n" +
                    " 78 24 117 140 255\n" +
                    " 79 24 117 140 255\n" +
                    " 80 24 117 140 255\n" +
                    " 81 24 117 140 255\n" +
                    " 82 24 117 140 255\n" +
                    " 83 24 117 140 255\n" +
                    " 84 11 44 122 255\n" +
                    " 85 11 44 122 255\n" +
                    " 86 11 44 122 255\n" +
                    " 87 11 44 122 255\n" +
                    " 88 11 44 122 255\n" +
                    " 89 11 44 122 255\n" +
                    " 90 11 44 122 255\n" +
                    " 91 11 44 122 255\n" +
                    " 92 11 44 122 255\n" +
                    " 93 11 44 122 255\n" +
                    " 94 11 44 122 255\n" +
                    " 95 11 44 122 255\n" +
                    " 96 11 44 122 255\n" +
                    " 97 11 44 122 255\n" +
                    " 98 11 44 122 255\n" +
                    " 99 11 44 122 255\n" +
                    " 100 11 44 122 255\n" +
                    " nv 191 191 191 0";



    public final static String NORMALIZED = "normalized";
    public final static String REAL = "real";

}
