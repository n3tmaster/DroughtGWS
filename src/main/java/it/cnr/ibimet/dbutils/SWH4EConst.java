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

    //Services const
    String WS_MODIS_SEARCH4FILES = "https://modwebsrv.modaps.eosdis.nasa.gov/axis2/services/MODAPSservices/searchForFiles?";
    String WS_MODIS_GETFILEURL = "https://modwebsrv.modaps.eosdis.nasa.gov/axis2/services/MODAPSservices/getFileUrls?";
    String CHIRPS_GET_RAIN = "https://data.chc.ucsb.edu/products/CHIRPS-2.0/global_daily/netcdf/p05/by_month/chirps-v2.0.";

    String MODIS_REPOBASE_URL = "https://ladsweb.modaps.eosdis.nasa.gov";
    String CHIRPS_GET_RAIN2 = ".days_p05.nc";

    String CHIRPS_GET_PRELIMINARY_RAIN = "https://data.chc.ucsb.edu/products/CHIRPS-2.0/prelim/global_daily/netcdf/p05/chirps-v2.0.";

    String NO_RESULTS = "no results";
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


    String GTIFF = "GTIFF";
    String PNG = "PNG";
    String AAIGrid = "AAIGrid";


    //LEGENDS
    String CRUD_LEGEND =
            "1 242 228 149 255\n"+
            " 2 210 129 73 255\n"+
            " 3 216 98 48 255\n"+
            " 4 135 238 152 255\n"+
            " 5 0 205 74 255\n"+
            " 6 4 139 49 255\n"+
            " nv 191 191 191 0";

    public final static String NORMALIZED = "normalized";
    public final static String REAL = "real";

    public final static String TCI_IMAGE = "TCI";
    public final static double MAX_POLYGON_AREA = 1500000.0;

    // Error codes
    public final static String POLYGON_TOO_BIG = "-2: PROVIDED POLYGON IS TOO BIG";
    public final static String DATA_NOT_FOUND = "-1: DATA NOT FOUND";
    public final static String POLYGON_IS_MANDATORY = "-3: YOU HAVE TO PROVIDE A POLYGON FOR DATA EXTRACTION";
    public final static String WRONG_IMAGE_TYPE = "-4: WRONG IMAGE TYPE";



}
