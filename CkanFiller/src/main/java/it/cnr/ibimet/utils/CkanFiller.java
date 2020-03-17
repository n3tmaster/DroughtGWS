package it.cnr.ibimet.utils;


import it.lr.libs.DBManager;


import org.apache.http.client.methods.HttpGet;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;


import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;


import java.io.*;
import java.net.SocketTimeoutException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.TimeoutException;


/**
 * Created by lerocchi on 20/03/17.
 */
public class CkanFiller {

    //CKAN references
    private final static String REST_BASE_URL = "/api/3/action/";
    private final static String PACKAGE_CREATE = "package_create";
    private final static String PACKAGE_SHOW = "package_show";
    private final static String RESOURCE_CREATE = "resource_create";
    private final static String RESOURCE_VIEW_CREATE = "resource_view_create";



    //Geoserver references
    private final static String GEOSERVER_BASE_URL = "/geoserver/wms/reflect?";
    private final static String GEOSERVER_LAYER = "layers";

    private final static String REST_BASE_GEOSERVER = "/geoserver/rest";
    private final static String REST_GEOSERVER_WORKSPACES = "/workspaces";
    private final static String REST_GEOSERVER_COVERAGESTORES = "/coveragestores";
    private final static String REST_GEOSERVER_COVERAGES = "/coverages";
    private final static String WORKSPACE_TCI = "/TCI";
    private final static String WORKSPACE_VCI = "/VCI";
    private final static String WORKSPACE_VHI = "/VHI";
    private final static String WORKSPACE_SPI3 = "/SPI3";
    private final static String WORKSPACE_SPI6 = "/SPI6";
    private final static String WORKSPACE_SPI12 = "/SPI12";

    //DO - REST references
    private final static String REST_BASE_DOWNLOAD = "/dgws/api/download";
    private final static String REST_J_GET_IMAGE = "/j_get_image";
    private final static String REST_J_GET_WHOLE_PNG = "/j_get_whole_png";     ///{image_type}/{year}/{doy}";
    private final static String REST_J_GET_WHOLE_GTIFF = "/j_get_whole_gtiff";     ///{image_type}/{year}/{doy}";
    private final static String REST_J_GET_WHOLE_AAIGRID= "/j_get_whole_aaigrid";     ///{image_type}/{year}/{doy}";
    private final static String REST_J_GET_WHOLE_WMS= " ";     ///{image_type}/{year}/{doy}";
    private final static String REST_J_CALC_TCI = "/j_calc_tci";



    //////
    private final static String NAME_SUFFIX = "_id_v14";
    private final static String KML_FORMAT = "KML";
    private final static String CSV_FORMAT = "CSV";
    private final static String WMS_FORMAT = "WMS";
    private final static String AAIGRID_FORMAT = "AAIGrid";
    private final static String PNG_FORMAT = "PNG";
    private final static String GEOTIFF_FORMAT = "GeoTIFF";
    private final static String GTIFF_FORMAT = "GTIFF";


    //dataset types
    private final static String PRECIPITAZIONE = "PRECIPITAZIONE";
    private final static String EVI = "EVI";
    private final static String LST = "LST";
    private final static String TCI = "TCI";
    private final static String VCI = "VCI";
    private final static String VHI = "VHI";
    private final static String SPI3 = "SPI3";
    private final static String SPI6 = "SPI6";
    private final static String SPI12 = "SPI12";
    private final static String NDVI = "NDVI";

    //tablenames
    private final static String TBL_PRECIPITAZIONE = "precipitazioni";
    private final static String TBL_TCI = "tci";
    private final static String TBL_VCI = "vci";
    private final static String TBL_VHI = "vhi";
    private final static String TBL_SPI3 = "spi3";
    private final static String TBL_SPI6 = "spi6";
    private final static String TBL_SPI12 = "spi12";

    //input parameters
    private final static String CREATE_STORE = "create_store";
    private final static String CREATE_COVERAGE = "create_coverage";
    private final static String CREATE_PACKAGE = "create_package";
    private final static String INIT_NEW_STATION = "init_new_dataset";
    private final static String CREATE_ALL_RESOURCES = "create_all_resources";
    private final static String UPDATE_ALL_STATIONS = "update_all_dataset";
    private final static String CCBY = "cc-by";


    private final static int DAILY = 1;
    private final static int MONTHLY = 31;


    private final static String INIT_CKAN_DB = "init_ckan_db";

    private String mode;
    private String dataset;
    private String auth_id;
    private String owner;
    private String base_url;
    private String package_id;
    private int id_mobile_station;
    private String table_type;
    private String source_ws_base_url;
    private String source_geoserver_url;
    private String mantainer, mantainer_email;
    private GregorianCalendar gdata;
    private String dbcontext;
    private String dburl;
    private String dbpass;
    private String dbuser;
    private String datasettype;
    private int timestep;

    private String geoserver_workspace;
    private String notes;
    private String owner_org;
    private String maintainer;
    private String author;
    private String author_email;
    private String maintainer_email;
    private String geoserver_path, geoserver_cred, geoserver_func, geoserver_lyr_start;

    private JSONArray groups_in;
    private JSONArray extras_in,extras_head;
    private JSONArray tags_in;
    private JSONObject pkg_info;
    private String tbl_name;
    private int pkg_year;
    /**
     *
     * @param dataset
     * @param auth_id
     * @param base_url
     * @param source_ws_base_url
     * @param dburl
     * @param dbuser
     * @param dbpass
     * @param datasettype
     * @param pkg_info
     * @param groups_in
     * @param tags_in
     * @param extras_in
     * @throws Exception
     */
    //TODO: da completarlo per renderlo generale
    public CkanFiller(String dataset, String auth_id,
                      String base_url, String source_geoserver_url, String source_ws_base_url,
                      String dburl, String dbuser, String dbpass, String datasettype,
                      JSONObject pkg_info, JSONArray groups_in, JSONArray tags_in, JSONArray extras_in) throws Exception{
        this.dataset=dataset;
        this.base_url=base_url;
                this.source_ws_base_url=source_ws_base_url;
                this.dburl=dburl;
                this.dbuser=dbuser;
                this.dbpass=dbpass;
                this.auth_id=auth_id;
                this.datasettype=datasettype;
                this.pkg_info = pkg_info;
                this.groups_in = groups_in;
                this.tags_in = tags_in;
                this.extras_head = extras_in;
                this.source_geoserver_url = source_geoserver_url;
                this.extras_in = new JSONArray();
        this.dbcontext = "";
        this.geoserver_path = "";
        this.geoserver_cred = "";
        if(datasettype.matches(TCI)){
            this.datasettype = datasettype;
            this.timestep = MONTHLY;
            this.tbl_name = TBL_TCI;
        }else if(datasettype.matches(PRECIPITAZIONE)) {
            this.datasettype = datasettype;
            this.tbl_name = TBL_PRECIPITAZIONE;
            this.timestep = DAILY;


        }else{
            throw new Exception("Datatype is wrong: "+datasettype);


        }

    }



    public CkanFiller(JSONObject settings) throws Exception {
        this.dataset = settings.get("dataset").toString();
        this.base_url = settings.get("ckan_base_url").toString();
        this.source_ws_base_url = settings.get("ws_base_url").toString();
        this.dburl = settings.get("dburl").toString();
        this.dbuser = settings.get("dbuser").toString();
        this.dbpass = settings.get("dbpass").toString();
        this.auth_id = settings.get("auth_id").toString();
        this.datasettype = settings.get("dataset_type").toString();
        this.dbcontext = "";
        this.source_geoserver_url = settings.get("geoserver_base_url").toString();
        this.geoserver_workspace  = this.dataset;
        this.geoserver_path = settings.get("geoserver_path").toString();
        this.geoserver_cred = settings.get("geoserver_cred").toString();
        this.geoserver_func = settings.get("geoserver_func").toString();
        this.geoserver_lyr_start = settings.get("geoserver_lyr_start").toString();
    }

    public CkanFiller(JSONObject settings,
                      JSONObject pkg_info, JSONArray groups_in, JSONArray tags_in, JSONArray extras_in) throws Exception{
        this.dataset = settings.get("dataset").toString();
        this.base_url = settings.get("ckan_base_url").toString();
        this.source_ws_base_url = settings.get("ws_base_url").toString();
        this.dburl = settings.get("dburl").toString();
        this.dbuser = settings.get("dbuser").toString();
        this.dbpass = settings.get("dbpass").toString();
        this.auth_id = settings.get("auth_id").toString();
        this.datasettype = settings.get("dataset_type").toString();

        this.pkg_info = pkg_info;
        this.groups_in = groups_in;
        this.tags_in = tags_in;
        this.extras_head = extras_in;
        this.source_geoserver_url = settings.get("geoserver_base_url").toString();
        this.extras_in = new JSONArray();
        this.dbcontext = "";
        this.geoserver_workspace  = this.dataset;
        this.geoserver_path = settings.get("geoserver_path").toString();
        this.geoserver_cred = settings.get("geoserver_cred").toString();
        this.geoserver_func = settings.get("geoserver_func").toString();
        this.geoserver_lyr_start = settings.get("geoserver_lyr_start").toString();
        this.mode = settings.get("mode").toString();

        if(datasettype.matches(TCI)){

            this.timestep = MONTHLY;
            this.tbl_name = TBL_TCI;
            this.geoserver_workspace = WORKSPACE_TCI;
        }else if(datasettype.matches(PRECIPITAZIONE)) {

            this.tbl_name = TBL_PRECIPITAZIONE;
            this.timestep = DAILY;


        }else if(datasettype.matches(VCI)){

            this.timestep = MONTHLY;
            this.tbl_name = TBL_VCI;
            this.geoserver_workspace = WORKSPACE_VCI;
        }else if(datasettype.matches(VHI)){

            this.timestep = MONTHLY;
            this.tbl_name = TBL_VHI;
            this.geoserver_workspace = WORKSPACE_VHI;
        }else if(datasettype.matches(SPI3)){

            this.timestep = MONTHLY;
            this.tbl_name = TBL_SPI3;
            this.geoserver_workspace = WORKSPACE_SPI3;
        }else if(datasettype.matches(SPI6)){

            this.timestep = MONTHLY;
            this.tbl_name = TBL_SPI6;
            this.geoserver_workspace = WORKSPACE_SPI6;
        }else if(datasettype.matches(SPI12)){

            this.timestep = MONTHLY;
            this.tbl_name = TBL_SPI12;
            this.geoserver_workspace = WORKSPACE_SPI12;
        }else{
            throw new Exception("Datatype is wrong: "+datasettype);


        }

    }


    // constructor used by J2EE services
    // it uses context name in order to retrieve postgresql information
    //TODO: da completarlo per renderlo generale
    public CkanFiller(String dataset, String auth_id, String owner,
                      String base_url, String source_ws_base_url,
                      String dbcontex) throws Exception{



        this.dataset = dataset;
        this.auth_id = auth_id;
        this.owner = owner;
        this.base_url = base_url;
        this.source_ws_base_url = source_ws_base_url;
        this.dbcontext = dbcontex;



    }


    /**
     *
     * @param: arg[0] settings_json : json file containing following settings
     *
     *                 1 - dataset                  : dataset name (ALL if you want to load all datasets)
     *                 2 - authorization            : auth_id
     *                 3 - base_url                 : base url of CKAN installation
     *                 4 - source_geoserver_base_url: base url of geoserver API
     *                 5 - source_ws_base_url       : base url of Web Services API for retriving metadata
     *                 6 - dburl                    : dburl of Drought Observatory DB
     *                 7 - dbuser                   : username for db access
     *                 8 - dbpass                   : password for db access
     *                 9 - dataset_type             : dataset type
     *                        -  PRECIPITAZIONE
     *                        -  EVI
     *                        -  SPI
     *                        -  LST
     *                        -  TCI
     *                        -  SPI3
     *                        -  SPI6
     *                        -  SPI12
     *                        -  NDVI
     *                 10- geoserver                 : path to geoserver data directory used for creating MOSAIC
     *                 11- mode                      : init_ckan_db - init new dataste series into CKAN db (with packages and data)
     *                                                 mapping views for Geoserver and WMS
     *                 12- geoserver_cred            : "user:password" for geoserver
     * @param: arg[1] pkg_info: JSON file containing package metadata following CKAN specs.
     * @param: arg[2] groups:   JSON file containing package groups information following CKAN specs.
     * @param: arg[3] tags:     JSON file containing package tags following CKAN specs.
     * @param: arg[4] extras:   JSON file containing package extras information (metadata) following CKAN specs.
     * @throws: Exception
     */
    public static void main(String[] args) throws Exception {
        CkanFiller cf;

        //System.out.println("ARGS: "+ args.length);

        if (args.length == 5) {
        //    System.out.println("sono dentro");
            try {

                JSONParser parser = new JSONParser();

                JSONObject j1 = (JSONObject) parser.parse(new FileReader(args[1]));
                JSONArray j2 = (JSONArray) parser.parse(new FileReader(args[2]));
                JSONArray j3 = (JSONArray) parser.parse(new FileReader(args[3]));
                JSONArray j4 = (JSONArray) parser.parse(new FileReader(args[4]));
                JSONObject j5 = (JSONObject) parser.parse(new FileReader(args[0]));


                cf = new CkanFiller(j5,j1,j2,j3,j4);

                if (cf.mode.toLowerCase().matches(INIT_CKAN_DB)) {
                    //check station existance
                    cf.initCKAN();
                }
            } catch (Exception ex) {
                System.out.println(ex.getMessage());

                System.out.println("CKAN_FILLER for Drought Observatory");
                System.out.println("USAGE:  java -jar ./ckan_filler.jar ------ \t\t\n");

                System.out.println("\t\tcreate_package: create new dataset from specific station_id stored into SWH database");
                System.out.println("\t\tcreate_all_resources: create all resources of specific mobile_station");
                System.out.println("\t\tinit_new_station: create new dataset and init it bringing all data (daily)");
                System.out.println("\t\tupdate_all_stations: update data of existing stations, automatically. if it doesn't exist it will be created in ckan database with all resources");

            } finally {

            }

        }else if(args.length == 1) {
            try {

                System.out.println(args[0]);

                JSONParser parser = new JSONParser();

                JSONObject j5 = (JSONObject) parser.parse(new FileReader(args[0]));


                cf = new CkanFiller(j5);

                if(cf.geoserver_func.matches(CREATE_STORE)){
                    System.out.println("creating store");
                    cf.prepareGeoserverCreateStore();
                }else if(cf.geoserver_func.matches(CREATE_COVERAGE)){
                    cf.prepareGeoserverCreateCoverage();
                }

            } catch (Exception ex) {
                System.out.println(ex.getMessage());

                System.out.println("CKAN_FILLER for Drought Observatory");
                System.out.println("USAGE:  java -jar ./ckan_filler.jar ------ \t\t\n");

                System.out.println("\t\tcreate_package: create new dataset from specific station_id stored into SWH database");
                System.out.println("\t\tcreate_all_resources: create all resources of specific mobile_station");
                System.out.println("\t\tinit_new_station: create new dataset and init it bringing all data (daily)");
                System.out.println("\t\tupdate_all_stations: update data of existing stations, automatically. if it doesn't exist it will be created in ckan database with all resources");

            } finally {

            }
        }else{
                System.out.println("Parameter error");
                System.out.println("CKAN_FILLER for Drought Observatory");
                System.out.println("USAGE:  java -jar ./ckan_filler.jar -----\t\t\n" );

                System.out.println("mode: type of operation:");
                System.out.println("\t\tcreate_package: create new dataset from specific station_id stored into SWH database");
                System.out.println("\t\tcreate_all_resources: create all resources of specific mobile_station");
                System.out.println("\t\tinit_new_station: create new dataset and init it bringing all data (daily)");
                System.out.println("\t\tupdate_all_stations: update data of existing stations, automatically. if it doesn't exist it will be created in ckan database with all resources");


        }

        return;
    }

    /**
     *
     * checkExists: check is the packageid is exists
     *
     * @return true : yes it exists
     *         false : no it doesn't
     */

    public boolean checkExists(){
        TDBManager tdb=null;


        try {
            JSONObject json = new JSONObject();
            String url = base_url + REST_BASE_URL + PACKAGE_SHOW;
            json.put("id",dataset.toLowerCase()+NAME_SUFFIX);

            //send json structure for package creation process
            JSONObject retData = sendPost(json, url,true);

            //Check success or failure
            if(Boolean.parseBoolean(retData.get("success").toString())){
                System.out.println("Package exists");
                JSONObject j = (JSONObject) retData.get("result");
                package_id = j.get("id").toString();

                System.out.println("this id: "+package_id);
                return true;
            }else{
                System.out.println("Package doesn't exist");

                return false;
            }
        }catch (Exception e){
            System.out.println(e.getMessage());
            System.err.println(e.getMessage());
            e.printStackTrace();
        }finally{

        }



        return true;
    }



    public boolean createResource(int doy, String monthString, String dayString){


        boolean therearedata=false;

        try {


            String url = this.base_url + REST_BASE_URL + RESOURCE_CREATE;
            //create json structure
            String url_of_resource_geotiff, url_of_resource_png, url_of_resource_aaigrid, url_of_resource_wms;
            JSONObject json = new JSONObject();

            json.put("package_id",package_id);
            json.put("description"," ");
            json.put("url"," ");
            json.put("name"," ");



                therearedata=true;
                json.remove("url");
                json.remove("description");



                url_of_resource_geotiff = createWSURL(doy,REST_J_GET_IMAGE,GTIFF_FORMAT.toLowerCase());

                url_of_resource_png = createWSURL(doy,REST_J_GET_IMAGE,PNG_FORMAT.toLowerCase());

                url_of_resource_aaigrid = createWSURL(doy,REST_J_GET_IMAGE,AAIGRID_FORMAT.toLowerCase());

                url_of_resource_wms = createWSURL(this.datasettype.toLowerCase()+"_"+this.pkg_year+"_"+doy+"_out");


                json.put("description",""+this.pkg_year+"-"+monthString+"-"+dayString + " DOY: "+doy + " GeoTIFF");
                json.put("url",url_of_resource_geotiff);
                json.put("format","GeoTIFF");
                json.put("name",""+this.pkg_year+"-"+monthString+"-"+dayString);

                //TODO: da migliorare questo e metterlo in un ciclo dinamico

                //send json structure for package creation process
                JSONObject retData = sendPost(json, url);

                //Check success or failure
                if(Boolean.parseBoolean(retData.get("success").toString())){

                    JSONObject j = (JSONObject) retData.get("result");
                    System.out.println("Resource GEOTIFF created. Id: "+j.get("id").toString());
                }else{
                    System.out.println("Resource GEOTIFF Creation Error: ");
                    System.out.println("======================= ");
                    System.out.println(retData.toString());

                }

                //prepare PNG resource
                json.remove("url");
                json.put("url",url_of_resource_png);
                json.remove("format");
                json.put("format","PNG");
                json.remove("description");
                json.put("description",""+this.pkg_year+"-"+monthString+"-"+dayString + " DOY: "+doy + " PNG");

                retData = sendPost(json, url);

                //Check success or failure
                if(Boolean.parseBoolean(retData.get("success").toString())){

                    JSONObject j = (JSONObject) retData.get("result");
                    System.out.println("Resource PNG created. Id: "+j.get("id").toString());
                }else{
                    System.out.println("Resource PNG Creation Error: ");
                    System.out.println("======================= ");
                    System.out.println(retData.toString());

                }

                //prepare AAIGrid resource
                json.remove("url");
                json.put("url",url_of_resource_aaigrid);
                json.remove("format");
                json.put("format","AAIGrid");
                json.remove("description");
                json.put("description",""+this.pkg_year+"-"+monthString+"-"+dayString + " DOY: "+doy + " AAIGrid");

                retData = sendPost(json, url);
                //Check success or failure
                if(Boolean.parseBoolean(retData.get("success").toString())){

                    JSONObject j = (JSONObject) retData.get("result");
                    System.out.println("Resource AAIGrid created. Id: "+j.get("id").toString());
                }else{
                    System.out.println("Resource AAIGrid Creation Error: ");
                    System.out.println("======================= ");
                    System.out.println(retData.toString());

                }

                //prepare WMS resource
                json.remove("url");
                json.put("url",url_of_resource_wms);
                json.remove("format");
                json.put("format","wms");
                json.remove("description");
                json.put("description",""+this.pkg_year+"-"+monthString+"-"+dayString + " DOY: "+doy + " WMS");

                retData = sendPost(json, url);
                //Check success or failure
                if(Boolean.parseBoolean(retData.get("success").toString())){

                    JSONObject j = (JSONObject) retData.get("result");
                    System.out.println("Resource wms created. Id: "+j.get("id").toString());

                    JSONObject j2 = new JSONObject();

                    j2.put("resource_id",j.get("id").toString());
                    j2.put("view_type","geo_view");
                    j2.put("title","Map");

                    retData = sendPost(j2, this.base_url + REST_BASE_URL + RESOURCE_VIEW_CREATE);
                    if(Boolean.parseBoolean(retData.get("success").toString())){

                        System.out.println("Resource view created");

                    }else{
                        System.out.println("Resource view creation Error: ");
                        System.out.println("======================= ");
                        System.out.println(retData.toString());
                    }
                }else{
                    System.out.println("Resource wms Creation Error: ");
                    System.out.println("======================= ");
                    System.out.println(retData.toString());

                }





        }catch (Exception e){
            System.out.println(e.getMessage());
            System.err.println(e.getMessage());
            e.printStackTrace();
        }finally{




        }

        return therearedata;
    }


    /**
     * initCKAN: initCKANDB for specified product type
     *
     * @return 0: ok
     * @return -1: no data retrieved
     */
    public int initCKAN() {
        TDBManager tdb=null;
        String monthString="", dayString="";
        int extras2bedo=0,res2bedo=0; 
        //Checking for geoserver_path existence

        try{




            System.out.println("OK");


            if(!geoserver_path.matches(""))
                prepareGeoserverCatalog();

            System.out.print("createPackage: connecting...");

            if(dbcontext.matches("")){
                tdb = new TDBManager("org.postgresql.Driver", this.dburl, this.dbuser, this.dbpass);
            }else{
                tdb = new TDBManager(dbcontext);
            }



            tdb.openConnection();

            String url = this.base_url + REST_BASE_URL + PACKAGE_CREATE;
            String sqlString="";

            switch(this.timestep){
                case DAILY:
                    sqlString = "select distinct extract(year from dtime) "+
                            "from postgis.acquisizioni inner join postgis.imgtypes using (id_imgtype) " +
                            "where imgtype = '"+this.dataset+"' "+
                            "order by 1";
                    break;


                case MONTHLY:
                    sqlString = "select distinct extract(year from dtime), extract(month from dtime), extract(day from dtime), id_acquisizione, extract(doy from dtime) "+
                            "from postgis.acquisizioni inner join postgis.imgtypes using (id_imgtype) " +
                            "where imgtype = '"+this.dataset+"' "+
                            "order by 1,2";

                    System.out.println("SQL : "+sqlString);

                    break;


            }



            tdb.setPreparedStatementRef(sqlString);

            tdb.runPreparedQuery();
            while(tdb.next()){




                JSONObject json = new JSONObject();
                JSONParser jParser = new JSONParser();

                if(tdb.getString(2).length()<2){
                    monthString = "0"+tdb.getString(2);
                }else{
                    monthString = tdb.getString(2);
                }

                if(tdb.getString(3).length()<2){
                    dayString = "0"+tdb.getString(3);
                }else{
                    dayString = tdb.getString(3);
                }

                json = this.pkg_info;
                json.put("name",this.dataset.toLowerCase()+"_"+tdb.getInteger(1)+monthString+dayString+NAME_SUFFIX);
                json.put("title",this.dataset + " - " + tdb.getInteger(1) + "/" + monthString + "/" + dayString);
                json.put("private","false");
                this.pkg_year = tdb.getInteger(1);

                System.out.println("PkgInfo created");

                //preparing groups : structure - name , name


                json.put("groups",groups_in);

                System.out.println("Groups created");
                //preparing groups : structure - name , name

                json.put("tags",tags_in);

                System.out.println("Tags created");
                //preparing groups : structure - key , value
                System.out.println("closing Connection");
                tdb.closeConnection();
                extras2bedo = tdb.getInteger(4);
                res2bedo=tdb.getInteger(5);

                extractExtras(extras2bedo);



                json.put("extras",extras_in);


                System.out.println("Extras created");
                //send json structure for package creation process


                JSONObject retData = sendPost(json,url);

                //Check success or failure

                if(retData.containsKey("result")){
                    if(retData.get("result").toString().matches("conflict")) {
                        System.out.println("Package exists, it will be skipped ");
                    } else if(Boolean.parseBoolean(retData.get("success").toString())){
                        JSONObject thisObj = (JSONObject) retData.get("result");

                        System.out.println("Package created. Id: "+thisObj.get("id"));
                        package_id = thisObj.get("id").toString();

                        //call resources creation procedure
                        createResource(res2bedo, monthString,dayString);

                    }
                }else if(Boolean.parseBoolean(retData.get("success").toString())){
                    JSONObject thisObj = (JSONObject) retData.get("result");

                    System.out.println("Package created. Id: "+thisObj.get("id"));
                    package_id = thisObj.get("id").toString();

                    //call resources creation procedure
                    createResource(res2bedo, monthString,dayString);

                }else{
                    System.out.println("Package Creation Error: ");
                    System.out.println("======================= ");
                    System.out.println(retData.toString());

                    tdb.closeConnection();

                    return -1;
                }


                //Get package id

            }
        }catch(Exception e){

            System.out.println(e.getMessage());

            e.printStackTrace();
        }finally{
            try{
                tdb.closeConnection();
            }catch(Exception e){
                System.out.println(e.getMessage());

                e.printStackTrace();
            }
        }


        return 0;
    }


    private void prepareGeoserverCatalog() { ;
        TDBManager tdb=null;
        String sqlString = "select postgis.organize_geoserver_views(?)";
        JSONArray SRSArr = new JSONArray();
        JSONObject SRS = new JSONObject();
        JSONObject jsonRoot = new JSONObject();
        JSONObject jsonWorkspace = new JSONObject();
        JSONObject json = new JSONObject();
        JSONObject jsonNamespace = new JSONObject();
        JSONArray jsonKeywordsArr = new JSONArray();
        JSONObject jsonKeywords = new JSONObject();
        JSONObject jsonStore  = new JSONObject();
        JSONArray jsonSupFormatArr = new JSONArray();
        JSONObject jsonSupFormat = new JSONObject();
        JSONObject jsonInterpolationMethods = new JSONObject();
        JSONArray jsonInterpolationMethodsArr = new JSONArray();
        JSONObject retData;
        List<String> elements = new ArrayList<String>();
        boolean closed = false;
        SRSArr.add("EPSG:4326");

        jsonSupFormatArr.add("GEOTIFF");
        jsonSupFormatArr.add("GIF");
        jsonSupFormatArr.add("PNG");
        jsonSupFormatArr.add("JPEG");
        jsonSupFormatArr.add("TIFF");
        jsonSupFormatArr.add("ImageMosaicJDBC");
        jsonSupFormatArr.add("ArcGrid");
        jsonSupFormatArr.add("Gtopo30");
        jsonSupFormatArr.add("ImageMosaic");
        jsonSupFormatArr.add("GeoPackage (mosaic)");

        jsonInterpolationMethodsArr.add("nearest neighbor");
        jsonInterpolationMethodsArr.add("bilinear");
        jsonInterpolationMethodsArr.add("bicubic");

        System.out.print("createPackage: connecting...");

        try {
            if (dbcontext.matches("")) {
                tdb = new TDBManager("org.postgresql.Driver", this.dburl, this.dbuser, this.dbpass);
            } else {
                tdb = new TDBManager(dbcontext);
            }


            tdb.openConnection();

            tdb.setPreparedStatementRef(sqlString);
            tdb.setParameter(DBManager.ParameterType.STRING, this.dataset.toLowerCase(), 1);

            tdb.runPreparedQuery();


            //TODO: implementing PL/pgSQL exceptions management

            //Preparing element list
            if (tdb.next()) {
                System.out.println("Prepare Views procedure complete!");

                System.out.print("creating mapping xml files...");

                if(this.geoserver_lyr_start.matches("")) {
                    sqlString = "select name, doi from " +
                            "(select name,substring((trim(both '_out' from name)) from 11)::integer as doi from postgis.MOSAIC " +
                            "where name like '"+this.dataset.toLowerCase()+"%' " +
                            "order by doi) as fool ";
                }else{
                    String whereStr = this.geoserver_lyr_start.substring(0,(this.dataset.length()+6));
                    whereStr = whereStr.replace("_out","");
                    sqlString = "select name, doi from " +
                            "(select name,substring((trim(both '_out' from name)) from "+(this.dataset.length()+7)+")::integer as doi from postgis.MOSAIC " +
                            "where name like '"+this.geoserver_lyr_start.substring(0,(this.dataset.length()+7))+"%' " +
                            "order by doi) as fool " +
                            "where doi > "+whereStr;

                }


                tdb.setPreparedStatementRef(sqlString);


                tdb.runPreparedQuery();
                System.out.print("collecting elements...");
                while (tdb.next()) {
                    elements.add(tdb.getString(1));
                }
                System.out.println("done");
            }


            System.out.println("closing connection");
            tdb.closeConnection();
            System.out.println("done");
            closed=true;
            for (String thisElement : elements) {
                System.out.print(thisElement + ".pgraster.xml...");

                List<String> lines = new ArrayList<String>();
                lines.add("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>");
                lines.add("<!DOCTYPE ImageMosaicJDBCConfig [");
                lines.add("<!ENTITY mapping PUBLIC \"mapping\"  \"mapping.pgraster.xml.inc\">");
                lines.add("<!ENTITY connect PUBLIC \"connect\"  \"connect.pgraster.xml.inc\">");
                lines.add("]>");

                lines.add("<config version=\"1.0\">");
                lines.add("<coverageName name=\"" + thisElement + "\"/>");
                lines.add("<coordsys name=\"EPSG:4326\"/>");

                lines.add("<!-- interpolation 1 = nearest neighbour, 2 = bipolar, 3 = bicubic -->");
                lines.add("<scaleop  interpolation=\"1\"/>");
                lines.add("<axisOrder ignore=\"false\"/>");
                lines.add("&mapping;");
                lines.add("&connect;");
                lines.add("</config>");


                Files.write(Paths.get(this.geoserver_path + "/" + thisElement + ".pgraster.xml"), lines);

                System.out.println("OK");

                String url = source_geoserver_url + REST_BASE_GEOSERVER + REST_GEOSERVER_WORKSPACES + "/" + datasettype + REST_GEOSERVER_COVERAGESTORES;

                json.put("name", thisElement);
                json.put("description", thisElement);
                json.put("enabled", "true");
                json.put("url", "file:coverages/" + thisElement + ".pgraster.xml");
                json.put("type", "ImageMosaicJDBC");

                jsonWorkspace.put("name", datasettype);
                jsonWorkspace.put("link", datasettype.toLowerCase());
                json.put("workspace", jsonWorkspace);
                jsonRoot.put("coverageStore", json);


                retData = sendPost(jsonRoot, url, false);


                Thread.sleep(10000);

                if (!retData.get("result").toString().matches("error")) {

                    System.out.println("Store created");


                } else {
                    System.out.println("Store exists! it will be skipped.");
                }
                jsonWorkspace.clear();
                json.clear();
                jsonRoot.clear();

            }
            System.out.println("Starting with coverages...");

            Thread.sleep(10000);
            System.out.println("OK");


            for(String thisElement : elements){
                //TODO: mettere dentro un if ed eseguire solo se non ci sono errori




                String url = source_geoserver_url + REST_BASE_GEOSERVER + REST_GEOSERVER_WORKSPACES + "/" + datasettype + REST_GEOSERVER_COVERAGESTORES + "/" + thisElement + REST_GEOSERVER_COVERAGES;

                json.put("name", thisElement);
                json.put("nativeName", thisElement);


                jsonNamespace.put("name", datasettype);
                jsonNamespace.put("href", source_geoserver_url + REST_BASE_GEOSERVER + REST_GEOSERVER_WORKSPACES + "/" + datasettype + ".json");

                json.put("namespace", jsonNamespace);

                json.put("title", thisElement);
                json.put("description", "Generate from ImageMosaicJDBC");


                jsonKeywordsArr.add(thisElement);
                jsonKeywordsArr.add("WCS");
                jsonKeywordsArr.add("ImageMosaicJDBC");

                jsonKeywords.put("string", jsonKeywordsArr);

                json.put("keywords", jsonKeywords);
                json.put("srs", "EPSG:4326");
                json.put("projectionPolicy", "REPROJECT_TO_DECLARED");
                json.put("enabled", "true");


                jsonStore.put("@class", "coverageStore");
                jsonStore.put("href", source_geoserver_url + REST_BASE_GEOSERVER + REST_GEOSERVER_WORKSPACES + "/" + datasettype + REST_GEOSERVER_COVERAGESTORES + "/" + thisElement + ".json");
                jsonStore.put("name", datasettype + ":" + thisElement);

                json.put("store", jsonStore);
                json.put("nativeFormat", "ImageMosaicJDBC");


                jsonSupFormat.put("string", jsonSupFormatArr);

                json.put("supportedFormats", jsonSupFormat);


                jsonInterpolationMethods.put("string", jsonInterpolationMethodsArr);


                json.put("interpolationMethods", jsonInterpolationMethods);

                json.put("defaultInterpolationMethod", "nearest neighbor");


                SRS.put("string", SRSArr);

                json.put("requestSRS", SRS);
                json.put("responseSRS", SRS);

                json.put("nativeCoverageName", thisElement);


                jsonRoot.put("coverage", json);
                ////
                retData = sendPost(jsonRoot, url, false);
                System.out.print("Layer return: " + retData.toJSONString() + "...");
                jsonRoot.clear();
                jsonNamespace.clear();
                jsonStore.clear();
                jsonSupFormat.clear();
                jsonInterpolationMethods.clear();
                jsonWorkspace.clear();
                SRS.clear();
                json.clear();
                jsonKeywordsArr.clear();
                jsonKeywords.clear();
                jsonKeywordsArr.clear();



                System.out.println("waiting");
                Thread.sleep(10000);



            }
        }catch(Exception e){
            System.out.println(e.getMessage());
            if (!closed){
                try {
                    tdb.closeConnection();
                }catch(Exception ee){
                    System.out.println(ee.getMessage());
                }
            }
        }finally {
        	if (!closed){
                try {
                    tdb.closeConnection();
                }catch(Exception ee){
                    System.out.println(ee.getMessage());
                }
            }
        }
    }

    private void prepareGeoserverCreateCoverage() { ;
        TDBManager tdb=null;
        String sqlString = "";
        JSONArray SRSArr = new JSONArray();
        JSONObject SRS = new JSONObject();
        JSONObject jsonRoot = new JSONObject();
        JSONObject jsonWorkspace = new JSONObject();
        JSONObject json = new JSONObject();
        JSONObject jsonNamespace = new JSONObject();
        JSONArray jsonKeywordsArr = new JSONArray();
        JSONObject jsonKeywords = new JSONObject();
        JSONObject jsonStore  = new JSONObject();
        JSONArray jsonSupFormatArr = new JSONArray();
        JSONObject jsonSupFormat = new JSONObject();
        JSONObject jsonInterpolationMethods = new JSONObject();
        JSONArray jsonInterpolationMethodsArr = new JSONArray();
        JSONObject retData;
        List<String> elements = new ArrayList<String>();
        boolean closed = false;
        SRSArr.add("EPSG:4326");

        jsonSupFormatArr.add("GEOTIFF");
        jsonSupFormatArr.add("GIF");
        jsonSupFormatArr.add("PNG");
        jsonSupFormatArr.add("JPEG");
        jsonSupFormatArr.add("TIFF");
        jsonSupFormatArr.add("ImageMosaicJDBC");
        jsonSupFormatArr.add("ArcGrid");
        jsonSupFormatArr.add("Gtopo30");
        jsonSupFormatArr.add("ImageMosaic");
        jsonSupFormatArr.add("GeoPackage (mosaic)");

        jsonInterpolationMethodsArr.add("nearest neighbor");
        jsonInterpolationMethodsArr.add("bilinear");
        jsonInterpolationMethodsArr.add("bicubic");

        System.out.print("createPackage: connecting...");

        try {
            if (dbcontext.matches("")) {
                tdb = new TDBManager("org.postgresql.Driver", this.dburl, this.dbuser, this.dbpass);
            } else {
                tdb = new TDBManager(dbcontext);
            }


            tdb.openConnection();


            System.out.println("Prepare Views procedure complete!");

            if(this.geoserver_lyr_start.matches("")) {
                sqlString = "select name, doi from " +
                        "(select name,substring((trim(both '_out' from name)) from 11)::integer as doi from postgis.MOSAIC " +
                        "where name like '"+this.dataset.toLowerCase()+"%' " +
                        "order by doi) as fool ";
            }else{
                String whereStr = this.geoserver_lyr_start.substring(0,(this.dataset.length()+6));
                whereStr = whereStr.replace("_out","");
                sqlString = "select name, doi from " +
                        "(select name,substring((trim(both '_out' from name)) from "+(this.dataset.length()+7)+")::integer as doi from postgis.MOSAIC " +
                        "where name like '"+this.geoserver_lyr_start.substring(0,(this.dataset.length()+7))+"%' " +
                        "order by doi) as fool " +
                        "where doi > "+whereStr;

            }



            System.out.println(sqlString);

            tdb.setPreparedStatementRef(sqlString);

            tdb.runPreparedQuery();

            System.out.print("collecting elements...");
            while (tdb.next()) {
                elements.add(tdb.getString(1));
            }
            System.out.println("done");



            System.out.println("closing connection");
            tdb.closeConnection();
            System.out.println("done");
            closed=true;

            System.out.println("Starting with coverages...");




            for(String thisElement : elements){
                //TODO: mettere dentro un if ed eseguire solo se non ci sono errori

                String url = source_geoserver_url + REST_BASE_GEOSERVER + REST_GEOSERVER_WORKSPACES + "/" + datasettype + REST_GEOSERVER_COVERAGESTORES + "/" + thisElement + REST_GEOSERVER_COVERAGES;

                json.put("name", thisElement);
                json.put("nativeName", thisElement);


                jsonNamespace.put("name", datasettype);
                jsonNamespace.put("href", source_geoserver_url + REST_BASE_GEOSERVER + REST_GEOSERVER_WORKSPACES + "/" + datasettype + ".json");

                json.put("namespace", jsonNamespace);

                json.put("title", thisElement);
                json.put("description", "Generate from ImageMosaicJDBC");


                jsonKeywordsArr.add(thisElement);
                jsonKeywordsArr.add("WCS");
                jsonKeywordsArr.add("ImageMosaicJDBC");

                jsonKeywords.put("string", jsonKeywordsArr);

                json.put("keywords", jsonKeywords);
                json.put("srs", "EPSG:4326");
                json.put("projectionPolicy", "REPROJECT_TO_DECLARED");
                json.put("enabled", "true");


                jsonStore.put("@class", "coverageStore");
                jsonStore.put("href", source_geoserver_url + REST_BASE_GEOSERVER + REST_GEOSERVER_WORKSPACES + "/" + datasettype + REST_GEOSERVER_COVERAGESTORES + "/" + thisElement + ".json");
                jsonStore.put("name", datasettype + ":" + thisElement);

                json.put("store", jsonStore);
                json.put("nativeFormat", "ImageMosaicJDBC");


                jsonSupFormat.put("string", jsonSupFormatArr);

                json.put("supportedFormats", jsonSupFormat);


                jsonInterpolationMethods.put("string", jsonInterpolationMethodsArr);


                json.put("interpolationMethods", jsonInterpolationMethods);

                json.put("defaultInterpolationMethod", "nearest neighbor");


                SRS.put("string", SRSArr);

                json.put("requestSRS", SRS);
                json.put("responseSRS", SRS);

                json.put("nativeCoverageName", thisElement);


                jsonRoot.put("coverage", json);
                ////
                retData = sendPost(jsonRoot, url, false);
                System.out.print("Layer return: " + retData.toJSONString() + "...");
                jsonRoot.clear();
                jsonNamespace.clear();
                jsonStore.clear();
                jsonSupFormat.clear();
                jsonInterpolationMethods.clear();
                jsonWorkspace.clear();
                SRS.clear();
                json.clear();
                jsonKeywordsArr.clear();
                jsonKeywords.clear();
                jsonKeywordsArr.clear();



                System.out.println("waiting");
                Thread.sleep(8000);



            }
        }catch(Exception e){
            System.out.println(e.getMessage());
            if (!closed){
                try {
                    System.out.println("sono qui cazzo");
                    tdb.closeConnection();
                }catch(Exception ee){
                    System.out.println(ee.getMessage());
                }
            }
        }
    }



    private void prepareGeoserverCreateStore() {
        TDBManager tdb=null;
        String sqlString = "select postgis.organize_geoserver_views(?)";
        JSONArray SRSArr = new JSONArray();
        JSONObject SRS = new JSONObject();
        JSONObject jsonRoot = new JSONObject();
        JSONObject jsonWorkspace = new JSONObject();
        JSONObject json = new JSONObject();
        JSONObject jsonNamespace = new JSONObject();
        JSONArray jsonKeywordsArr = new JSONArray();
        JSONObject jsonKeywords = new JSONObject();
        JSONObject jsonStore  = new JSONObject();
        JSONArray jsonSupFormatArr = new JSONArray();
        JSONObject jsonSupFormat = new JSONObject();
        JSONObject jsonInterpolationMethods = new JSONObject();
        JSONArray jsonInterpolationMethodsArr = new JSONArray();
        JSONObject retData;
        List<String> elements = new ArrayList<String>();
        boolean closed = false;
        SRSArr.add("EPSG:4326");

        jsonSupFormatArr.add("GEOTIFF");
        jsonSupFormatArr.add("GIF");
        jsonSupFormatArr.add("PNG");
        jsonSupFormatArr.add("JPEG");
        jsonSupFormatArr.add("TIFF");
        jsonSupFormatArr.add("ImageMosaicJDBC");
        jsonSupFormatArr.add("ArcGrid");
        jsonSupFormatArr.add("Gtopo30");
        jsonSupFormatArr.add("ImageMosaic");
        jsonSupFormatArr.add("GeoPackage (mosaic)");

        jsonInterpolationMethodsArr.add("nearest neighbor");
        jsonInterpolationMethodsArr.add("bilinear");
        jsonInterpolationMethodsArr.add("bicubic");

        System.out.print("createPackage: connecting...");

        try {
            if (dbcontext.matches("")) {
                tdb = new TDBManager("org.postgresql.Driver", this.dburl, this.dbuser, this.dbpass);
            } else {
                tdb = new TDBManager(dbcontext);
            }


            tdb.openConnection();

            tdb.setPreparedStatementRef(sqlString);
            tdb.setParameter(DBManager.ParameterType.STRING, this.dataset.toLowerCase(), 1);

            tdb.runPreparedQuery();


            //TODO: implementing PL/pgSQL exceptions management

            //Preparing element list
            if (tdb.next()) {
                System.out.println("Prepare Views procedure complete!");

                System.out.print("creating mapping xml files...");

                if(this.geoserver_lyr_start.matches("")) {
                    sqlString = "select name, doi from " +
                            "(select name,substring((trim(both '_out' from name)) from 11)::integer as doi from postgis.MOSAIC " +
                            "where name like '"+this.dataset.toLowerCase()+"%' " +
                            "order by doi) as fool ";
                }else{
                    String whereStr = this.geoserver_lyr_start.substring(0,(this.dataset.length()+6));
                    whereStr = whereStr.replace("_out","");
                    sqlString = "select name, doi from " +
                            "(select name,substring((trim(both '_out' from name)) from "+(this.dataset.length()+7)+")::integer as doi from postgis.MOSAIC " +
                            "where name like '"+this.geoserver_lyr_start.substring(0,(this.dataset.length()+7))+"%' " +
                            "order by doi) as fool " +
                            "where doi > "+whereStr;

                }


                tdb.setPreparedStatementRef(sqlString);


                tdb.runPreparedQuery();
                System.out.print("collecting elements...");
                while (tdb.next()) {
                    elements.add(tdb.getString(1));
                }

                System.out.println("done");
            }


            System.out.println("closing connection");
            tdb.closeConnection();
            System.out.println("done");
            closed=true;
            for (String thisElement : elements) {
                System.out.print(thisElement + ".pgraster.xml...");

                List<String> lines = new ArrayList<String>();
                lines.add("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>");
                lines.add("<!DOCTYPE ImageMosaicJDBCConfig [");
                lines.add("<!ENTITY mapping PUBLIC \"mapping\"  \"mapping.pgraster.xml.inc\">");
                lines.add("<!ENTITY connect PUBLIC \"connect\"  \"connect.pgraster.xml.inc\">");
                lines.add("]>");

                lines.add("<config version=\"1.0\">");
                lines.add("<coverageName name=\"" + thisElement + "\"/>");
                lines.add("<coordsys name=\"EPSG:4326\"/>");

                lines.add("<!-- interpolation 1 = nearest neighbour, 2 = bipolar, 3 = bicubic -->");
                lines.add("<scaleop  interpolation=\"1\"/>");
                lines.add("<axisOrder ignore=\"false\"/>");
                lines.add("&mapping;");
                lines.add("&connect;");
                lines.add("</config>");


                Files.write(Paths.get(this.geoserver_path + "/" + thisElement + ".pgraster.xml"), lines);

                System.out.println("OK");

                String url = source_geoserver_url + REST_BASE_GEOSERVER + REST_GEOSERVER_WORKSPACES + "/" + datasettype + REST_GEOSERVER_COVERAGESTORES;

                json.put("name", thisElement);
                json.put("description", thisElement);
                json.put("enabled", "true");
                json.put("url", "file:coverages/" + thisElement + ".pgraster.xml");
                json.put("type", "ImageMosaicJDBC");

                jsonWorkspace.put("name", datasettype);
                jsonWorkspace.put("link", datasettype.toLowerCase());
                json.put("workspace", jsonWorkspace);
                jsonRoot.put("coverageStore", json);


                retData = sendPost(jsonRoot, url, false);


                Thread.sleep(8000);

                if (!retData.get("result").toString().matches("error")) {

                    System.out.println("Store created");


                } else {
                    System.out.println("Store exists! it will be skipped.");
                }
                jsonWorkspace.clear();
                json.clear();
                jsonRoot.clear();

            }
            System.out.println("Starting with coverages...");

            Thread.sleep(8000);
            System.out.println("Done");



        }catch(Exception e){
            System.out.println(e.getMessage());
            if (!closed){
                try {
                    tdb.closeConnection();
                }catch(Exception ee){
                    System.out.println(ee.getMessage());
                }
            }
        }
    }

    private void extractExtras(int id_acquisizione){

        String sqlString;
        JSONObject j;
        TDBManager tdb=null;
        extras_in.clear();

        extras_in = extras_head;


        try{
            System.out.print("createPackage: connecting...");

            if(dbcontext.matches("")){
                tdb = new TDBManager("org.postgresql.Driver", this.dburl, this.dbuser, this.dbpass);
            }else{
                tdb = new TDBManager(dbcontext);
            }


            //retrieving stats



            sqlString="SELECT (stats).count::numeric, (stats).sum::numeric, (stats).mean::numeric, (stats).stddev::numeric, (stats).min::numeric, (stats).max::numeric FROM " +
                    " (SELECT ST_SummaryStatsAgg( (SELECT ST_Union(rast) as stats from postgis."+this.tbl_name+" where id_acquisizione="+id_acquisizione+"), 1, TRUE) as stats) as foo" ;

            tdb.openConnection();

            tdb.setPreparedStatementRef(sqlString);


            System.out.println("SQL: "+sqlString);
            tdb.runPreparedQuery();


            if (tdb.next()){


                j = new JSONObject();
                j.put("key", "N. Pixels:");
                j.put("value", tdb.getInteger(1));

                extras_in.add(j);


                j = new JSONObject();
                j.put("key", "Sum:");
                j.put("value", tdb.getInteger(2));
                extras_in.add(j);


                j = new JSONObject();
                j.put("key", "Average:");
                j.put("value", tdb.getInteger(3));

                extras_in.add(j);

                j = new JSONObject();
                j.put("key", "Standard Deviation:");
                j.put("value", tdb.getInteger(4));
                extras_in.add(j);


                j = new JSONObject();
                j.put("key", "Min Value:");
                j.put("value", tdb.getInteger(5));

                extras_in.add(j);


                j = new JSONObject();
                j.put("key", "Max Value:");
                j.put("value", tdb.getInteger(6));

                extras_in.add(j);




            }
         


            sqlString="SELECT (foo.md).upperleftx,(foo.md).upperlefty,(foo.md).width,(foo.md).height,(foo.md).srid FROM " +
                    " (SELECT ST_MetaData(ST_Union(rast)) as md from postgis."+tbl_name+" where id_acquisizione="+id_acquisizione+") as foo" ;
        //   tdb.openConnection();

            tdb.setPreparedStatementRef(sqlString);
            System.out.println("SQL: "+sqlString);
            tdb.runPreparedQuery();


            if (tdb.next()){

                j = new JSONObject();
                j.put("key", "Upper Left X:");
                j.put("value", tdb.getInteger(1));

                extras_in.add(j);

                j = new JSONObject();
                j.put("key", "Upper Left Y:");
                j.put("value", tdb.getInteger(2));

                extras_in.add(j);

                j = new JSONObject();
                j.put("key", "Width:");
                j.put("value", tdb.getInteger(3));

                extras_in.add(j);

                j = new JSONObject();
                j.put("key", "Height:");
                j.put("value", tdb.getInteger(4));

                extras_in.add(j);


                j = new JSONObject();
                j.put("key", "SRID:");
                j.put("value", tdb.getInteger(5));

                extras_in.add(j);

            }

            sqlString="SELECT ST_BandPixelType(ST_Union(rast)) from postgis."+tbl_name+" where id_acquisizione="+id_acquisizione ;
     //       tdb.openConnection();

            tdb.setPreparedStatementRef(sqlString);
            System.out.println("SQL: "+sqlString);
            tdb.runPreparedQuery();
            if (tdb.next()) {
                j = new JSONObject();
                j.put("key", "Pixel Type:");
                j.put("value", tdb.getString(1));

                extras_in.add(j);

            }

        }catch (Exception e){
            System.out.println(e.getMessage());
            System.err.println(e.getMessage());
            e.printStackTrace();
        }finally{


            try{
                tdb.closeConnection();
            }catch(Exception e){
                System.out.println(e.getMessage());
                System.err.println(e.getMessage());
                e.printStackTrace();
            }



        }
    }


    public JSONObject sendPost(JSONObject json2send,String url, boolean jsonResult) {



        try{
            final HttpParams httpParams = new BasicHttpParams();

            HttpConnectionParams.setConnectionTimeout(httpParams, 120000);
            HttpConnectionParams.setSoTimeout(httpParams,120000);
            HttpClient httpClient = new DefaultHttpClient(httpParams);


            System.out.println("URL: "+url);

            //Prepare HttpPost connection
            HttpPost request = new HttpPost(url);

            System.out.println("Entity to send: "+json2send.toString());

            StringEntity params =new StringEntity(json2send.toString(), "UTF-8");   //passing json structure

            // System.out.println("Encoded string: "+params.toString());
            request.setHeader("Authorization", "Basic " +  Base64.getEncoder().encodeToString((geoserver_cred).getBytes()));
            request.addHeader("Accept", "application/json");
            request.addHeader("Content-Type", "application/json");
            request.setEntity(params);

            System.out.println("Sending data");


            HttpResponse response = httpClient.execute(request);

            //check response
            if(response.getStatusLine().getStatusCode() == HttpStatus.SC_CONFLICT){

                System.out.println("Error code: "+response.getStatusLine().getStatusCode());


            }else if(response.getStatusLine().getStatusCode() == HttpStatus.SC_CREATED ||
                    response.getStatusLine().getStatusCode() == HttpStatus.SC_CONTINUE){

                System.out.println("OK it was sent");
                //retrieve response data
                HttpEntity retData = response.getEntity();


                InputStream in = retData.getContent();


                //convert into JSON structure
                BufferedReader bReader = new BufferedReader(new InputStreamReader(in));
                StringBuilder sBuilder = new StringBuilder();
                String line = null;

                //if func manages no json result, it will convert into json structure
                if(!jsonResult)
                    sBuilder.append("{ \"result\": \"\n");


                while ((line = bReader.readLine()) != null) {
                    sBuilder.append(line + "\n");
                }
                if(!jsonResult)
                    sBuilder.append("\"}\n");

                System.out.println("RetString: "+sBuilder.toString());
                JSONParser parser = new JSONParser();
                JSONObject out = (JSONObject) parser.parse(sBuilder.toString());





                return out;

            }else if(response.getStatusLine().getStatusCode() == HttpStatus.SC_UNAUTHORIZED){
                System.out.println("Error code: "+response.getStatusLine().getStatusCode());
            }else {
                System.out.println("Error code: "+response.getStatusLine().getStatusCode());

                StringBuilder sBuilder = new StringBuilder();
                String line = null;

                sBuilder.append("{\"result\":\"error\"}");


                JSONParser parser = new JSONParser();
                JSONObject out = (JSONObject) parser.parse(sBuilder.toString());

                return out;
            }


        }catch (SocketTimeoutException e){
            StringBuilder sBuilder = new StringBuilder();
            String line = null;

            sBuilder.append("{\"result\":\"timeout-complete\"}");


            JSONParser parser = new JSONParser();
            try{
                JSONObject out = (JSONObject) parser.parse(sBuilder.toString());
                return out;
            }catch(ParseException ee){
                System.out.println("Error code: "+ee.getMessage());
                return null;
            }

        }catch (IOException e){
            System.out.println("Error code: "+e.getMessage());

        }catch (ParseException e){
            System.out.println("Error code: "+e.getMessage());
        }catch (Exception e){
            System.out.println("Error code: "+e.getMessage());
        }


        return null;

    }


    public JSONObject sendPost(JSONObject json2send,String url) {



        try{
            final HttpParams httpParams = new BasicHttpParams();

        //    HttpConnectionParams.setConnectionTimeout(httpParams, 3000);
        //    HttpConnectionParams.setSoTimeout(httpParams,3000);
            HttpClient httpClient = new DefaultHttpClient();


            System.out.println("URL: "+url);

            //Prepare HttpPost connection
            HttpPost request = new HttpPost(url);

            System.out.println("Entity to send: "+json2send.toString());

            StringEntity params =new StringEntity(json2send.toString(), "UTF-8");   //passing json structure

            // System.out.println("Encoded string: "+params.toString());
            request.addHeader("content-type", "application/x-www-form-urlencoded");
            request.addHeader("Authorization", auth_id);   //passing authorization id

            request.setEntity(params);

            System.out.println("Sending data");


            HttpResponse response = httpClient.execute(request);

            //check response
            if(response.getStatusLine().getStatusCode() == HttpStatus.SC_CONFLICT){

                System.out.println("Conflict resource exists: "+response.getStatusLine().getStatusCode());
                StringBuilder sBuilder = new StringBuilder();
                String line = null;

                sBuilder.append("{\"result\":\"conflict\"}");


                JSONParser parser = new JSONParser();
                JSONObject out = (JSONObject) parser.parse(sBuilder.toString());

                return out;

            }else if(response.getStatusLine().getStatusCode() == HttpStatus.SC_CREATED ||
                    response.getStatusLine().getStatusCode() == HttpStatus.SC_CONTINUE ||
                    response.getStatusLine().getStatusCode() == HttpStatus.SC_OK){

                System.out.println("OK it was sent");
                //retrieve response data
                HttpEntity retData = response.getEntity();


                InputStream in = retData.getContent();


                //convert into JSON structure
                BufferedReader bReader = new BufferedReader(new InputStreamReader(in));
                StringBuilder sBuilder = new StringBuilder();
                String line = null;

                //if func manages no json result, it will convert into json structure

                while ((line = bReader.readLine()) != null) {
                    sBuilder.append(line + "\n");
                }

                System.out.println("RetString: "+sBuilder.toString());
                JSONParser parser = new JSONParser();
                JSONObject out = (JSONObject) parser.parse(sBuilder.toString());





                return out;

            }else if(response.getStatusLine().getStatusCode() == HttpStatus.SC_UNAUTHORIZED){
                System.out.println("Unauthorized: "+response.getStatusLine().getStatusCode());
            }else {
                System.out.println("Error code: "+response.getStatusLine().getStatusCode());

                StringBuilder sBuilder = new StringBuilder();
                String line = null;

                sBuilder.append("{\"result\":\"error\"}");


                JSONParser parser = new JSONParser();
                JSONObject out = (JSONObject) parser.parse(sBuilder.toString());

                return out;
            }


        }catch (SocketTimeoutException e){
            StringBuilder sBuilder = new StringBuilder();
            String line = null;

            sBuilder.append("{\"result\":\"timeout-complete\"}");


            JSONParser parser = new JSONParser();
            try{
                JSONObject out = (JSONObject) parser.parse(sBuilder.toString());
                return out;
            }catch(ParseException ee){
                System.out.println("Error code: "+ee.getMessage());
                return null;
            }

        }catch (IOException e){
            System.out.println("Error code: "+e.getMessage());

        }catch (ParseException e){
            System.out.println("Error code: "+e.getMessage());
        }catch (Exception e){
            System.out.println("Error code: "+e.getMessage());
        }


        return null;

    }


    public JSONObject sendGet(JSONObject json2send,String url, boolean jsonResult) throws Exception {



        HttpClient httpClient = new DefaultHttpClient();


        System.out.println("URL: "+url);

        //Prepare HttpPost connection
        HttpGet request = new HttpGet(url);

        System.out.println("Entity to send: "+json2send.toString());

        StringEntity params =new StringEntity(json2send.toString(), "UTF-8");   //passing json structure

        // System.out.println("Encoded string: "+params.toString());
        request.setHeader("Authorization", "Basic " +  Base64.getEncoder().encodeToString((geoserver_cred).getBytes()));
        request.addHeader("Accept", "application/json");
        request.addHeader("Content-Type", "application/json");
       // request.setEntity(params);

        System.out.println("Sending data");

        HttpResponse response = httpClient.execute(request);

        //check response
        if(response.getStatusLine().getStatusCode() == HttpStatus.SC_CONFLICT){

            System.out.println("Error code: "+response.getStatusLine().getStatusCode());

            throw new Exception((response.getStatusLine().getReasonPhrase()));
        }else if(response.getStatusLine().getStatusCode() == HttpStatus.SC_CREATED ||
                response.getStatusLine().getStatusCode() == HttpStatus.SC_CONTINUE){

            System.out.println("OK it was sent");
            //retrieve response data
            HttpEntity retData = response.getEntity();


            InputStream in = retData.getContent();


            //convert into JSON structure
            BufferedReader bReader = new BufferedReader(new InputStreamReader(in));
            StringBuilder sBuilder = new StringBuilder();
            String line = null;

            //if func manages no json result, it will convert into json structure
            if(!jsonResult)
                sBuilder.append("{ \"result\": \"\n");


            while ((line = bReader.readLine()) != null) {
                sBuilder.append(line + "\n");
            }
            if(!jsonResult)
                sBuilder.append("\"}\n");

            System.out.println("RetString: "+sBuilder.toString());
            JSONParser parser = new JSONParser();
            JSONObject out = (JSONObject) parser.parse(sBuilder.toString());





            return out;

        }else if(response.getStatusLine().getStatusCode() == HttpStatus.SC_UNAUTHORIZED){
            System.out.println("Error code: "+response.getStatusLine().getStatusCode());
        }else {
            System.out.println("Error code: "+response.getStatusLine().getStatusCode());

            StringBuilder sBuilder = new StringBuilder();
            String line = null;

            sBuilder.append("{\"result\":\"error\"}");


            JSONParser parser = new JSONParser();
            JSONObject out = (JSONObject) parser.parse(sBuilder.toString());

            return out;
        }


        return null;

    }
    
    private String createWSURL(int doy, String format, String outputformat){

        //  if(datasettype.matches(TCI)) {
        //      return (source_ws_base_url + REST_BASE_DOWNLOAD + REST_J_CALC_TCI + "?year=" + this.pkg_year + "&gg=" + doy+"&format="+format+"&streamed=0");
        //  }
  //droughtsdi.fi.ibimet.cnr.it/dgws/api/download/j_get_image/vhi/2018/129/png/
          return source_ws_base_url + REST_BASE_DOWNLOAD + format + "/" + datasettype.toLowerCase() + "/" + this.pkg_year + "/" + doy+"/"+outputformat+"/";
      }

    private String createWSURL(int doy, String format){

      //  if(datasettype.matches(TCI)) {
      //      return (source_ws_base_url + REST_BASE_DOWNLOAD + REST_J_CALC_TCI + "?year=" + this.pkg_year + "&gg=" + doy+"&format="+format+"&streamed=0");
      //  }
//droughtsdi.fi.ibimet.cnr.it/dgws/api/download/j_get_image/vhi/2018/129/png/
        return source_ws_base_url + REST_BASE_DOWNLOAD + format + "/" + datasettype.toLowerCase() + "/" + this.pkg_year + "/" + doy;
    }

    private String createWSURL(String layer_name){

       // if(datasettype.matches(TCI)) {
        return (source_geoserver_url + GEOSERVER_BASE_URL + "#" + datasettype.toUpperCase() + ":" + layer_name);
       // }

       // return "";
    }

}
