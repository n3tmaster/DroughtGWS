package it.cnr.ibimet.utils;


import it.lr.libs.DBManager;


//import jdk.nashorn.internal.parser.JSONParser;
//import org.codehaus.jettison.json.JSONArray;
//import org.codehaus.jettison.json.JSONObject;

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


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.StringTokenizer;


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


    //DO - REST references

    private final static String REST_BASE_DOWNLOAD = "/dgws/api/download";
    private final static String REST_J_GET_WHOLE_PNG = "/j_get_whole_png";     ///{image_type}/{year}/{doy}";
    private final static String REST_J_GET_WHOLE_GTIFF = "/j_get_whole_gtiff";     ///{image_type}/{year}/{doy}";
    private final static String REST_J_GET_WHOLE_AAIGRID= "/j_get_whole_png";     ///{image_type}/{year}/{doy}";
    private final static String REST_J_GET_WHOLE_WMS= " ";     ///{image_type}/{year}/{doy}";
    private final static String REST_J_CALC_TCI = "/j_calc_tci";




    //////


    private final static String NAME_SUFFIX = "_id_v5";
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

    //input parameters
    private final static String CREATE_PACKAGE = "create_package";
    private final static String INIT_NEW_STATION = "init_new_dataset";
    private final static String CREATE_ALL_RESOURCES = "create_all_resources";
    private final static String UPDATE_ALL_STATIONS = "update_all_dataset";
    private final static String CCBY = "cc-by";


    private final static int DAILY = 1;
    private final static int MONTHLY = 31;


    private final static String INIT_CKAN_DB = "init_ckan_db";

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

    private String notes;
    private String owner_org;
    private String maintainer;
    private String author;
    private String author_email;
    private String maintainer_email;

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
     *
     * @param: arg[0] dataset : dataset name (ALL if you want to load all datasets)
     * @param: arg[1] authorization : auth_id
     * @param: arg[2] base_url : base url of CKAN installation
     * @param: arg[3] source_geoserver_base_url: base url of geoserver API
     * @param: arg[4] source_ws_base_url : base url of Web Services API for retriving metadata
     * @param: arg[5] dburl : dburl of Drought Observatory DB
     * @param: arg[6] dbuser : username for db access
     * @param: arg[7] dbpass : password for db access
     * @param: arg[8] dataset_type : dataset type
     *                        -  PRECIPITAZIONE
     *                        -  EVI
     *                        -  SPI
     *                        -  LST
     *                        -  TCI
     *                        -  SPI3
     *                        -  SPI6
     *                        -  SPI12
     *                        -  NDVI
     * @param: arg[9]  pkg_info: JSON file containing package metadata following CKAN specs.
     * @param: arg[10] groups:   JSON file containing package groups information following CKAN specs.
     * @param: arg[11] tags:     JSON file containing package tags following CKAN specs.
     * @param: arg[12] extras:   JSON file containing package extras information (metadata) following CKAN specs.
     * @param: arg[13] mode:     init_ckan_db - init new dataste series into CKAN db (with packages and data)
     *
     * @throws: Exception
     */
    public static void main(String[] args) throws Exception {

        if(args.length != 14){
            System.out.println("Parameter error");
        }else{

            JSONParser parser = new JSONParser();

            JSONObject j1 = (JSONObject) parser.parse(new FileReader(args[9]));
            JSONArray j2 = (JSONArray) parser.parse(new FileReader(args[10]));
            JSONArray j3 = (JSONArray) parser.parse(new FileReader(args[11]));
            JSONArray j4 = (JSONArray) parser.parse(new FileReader(args[12]));

            //TODO: da togliere la password hardcoded
            CkanFiller cf = new CkanFiller(args[0],args[1],args[2],args[3],args[4],args[5],args[6],"ss!2017pwd",args[8], j1,j2,j3,j4);


            try{


                if(args[13].toLowerCase().matches(INIT_CKAN_DB)) {
                    //check station existance

                    cf.initCKAN();

                }


            }catch(Exception ex){
                System.out.println(ex.getMessage());

                System.out.println("CKAN_FILLER for SensorWebHub platform");
                System.out.println("USAGE:  java -jar ./ckan_filler.jar station_id auth_id owner base_url dburl dbuser dbpass mode \t\t\n" );
                System.out.println("OPTIONS: ");
                System.out.println("station_id: name of mobile_station stored into SensorWebHub Database");
                System.out.println("auth_id: authorization id of the user who registered into ckan database");
                System.out.println("owner: organization id of datasets");
                System.out.println("base_url: base url of CKAN installation");
                System.out.println("swhrest_base_url: base url of SensorWeb Hub RESTful API");
                System.out.println("dburl: SensorWeb Hub database url (PostgreSQL) ");
                System.out.println("dbuser: username for SWH database");
                System.out.println("dbpass: password for SWH database");
                System.out.println("mode: type of operation:");
                System.out.println("\t\tcreate_package: create new dataset from specific station_id stored into SWH database");
                System.out.println("\t\tcreate_all_resources: create all resources of specific mobile_station");
                System.out.println("\t\tinit_new_station: create new dataset and init it bringing all data (daily)");
                System.out.println("\t\tupdate_all_stations: update data of existing stations, automatically. if it doesn't exist it will be created in ckan database with all resources");

            }finally{

            }
        }





        //

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
            JSONObject retData = sendPost(json, url);

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


            String url = base_url + REST_BASE_URL + RESOURCE_CREATE;
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






                url_of_resource_geotiff = createWSURL(doy,GTIFF_FORMAT);


                url_of_resource_png = createWSURL(doy,PNG_FORMAT);

                url_of_resource_aaigrid = createWSURL(doy,AAIGRID_FORMAT);

                url_of_resource_wms = createWSURL(doy,WMS_FORMAT,this.datasettype.toLowerCase()+"_"+this.pkg_year+"_"+doy+"_out");


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

                    retData = sendPost(j2, base_url + REST_BASE_URL + RESOURCE_VIEW_CREATE);
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

        try{
            System.out.print("createPackage: connecting...");

            if(dbcontext.matches("")){
                tdb = new TDBManager("org.postgresql.Driver", this.dburl, this.dbuser, this.dbpass);
            }else{
                tdb = new TDBManager(dbcontext);
            }


            tdb.openConnection();
            System.out.println("OK");
            String url = base_url + REST_BASE_URL + PACKAGE_CREATE;
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




                extractExtras(tdb.getInteger(4));



                json.put("extras",extras_in);


                System.out.println("Extras created");
                //send json structure for package creation process


                JSONObject retData = sendPost(json,url);

                //Check success or failure


                if(Boolean.parseBoolean(retData.get("success").toString())){
                    JSONObject thisObj = (JSONObject) retData.get("result");

                    System.out.println("Package created. Id: "+thisObj.get("id"));
                    package_id = thisObj.get("id").toString();

                    //call resources creation procedure
                    createResource(tdb.getInteger(5), monthString,dayString);



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
            tdb.openConnection();

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
            tdb.openConnection();

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

    public JSONObject sendPost(JSONObject json2send,String url) throws Exception {



        HttpClient httpClient = new DefaultHttpClient();

        System.out.println("Create HTTP-POST connection with id: "+auth_id);
        System.out.println("URL: "+url);

        //Prepare HttpPost connection
        HttpPost request = new HttpPost(url);

        System.out.println("Entity to send: "+json2send.toString());

        StringEntity params =new StringEntity(json2send.toString(), "UTF-8");   //passing json structure

        System.out.println("Encoded string: "+params.toString());
    //    request.addHeader("content-type", "application/x-www-form-urlencoded");
        request.addHeader("Authorization", auth_id);   //passing authorization id
        request.setEntity(params);

        System.out.println("Sending data");

        HttpResponse response = httpClient.execute(request);

        //check response
        if(response.getStatusLine().getStatusCode() == HttpStatus.SC_CONFLICT){

            System.out.println("Error code: "+response.getStatusLine().getStatusCode());

            throw new Exception((response.getStatusLine().getReasonPhrase()));
        }

        System.out.println("OK it was sent");
        //retrieve response data
        HttpEntity retData = response.getEntity();


        InputStream in = retData.getContent();


        //convert into JSON structure
        BufferedReader bReader = new BufferedReader(new InputStreamReader(in));
        StringBuilder sBuilder = new StringBuilder();
        String line = null;

        while ((line = bReader.readLine()) != null) {
            sBuilder.append(line + "\n");
        }


        // System.out.println("RetString: "+sBuilder.toString());

        JSONParser parser = new JSONParser();
        JSONObject out = (JSONObject) parser.parse(sBuilder.toString());


        return out;

    }




    private String createWSURL(int doy, String format){

        if(datasettype.matches(TCI)) {
            return (source_ws_base_url + REST_BASE_DOWNLOAD + REST_J_CALC_TCI + "?year=" + this.pkg_year + "&gg=" + doy+"&format="+format+"&streamed=0");
        }

        return "";
    }

    private String createWSURL(int doy, String format, String layer_name){

        if(datasettype.matches(TCI)) {
            return (source_geoserver_url + GEOSERVER_BASE_URL + "#" + TCI + ":" + layer_name);
        }

        return "";
    }

}
