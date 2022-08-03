package it.cnr.ibimet.dgws;


import it.cnr.ibimet.dbutils.ErrorCode;
import it.cnr.ibimet.dbutils.SWH4EConst;
import it.cnr.ibimet.dbutils.TDBManager;
import it.lr.libs.DBManager;


import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;


import javax.annotation.security.PermitAll;
import javax.ws.rs.*;
import javax.ws.rs.core.Application;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.io.*;

import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.logging.Logger;

import static it.cnr.ibimet.dgws.ReclassConst.SPI_RECLASS_ANALISYS;
import static it.cnr.ibimet.dgws.ReclassConst.SPI_RECLASS_CCI;

/**
 * Created by lerocchi on 16/02/17.
 *
 * GetRaster
 *
 * retrieves raster data from postgis
 */
@Path("/calculate")

/**
 * Created by lerocchi on 14/09/17.
 */
public class Calculate  extends Application implements SWH4EConst {

    static Logger logger = Logger.getLogger(String.valueOf(Calculate.class));





    @POST
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces("image/tiff")
    @Path("/eta/{dtime}/{srid}")
    public Response calculateETa2(@PathParam("dtime") String dtime,
                                 @PathParam("srid") String srid,
                                 String polygon){

        byte[] imgOut=null;
        TDBManager tdb=null;
        String sqlString;

        try {

            logger.info("begin procedure "+dtime);




            if(polygon.matches("")){
                logger.info("polygon is empty");
                return Response.status(200).entity("polygon is empty").build();
            }else{
                tdb = new TDBManager("jdbc/ssdb");

                logger.info("call ETa calculation");
                Procedures thisProc = new Procedures(tdb);
                imgOut = thisProc.calculate_ETa(polygon, srid, dtime);


                if (imgOut != null) {
                    logger.info("Calculated, closing connection");
                    tdb.closeConnection();
                }else{
                    logger.info("image not calculated, closing connection");
                    try{
                        tdb.closeConnection();
                    }catch (Exception ee){
                        logger.warning(ee.getMessage());
                    }
                    return  Response.status(Response.Status.NOT_FOUND).entity("Failed attempt to calculate ETa image").build();
                }
            }

        }catch(Exception e){
            logger.warning(e.getMessage());

            try{
                tdb.closeConnection();
            }catch (Exception ee){
                logger.warning("Error "+ee.getMessage());
            }

            return Response.status(500).entity(e.getMessage()).build();
        }finally {
            try{
                tdb.closeConnection();
            }catch (Exception ee){
                logger.warning(ee.getMessage());
            }
        }


        logger.info("done");

        Response.ResponseBuilder responseBuilder = Response.ok(imgOut);
        responseBuilder.header("Content-Disposition", "attachment; filename=\"ETa_"+dtime+".tiff\"");

        return responseBuilder.build();


    }

    @GET
    @Produces("image/tiff")
    @Path("/rain_cum/{year}/{month}")
    public Response calculateRainSum(@PathParam("year") String year,
                                 @PathParam("month") String month){

        byte[] imgOut=null;
        TDBManager tdb=null;
        GregorianCalendar gc = new GregorianCalendar();
        Vector<InputStream> inputStreams = new Vector<InputStream>();




        try {

            tdb = new TDBManager("jdbc/ssdb");
            String sqlString=null;

            sqlString="SELECT ST_AsGDALRaster(ST_Union(rast, 'SUM'),'GTiff') " +
                    "FROM postgis.precipitazioni inner join acquisizioni using(id_acquisizione) " +
                    "WHERE id_imgtype = 1 " +
                    "AND   extract(month from dtime)=? "+
                    "AND   extract(year from dtime)=? " +
                    "GROUP BY extract(month from dtime), extract(year from dtime)";

            System.out.println("SQL: "+sqlString);

            tdb.setPreparedStatementRef(sqlString);
            tdb.setParameter(DBManager.ParameterType.INT,month,1);
            tdb.setParameter(DBManager.ParameterType.INT,year,2);


            tdb.runPreparedQuery();

            if (tdb.next()) {
                System.out.println("eccomi");
                imgOut = tdb.getPStmt().getResultSet().getBytes(1);
                System.out.println("Image Readed length: "+imgOut.length);
            }else{
                try{
                    tdb.closeConnection();
                }catch (Exception ee){
                    System.out.println("Error "+ee.getMessage());
                }
                return  Response.status(Response.Status.NOT_FOUND).entity("Image rain sum of "+month+"-"+year+" not found ").build();
            }

        }catch(Exception e){
            System.out.println("Error  : "+e.getMessage());

            try{
                tdb.closeConnection();
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
            }

            return Response.status(500).entity(e.getMessage()).build();
        }finally {
            try{
                tdb.closeConnection();
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
            }
        }


        System.out.println("fine");

        Response.ResponseBuilder responseBuilder = Response.ok(imgOut);
        responseBuilder.header("Content-Disposition", "attachment; filename=\"raincum_"+year+"_"+month+".tiff\"");

        return responseBuilder.build();


    }


    @GET
    @Produces(MediaType.TEXT_PLAIN)
    @Path("/tci/{year}/{doy}")
    public Response calculateTCI(@PathParam("year") String year,
                                     @PathParam("doy") String doy){

        byte[] imgOut=null;
        TDBManager tdb=null;
        GregorianCalendar gc = new GregorianCalendar();
        Vector<InputStream> inputStreams = new Vector<InputStream>();




        try {

            tdb = new TDBManager("jdbc/ssdb");
            String sqlString=null;



            System.out.print("Calculating TCI..." + doy + "-" + year);

            sqlString = "select count(*) from postgis.calculate_tci(?, ?)";
            tdb.setPreparedStatementRef(sqlString);
            tdb.setParameter(DBManager.ParameterType.INT, doy, 1);
            tdb.setParameter(DBManager.ParameterType.INT, year, 2);
            tdb.runPreparedQuery();

            if (tdb.next()) {
                System.out.println("Success.");

            } else {
                System.out.println("Attempt calculate TCI.");

                try{
                    System.out.println("TCI - Closing connections");
                    tdb.closeConnection();
                }catch (Exception ee){
                    System.out.println("Error "+ee.getMessage());
                }
                return  Response.status(Response.Status.NOT_FOUND).entity("LST for "+doy+"-"+year+" not found ").build();

            }

            System.out.println("TCI - Closing connections");

            tdb.closeConnection();
            System.out.println("Done.");



        }catch(Exception e){
            System.out.println("Error  : "+e.getMessage());

            try{
                tdb.closeConnection();
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
            }

            return Response.status(500).entity(e.getMessage()).build();
        }


        System.out.println("done");


        return Response.status(Response.Status.OK).entity("TCI for "+doy+"-"+year+" calculated ").build();


    }

    @GET
    @Produces("image/tiff")
    @Path("/pre_rain_cum/{year}/{month}")
    public Response calculatePreRainSum(@PathParam("year") String year,
                                     @PathParam("month") String month){

        byte[] imgOut=null;
        TDBManager tdb=null;
        GregorianCalendar gc = new GregorianCalendar();
        Vector<InputStream> inputStreams = new Vector<InputStream>();




        try {

            tdb = new TDBManager("jdbc/ssdb");
            String sqlString=null;

            sqlString="SELECT ST_AsGDALRaster(ST_Union(rast, 'SUM'),'GTiff') " +
                    "FROM postgis.pre_rains inner join acquisizioni using(id_acquisizione) " +
                    "WHERE id_imgtype = 18 " +
                    "AND   extract(month from dtime)=? "+
                    "AND   extract(year from dtime)=? " +
                    "GROUP BY extract(month from dtime), extract(year from dtime)";

            System.out.println("SQL: "+sqlString);

            tdb.setPreparedStatementRef(sqlString);
            tdb.setParameter(DBManager.ParameterType.INT,month,1);
            tdb.setParameter(DBManager.ParameterType.INT,year,2);

            tdb.runPreparedQuery();

            if (tdb.next()) {
                System.out.println("eccomi");
                imgOut = tdb.getPStmt().getResultSet().getBytes(1);
                System.out.println("Image Readed length: "+imgOut.length);
            }else{
                try{
                    tdb.closeConnection();
                }catch (Exception ee){
                    System.out.println("Error "+ee.getMessage());
                }
                return  Response.status(Response.Status.NOT_FOUND).entity("Image rain sum of "+month+"-"+year+" not found ").build();
            }

        }catch(Exception e){
            System.out.println("Error  : "+e.getMessage());

            try{
                tdb.closeConnection();
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
            }

            return Response.status(500).entity(e.getMessage()).build();
        }finally {
            try{
                tdb.closeConnection();
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
            }
        }


        System.out.println("done.");

        Response.ResponseBuilder responseBuilder = Response.ok(imgOut);
        responseBuilder.header("Content-Disposition", "attachment; filename=\"raincum_"+year+"_"+month+".tiff\"");

        return responseBuilder.build();


    }

    @GET
    @Produces("image/tiff")
    @Path("/lst_max/{year}/{doy}")
    public Response calculateLSTMax(@PathParam("year") String year,
                                     @PathParam("doy") String doy){

        byte[] imgOut=null;
        TDBManager tdb=null;
        GregorianCalendar gc = new GregorianCalendar();
        Vector<InputStream> inputStreams = new Vector<InputStream>();




        try {

            tdb = new TDBManager("jdbc/ssdb");
            String sqlString=null;


            gc.set(Calendar.YEAR, Integer.parseInt(year));
            gc.set(Calendar.DAY_OF_YEAR, Integer.parseInt(doy));



            sqlString="select ST_asGDALRaster(postgis.calculate_lst_max(?),'GTiff') ";

            System.out.println("SQL: "+sqlString);

            tdb.setPreparedStatementRef(sqlString);

            tdb.setParameter(DBManager.ParameterType.DATE,gc,1);
            tdb.runPreparedQuery();

            if (tdb.next()) {
                System.out.println("eccomi");
                imgOut = tdb.getPStmt().getResultSet().getBytes(1);
                System.out.println("Image Readed length: "+imgOut.length);
            }else{
                try{
                    tdb.closeConnection();
                }catch (Exception ee){
                    System.out.println("Error "+ee.getMessage());
                }
                return  Response.status(Response.Status.NOT_FOUND).entity("Image lst of "+doy+"-"+year+" not found ").build();
            }

        }catch(Exception e){
            System.out.println("Error  : "+e.getMessage());

            try{
                tdb.closeConnection();
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
            }

            return Response.status(500).entity(e.getMessage()).build();
        }finally {
            try{
                tdb.closeConnection();
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
            }
        }


        System.out.println("fine");

        Response.ResponseBuilder responseBuilder = Response.ok(imgOut);
        responseBuilder.header("Content-Disposition", "attachment; filename=\"lst_max_"+year+"_"+doy+".tiff\"");

        return responseBuilder.build();


    }


    @GET
    @Produces("image/tiff")
    @Path("/lst_min/{year}/{doy}")
    public Response calculateLSTMin(@PathParam("year") String year,
                                    @PathParam("doy") String doy){

        byte[] imgOut=null;
        TDBManager tdb=null;
        GregorianCalendar gc = new GregorianCalendar();
        Vector<InputStream> inputStreams = new Vector<InputStream>();




        try {

            tdb = new TDBManager("jdbc/ssdb");
            String sqlString=null;


            gc.set(Calendar.YEAR, Integer.parseInt(year));
            gc.set(Calendar.DAY_OF_YEAR, Integer.parseInt(doy));



            sqlString="select ST_asGDALRaster(postgis.calculate_lst_min(?),'GTiff') ";

            System.out.println("SQL: "+sqlString);

            tdb.setPreparedStatementRef(sqlString);

            tdb.setParameter(DBManager.ParameterType.DATE,gc,1);
            tdb.runPreparedQuery();

            if (tdb.next()) {
                System.out.println("eccomi");
                imgOut = tdb.getPStmt().getResultSet().getBytes(1);
                System.out.println("Image Readed length: "+imgOut.length);
            }else{
                try{
                    tdb.closeConnection();
                }catch (Exception ee){
                    System.out.println("Error "+ee.getMessage());
                }
                return  Response.status(Response.Status.NOT_FOUND).entity("Image lst of "+doy+"-"+year+" not found ").build();
            }

        }catch(Exception e){
            System.out.println("Error  : "+e.getMessage());

            try{
                tdb.closeConnection();
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
            }

            return Response.status(500).entity(e.getMessage()).build();
        }finally {
            try{
                tdb.closeConnection();
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
            }
        }


        System.out.println("fine");

        Response.ResponseBuilder responseBuilder = Response.ok(imgOut);
        responseBuilder.header("Content-Disposition", "attachment; filename=\"lst_min_"+year+"_"+doy+".tiff\"");

        return responseBuilder.build();


    }



    @GET
    @Produces("image/tiff")
    @Path("/resample_tci/{year}/{doy}")
    public Response calculateResampleTCI(@PathParam("year") String year,
                                     @PathParam("doy") String doy){

        byte[] imgOut=null;
        TDBManager tdb=null;

        Vector<InputStream> inputStreams = new Vector<InputStream>();




        try {

            tdb = new TDBManager("jdbc/ssdb");
            String sqlString=null;


            sqlString="select ST_asGDALRaster(postgis.calculate_resample_tci("+doy+", "+year+"),'GTiff') ";

            System.out.println("SQL: "+sqlString);

            tdb.setPreparedStatementRef(sqlString);

            tdb.runPreparedQuery();

            if (tdb.next()) {
                System.out.println("eccomi");
                imgOut = tdb.getPStmt().getResultSet().getBytes(1);
                System.out.println("Image Readed length: "+imgOut.length);
            }else{
                try{
                    tdb.closeConnection();
                }catch (Exception ee){
                    System.out.println("Error "+ee.getMessage());
                }
                return  Response.status(Response.Status.NOT_FOUND).entity("Image lst of "+doy+"-"+year+" not found ").build();
            }

        }catch(Exception e){
            System.out.println("Error  : "+e.getMessage());

            try{
                tdb.closeConnection();
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
            }

            return Response.status(500).entity(e.getMessage()).build();
        }finally {
            try{
                tdb.closeConnection();
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
            }
        }


        System.out.println("fine");

        Response.ResponseBuilder responseBuilder = Response.ok(imgOut);
        responseBuilder.header("Content-Disposition", "attachment; filename=\"resample_tci_"+year+"_"+doy+".tiff\"");

        return responseBuilder.build();


    }




    @GET
    @Produces("image/tiff")
    @Path("/mean_tci/{year}/{doy}")
    public Response calculateMeanTCI(@PathParam("year") String year,
                                    @PathParam("doy") String doy){

        byte[] imgOut=null;
        TDBManager tdb=null;

        Vector<InputStream> inputStreams = new Vector<InputStream>();




        try {

            tdb = new TDBManager("jdbc/ssdb");
            String sqlString=null;


            sqlString="select ST_asGDALRaster(postgis.calculate_mean_tci("+doy+", "+year+"),'GTiff') ";

            System.out.println("SQL: "+sqlString);

            tdb.setPreparedStatementRef(sqlString);

            tdb.runPreparedQuery();

            if (tdb.next()) {
                System.out.println("eccomi");
                imgOut = tdb.getPStmt().getResultSet().getBytes(1);
                System.out.println("Image Readed length: "+imgOut.length);
            }else{
                try{
                    tdb.closeConnection();
                }catch (Exception ee){
                    System.out.println("Error "+ee.getMessage());
                }
                return  Response.status(Response.Status.NOT_FOUND).entity("Image lst of "+doy+"-"+year+" not found ").build();
            }

        }catch(Exception e){
            System.out.println("Error  : "+e.getMessage());

            try{
                tdb.closeConnection();
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
            }

            return Response.status(500).entity(e.getMessage()).build();
        }finally {
            try{
                tdb.closeConnection();
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
            }
        }


        System.out.println("fine");

        Response.ResponseBuilder responseBuilder = Response.ok(imgOut);
        responseBuilder.header("Content-Disposition", "attachment; filename=\"mean_tci_"+year+"_"+doy+".tiff\"");

        return responseBuilder.build();


    }



    @GET
    @Produces(MediaType.TEXT_PLAIN)
    @Path("/{imgtype}/{year}/{doy}/{normalized}")
    public Response calculateTCI(@PathParam("imgtype") String imgtype,
                                 @PathParam("year") String year,
                                 @PathParam("doy") String doy,
                                 @PathParam("normalized") String normalized){

        byte[] imgOut=null;


        boolean create_it;
        TDBManager tdb=null;
        GregorianCalendar gc = new GregorianCalendar();
        Vector<InputStream> inputStreams = new Vector<InputStream>();
        String normalize2;
        try {



            if(normalized.toLowerCase().matches(NORMALIZED)){
            //    normalize = "st_reclass(rast,1,'[0.0-100.0]:1-100,(100.0-32767.0]:100','8BUI')";
                normalize2 = "st_reclass(postgis.calculate_"+imgtype+"(?,true),1,'[0.0-100.0]:1-100,(100.0-32767.0]:100','8BUI')";
            }else if(normalized.toLowerCase().matches(REAL)){

            //    normalize = "rast";
                normalize2 = "postgis.calculate_"+imgtype+"(?,true)";
            }else{

                //Error
                normalize2="";
                return Response.status(500).entity("Missing NORMALIZED parameter!").build();

            }

            gc.set(Calendar.YEAR, Integer.parseInt(year));
            gc.set(Calendar.HOUR_OF_DAY,0);
            gc.set(Calendar.MINUTE,0);
            gc.set(Calendar.SECOND,0);
            gc.set(Calendar.MILLISECOND,0);
            gc.set(Calendar.DAY_OF_YEAR, Integer.parseInt(doy));



            tdb = new TDBManager("jdbc/ssdb");
            String sqlString=null;

            //Check if TCI exists
            sqlString="select rast, a.id_acquisizione " +
                    "from postgis."+imgtype+" as a inner join postgis.acquisizioni as b using (id_acquisizione) "+
                    "where b.dtime = ?";


            tdb.setPreparedStatementRef(sqlString);

            tdb.setParameter(DBManager.ParameterType.DATE,gc,1);

            tdb.runPreparedQuery();

            if (tdb.next()) {

                System.out.println(imgtype.toUpperCase()+" exists...it will be recreated");


                String id_acquisizione = ""+tdb.getInteger(2);
                sqlString="delete from postgis."+imgtype+" where id_acquisizione = "+id_acquisizione;
                tdb.setPreparedStatementRef(sqlString);
                tdb.performInsert();

                System.out.print("old image deleted...");


                sqlString="delete from postgis.acquisizioni where id_acquisizione = "+id_acquisizione;
                tdb.setPreparedStatementRef(sqlString);
                tdb.performInsert();
                System.out.println("old acquisizione deleted");
                create_it=true;

            }



            sqlString="select "+normalize2;

            System.out.println(sqlString);

            tdb.setPreparedStatementRef(sqlString);

            tdb.setParameter(DBManager.ParameterType.DATE,gc,1);

            tdb.runPreparedQuery();

            if (tdb.next()) {
                try{
                    tdb.closeConnection();
                }catch (Exception ee){
                    System.out.println("Error "+ee.getMessage());
                }
                return  Response.status(Response.Status.OK).entity(imgtype.toUpperCase()+" image of "+doy+"-"+year+" calculated ").build();
            }else{
                try{
                    tdb.closeConnection();
                }catch (Exception ee){
                    System.out.println("Error "+ee.getMessage());
                }
                return  Response.status(Response.Status.OK).entity("Error occurred: maybe the "+imgtype.toUpperCase()+" image of "+doy+"-"+year+" doesn't exist ").build();
            }

        }catch(Exception e){
            System.out.println("Error  : "+e.getMessage());

            try{
                tdb.closeConnection();
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
            }

            return Response.status(500).entity(e.getMessage()).build();
        }



    }



    @GET
    @Produces("image/tiff")
    @Path("/ndvi_max/{year}/{doy}")
    public Response calculateNDVIMax(@PathParam("year") String year,
                                    @PathParam("doy") String doy){

        byte[] imgOut=null;
        TDBManager tdb=null;
        GregorianCalendar gc = new GregorianCalendar();
        Vector<InputStream> inputStreams = new Vector<InputStream>();




        try {

            tdb = new TDBManager("jdbc/ssdb");
            String sqlString=null;




            sqlString="select ST_asGDALRaster(postgis.calculate_ndvi_max(?,?),'GTiff') ";

            System.out.println("SQL: "+sqlString);

            tdb.setPreparedStatementRef(sqlString);

            tdb.setParameter(DBManager.ParameterType.INT,doy,1);
            tdb.setParameter(DBManager.ParameterType.INT,year,2);
            tdb.runPreparedQuery();

            if (tdb.next()) {
                System.out.println("eccomi");
                imgOut = tdb.getPStmt().getResultSet().getBytes(1);
                System.out.println("Image Readed length: "+imgOut.length);
            }else{
                try{
                    tdb.closeConnection();
                }catch (Exception ee){
                    System.out.println("Error "+ee.getMessage());
                }
                return  Response.status(Response.Status.NOT_FOUND).entity("Image lst of "+doy+"-"+year+" not found ").build();
            }

        }catch(Exception e){
            System.out.println("Error  : "+e.getMessage());

            try{
                tdb.closeConnection();
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
            }

            return Response.status(500).entity(e.getMessage()).build();
        }finally {
            try{
                tdb.closeConnection();
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
            }
        }


        System.out.println("fine");

        Response.ResponseBuilder responseBuilder = Response.ok(imgOut);
        responseBuilder.header("Content-Disposition", "attachment; filename=\"ndvi_max_"+year+"_"+doy+".tiff\"");

        return responseBuilder.build();


    }

    @GET
    @Produces("image/tiff")
    @Path("/ndvi_min/{year}/{doy}")
    public Response calculateNDVIMin(@PathParam("year") String year,
                                     @PathParam("doy") String doy){

        byte[] imgOut=null;
        TDBManager tdb=null;
        GregorianCalendar gc = new GregorianCalendar();
        Vector<InputStream> inputStreams = new Vector<InputStream>();




        try {

            tdb = new TDBManager("jdbc/ssdb");
            String sqlString=null;




            sqlString="select ST_asGDALRaster(postgis.calculate_ndvi_min(?,?),'GTiff') ";

            System.out.println("SQL: "+sqlString);

            tdb.setPreparedStatementRef(sqlString);

            tdb.setParameter(DBManager.ParameterType.INT,doy,1);
            tdb.setParameter(DBManager.ParameterType.INT,year,2);
            tdb.runPreparedQuery();

            if (tdb.next()) {
                System.out.println("eccomi");
                imgOut = tdb.getPStmt().getResultSet().getBytes(1);
                System.out.println("Image Readed length: "+imgOut.length);
            }else{
                try{
                    tdb.closeConnection();
                }catch (Exception ee){
                    System.out.println("Error "+ee.getMessage());
                }
                return  Response.status(Response.Status.NOT_FOUND).entity("Image lst of "+doy+"-"+year+" not found ").build();
            }

        }catch(Exception e){
            System.out.println("Error  : "+e.getMessage());

            try{
                tdb.closeConnection();
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
            }

            return Response.status(500).entity(e.getMessage()).build();
        }finally {
            try{
                tdb.closeConnection();
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
            }
        }


        System.out.println("fine");

        Response.ResponseBuilder responseBuilder = Response.ok(imgOut);
        responseBuilder.header("Content-Disposition", "attachment; filename=\"ndvi_min_"+year+"_"+doy+".tiff\"");

        return responseBuilder.build();


    }



    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/series/{image_type}{polygon:(/polygon/.+?)?}{srid_from:(/srid_from/.+?)?}{internal:(/internal/.+?)?}{region_name:(/region_name/.+?)?}")
    public Response calculateSeries(@PathParam("image_type") String image_type,
                                    @PathParam("polygon") String polygon,
                                    @PathParam("srid_from") String sridfrom,
                                    @PathParam("internal") String internal,
                                    @PathParam("region_name") String region_name){



        String retData="";
        try {




            String sqlString=null;



            if(polygon.matches("") || polygon == null || sridfrom.matches("") || sridfrom == null) {
                //check if internal number or region name are passed

                if(!(internal.matches("") || internal == null)) {

                    System.out.println(internal);
                    System.out.println(internal.split("/")[2]);

                    sqlString = "select * from postgis.calculate_stat_series('"+
                            image_type+"', (select the_geom from postgis.region_geoms where _id_region="+internal.split("/")[2]+"))";

                }else   if(!(region_name.matches("") || region_name == null)) {

                    System.out.println(region_name);
                    System.out.println(region_name.split("/")[2]);
                    sqlString = "select * from postgis.calculate_stat_series('"+
                            image_type+"', (select the_geom from postgis.region_geoms inner join postgis.regions using (_id_region) where name = '"+region_name.split("/")[2].toLowerCase()+"'))";
                }
                
            }else{

                System.out.println("Call calculate stat series");
                sqlString = "select * from postgis.calculate_stat_series('"+
                        image_type+"', ST_GeomFromText('"+polygon.split("/")[2]+"',"+sridfrom.split("/")[2]+"))";


            }


            retData = runStatCalc(sqlString);
        }catch(Exception e){
            System.out.println("Error  : "+e.getMessage());



            return Response.status(500).entity(e.getMessage()).build();
        }


        Response.ResponseBuilder responseBuilder = Response.ok(retData);

        return responseBuilder.build();
    }


    /**
     * calculateSeries - POST version, uses GeoJSON format for given polygon
     *
     * @param image_type
     * @param polygon
     * @return
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/series/{image_type}")
    public Response calculateSeries(@PathParam("image_type") String image_type,
                                    String polygon){



        String retData="";
        try {


            JSONParser parser = new JSONParser();

            System.out.println("SERIES - POST - JSON: start procedure");

            JSONObject jsonObject = (JSONObject) parser.parse(polygon.trim());

            if(jsonObject.toJSONString().trim().matches("")){
                Response.status(Response.Status.OK).entity(ErrorCode.POLYGON_IS_MANDATORY_STR).build();
            }


            String sqlString=null;




            System.out.println("Call calculate stat series");
            sqlString = "select * from postgis.calculate_stat_series('"+
                    image_type+"', ST_GeomFromJSON('"+jsonObject.get("geometry").toString()+"'))";





            retData = runStatCalc(sqlString);
        }catch(Exception e){
            System.out.println("SERIES - POST - JSON: "+e.getMessage());



            return Response.status(500).entity(e.getMessage()).build();
        }


        Response.ResponseBuilder responseBuilder = Response.ok(retData);

        return responseBuilder.build();
    }

    /**
     * calculateSeries - POST version, uses GeoJSON format for given polygon
     *
     * @param image_type
     * @param polygon
     * @return
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.TEXT_PLAIN)
    @Path("/series/{image_type}")
    public Response calculateSeriesCSV(@PathParam("image_type") String image_type,
                                    String polygon){



        String retData="";
        try {


            JSONParser parser = new JSONParser();

            System.out.println("SERIES-CSV - POST - JSON: start procedure");

            JSONObject jsonObject = (JSONObject) parser.parse(polygon.trim());

            if(jsonObject.toJSONString().trim().matches("")){
                Response.status(Response.Status.OK).entity(ErrorCode.POLYGON_IS_MANDATORY_STR).build();
            }


            String sqlString=null;




            System.out.println("Call calculate stat series");
            sqlString = "select * from postgis.calculate_stat_series('"+
                    image_type+"', ST_GeomFromJSON('"+jsonObject.get("geometry").toString()+"'))";





            retData = runStatCalcCSV(sqlString);
        }catch(Exception e){
            System.out.println("SERIES-CSV - POST - JSON: "+e.getMessage());



            return Response.status(500).entity(e.getMessage()).build();
        }


        Response.ResponseBuilder responseBuilder = Response.ok(retData);

        return responseBuilder.build();
    }

    /**
     * calculateSeries - POST version, uses WKT format for given polygon
     *
     * @param image_type
     * @param polygon
     * @return
     */
    @POST
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/series/{image_type}/{srid_from}")
    public Response calculateSeries(@PathParam("image_type") String image_type,
                                    @PathParam("srid_from") String srid_from,
                                    String polygon){



        String retData="";
        try {




            String sqlString=null;

            System.out.println("SERIES - POST - WKT: start procedure");



            System.out.println("Call calculate stat series");
            sqlString = "select * from postgis.calculate_stat_series('"+
                    image_type+"', ST_GeomFromText('"+polygon+"',"+srid_from+"))";





            retData = runStatCalc(sqlString);
        }catch(Exception e){
            System.out.println("SERIES - POST - WKT: "+e.getMessage());



            return Response.status(500).entity(e.getMessage()).build();
        }


        Response.ResponseBuilder responseBuilder = Response.ok(retData);

        return responseBuilder.build();
    }


    /**
     * calculateSeries - POST version, uses WKT format for given polygon
     *
     * @param image_type
     * @param polygon
     * @return
     */
    @POST
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(MediaType.TEXT_PLAIN)
    @Path("/series/{image_type}/{srid_from}")
    public Response calculateSeriesCSV(@PathParam("image_type") String image_type,
                                    @PathParam("srid_from") String srid_from,
                                    String polygon){



        String retData="";
        try {




            String sqlString=null;

            System.out.println("SERIES-CSV - POST - WKT: start procedure");



            System.out.println("Call calculate stat series");
            sqlString = "select * from postgis.calculate_stat_series('"+
                    image_type+"', ST_GeomFromText('"+polygon+"',"+srid_from+"))";





            retData = runStatCalcCSV(sqlString);
        }catch(Exception e){
            System.out.println("SERIES-CSV - POST - WKT: "+e.getMessage());



            return Response.status(500).entity(e.getMessage()).build();
        }


        Response.ResponseBuilder responseBuilder = Response.ok(retData);

        return responseBuilder.build();
    }

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    @Path("/seriescsv/{image_type}{polygon:(/polygon/.+?)?}{srid_from:(/srid_from/.+?)?}{internal:(/internal/.+?)?}{region_name:(/region_name/.+?)?}")
    public Response calculateSeriesCSV(@PathParam("image_type") String image_type,
                                    @PathParam("polygon") String polygon,
                                    @PathParam("srid_from") String sridfrom,
                                    @PathParam("internal") String internal,
                                    @PathParam("region_name") String region_name){



        String retData="";
        try {




            String sqlString=null;



            if(polygon.matches("") || polygon == null || sridfrom.matches("") || sridfrom == null) {
                //check if internal number or region name are passed

                if(!(internal.matches("") || internal == null)) {

                    System.out.println(internal);
                    System.out.println(internal.split("/")[2]);

                    sqlString = "select * from postgis.calculate_stat_series('"+
                            image_type+"', (select the_geom from postgis.region_geoms where _id_region="+internal.split("/")[2]+"))";

                }else   if(!(region_name.matches("") || region_name == null)) {

                    System.out.println(region_name);
                    System.out.println(region_name.split("/")[2]);
                    sqlString = "select * from postgis.calculate_stat_series('"+
                            image_type+"', (select the_geom from postgis.region_geoms inner join postgis.regions using (_id_region) where name = '"+region_name.split("/")[2].toLowerCase()+"'))";
                }

            }else{

                if(image_type.matches("spi3") || image_type.matches("spi6") || image_type.matches("spi12")){
                    System.out.println("Call calculate stat series SPI :"+image_type.substring(3));
                    sqlString = "select * from postgis.calculate_stat_series_adv2('"+
                            image_type+"', ST_GeomFromText('"+polygon.split("/")[2]+"',"+sridfrom.split("/")[2]+"),"+image_type.substring(3)+")";
                }else{
                    System.out.println("Call calculate stat series");
                    sqlString = "select * from postgis.calculate_stat_series('"+
                            image_type+"', ST_GeomFromText('"+polygon.split("/")[2]+"',"+sridfrom.split("/")[2]+"))";

                }



            }


            retData = runStatCalcCSV(sqlString);
        }catch(Exception e){
            System.out.println("Error  : "+e.getMessage());



            return Response.status(500).entity(e.getMessage()).build();
        }


        Response.ResponseBuilder responseBuilder = Response.ok(retData);

        return responseBuilder.build();
    }

    private String runStatCalcCSV(String sqlString){
        String retData="";
        DecimalFormat df = new DecimalFormat("##0.00",
                DecimalFormatSymbols.getInstance(Locale.US));

        TDBManager tdb=null;
        try {



            tdb = new TDBManager("jdbc/ssdb");



            System.out.println(sqlString);

            tdb.setPreparedStatementRef(sqlString);



            tdb.runPreparedQuery();
            retData = "doy;year;count;mean;stddev;min;max;q25;q75\n";
            while (tdb.next()) {

                retData = retData + tdb.getInteger(1) + ";" +
                        tdb.getInteger(2) + ";" +
                        tdb.getInteger(7) + ";" +
                        df.format(tdb.getDouble(3)) + ";" +
                        df.format(tdb.getDouble(4)) + ";" +
                        df.format(tdb.getDouble(5)) + ";" +
                        df.format(tdb.getDouble(6)) + ";" +
                        df.format(tdb.getDouble(8)) + ";" +
                        df.format(tdb.getDouble(9)) + "\n";

            }

        }catch(Exception e){
            System.out.println("Error  : "+e.getMessage());

            try{
                tdb.closeConnection();
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
            }

            return e.getMessage();
        }finally {
            try{
                tdb.closeConnection();
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
            }
        }


        return retData;

    }

    private String runStatCalc(String sqlString){
        String retData="";


        TDBManager tdb=null;
        try {



            tdb = new TDBManager("jdbc/ssdb");



            System.out.println(sqlString);

            tdb.setPreparedStatementRef(sqlString);



            tdb.runPreparedQuery();
            JSONArray jArray= new JSONArray();
            while (tdb.next()) {

                JSONObject jobj = new JSONObject();


                jobj.put("doy", tdb.getInteger(1));
                jobj.put("year", tdb.getInteger(2));
                jobj.put("count", tdb.getInteger(7));
                jobj.put("mean", tdb.getDouble(3));
                jobj.put("stddev", tdb.getDouble(4));
                jobj.put("min", tdb.getDouble(5));
                jobj.put("max", tdb.getDouble(6));
                jobj.put("q25", tdb.getDouble(8));
                jobj.put("q75", tdb.getDouble(9));

                jArray.add(jobj);

            }


            retData = jArray.toJSONString();

        }catch(Exception e){
            System.out.println("Error  : "+e.getMessage());

            try{
                tdb.closeConnection();
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
            }

            return e.getMessage();
        }finally {
            try{
                tdb.closeConnection();
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
            }
        }


        return retData;

    }




    @GET
    @Produces("image/png")
    @Path("/j_heat_map_calc/{streamed}")
    public Response calcHeatMap(@QueryParam("streamed") String streamed)

    {
        TDBManager tdb=null;

        double ref_value=0.0;
        File fileout=null;

        try {

            String legend;
            int id_ids;
            ProcessBuilder builder=null;
            ByteArrayOutputStream is;
            Process process=null;
            StreamGobbler streamGobbler;


            int exitCode;

            System.out.println("J_HEAT_MAP - start procedure");


            tdb = new TDBManager("jdbc/ssdb");
            String sqlString=null;


            builder = new ProcessBuilder();
            builder.redirectErrorStream(true);  //Redirect error on stdout

            builder.command("rm","-f",System.getProperty("java.io.tmpdir")+"/testout.png");  //TODO: to change with other name when procedure development will be completed

            System.out.println("J_HEAT_MAP - deleting old files");
            process = builder.start();

            streamGobbler =
                    new StreamGobbler(process.getInputStream(), System.out::println);
            Executors.newSingleThreadExecutor().submit(streamGobbler);

            exitCode = process.waitFor();
            assert exitCode == 0;

            System.out.println("J_HEAT_MAP - perform query");
            sqlString = "select * from postgis.heat_map_calc()";

            tdb.setPreparedStatementRef(sqlString);

            tdb.runPreparedQuery();

            if (tdb.next()) {

                System.out.println("J_HEAT_MAP - change owner");
                builder.command("chown","tomcat7:tomcat7",System.getProperty("java.io.tmpdir")+"/testout.png");  //TODO: to change with other name when procedure development will be completed

                process = builder.start();

                streamGobbler =
                        new StreamGobbler(process.getInputStream(), System.out::println);
                Executors.newSingleThreadExecutor().submit(streamGobbler);

                exitCode = process.waitFor();
                assert exitCode == 0;



                System.out.println("J_HEAT_MAP - Reading file from "+System.getProperty("java.io.tmpdir"));
                fileout = new File(System.getProperty("java.io.tmpdir") + "/testout.png"); //TODO: change filename




            }

            System.out.println("J_HEAT_MAP - closing connection");
            tdb.closeConnection();


        }catch(Exception e){
            System.out.println("Error  : "+e.getMessage());


            try{
                System.out.println("J_HEAT_MAP - closing connection");
                tdb.closeConnection();
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
            }

            return Response.status(500).entity(e.getMessage()).build();
        }

        Response.ResponseBuilder responseBuilder = Response.ok((Object)fileout);
        responseBuilder.header("Content-Disposition", "attachment; filename=\"heat_map.png\"");

        return responseBuilder.build();

    }

    /**
     * calculate SPI condition on over EU for given year and month
     * @param spi_type - index to be downloaded :
     *              SPI1
     *              SPI3
     *              SPI6
     *              SPI12
     *              SPI24
     * @param year
     * @param month

     * @return
     */
    @GET
    @Produces(MediaType.TEXT_PLAIN)
    @Path("/spi_conditions/eu/{spi_type}/{year}/{month}")
    public Response calcSpiEU(
                                             @PathParam("spi_type") String spi_type,
                                             @PathParam("year") String year,
                                             @PathParam("month") String month){


        TDBManager tdb=null;
        String strOut = "";

        try {

            String sqlString=null;

            logger.info("open connection");
            tdb = new TDBManager("jdbc/ssdb");

            sqlString = "select * from postgis.calc_spi_stats_over_eu( " +
                    "?,?,?,'"+SPI_RECLASS_ANALISYS+"')";



            logger.info(sqlString+" "+spi_type+" "+year+" "+month+" ");

            tdb.setPreparedStatementRef(sqlString);
            tdb.setParameter(DBManager.ParameterType.INT,month,1);
            tdb.setParameter(DBManager.ParameterType.INT,year,2);
            tdb.setParameter(DBManager.ParameterType.STRING,spi_type.toLowerCase(),3);

            tdb.runPreparedQuery();

            strOut = "Country; Classe; Numero Pixels; Percentuale\n";
            while (tdb.next()) {

                strOut = strOut + tdb.getString(1) + ";" + tdb.getDouble(2) + ";" + tdb.getInteger(3) + ";" + tdb.getDouble(4)+ "\n";

            }



            logger.info("closing connection");
            tdb.closeConnection();
        }catch(Exception e){
            logger.warning(e.getMessage());

            try{
                tdb.closeConnection();
            }catch (Exception ee){
                logger.warning(ee.getMessage());
            }

            return Response.status(500).entity(e.getMessage()).build();
        }

        Response.ResponseBuilder responseBuilder = Response.ok(strOut);

        return responseBuilder.build();
    }

    /**
     * calculate SPI condition on region by given country, month and year
     * @param spi_type - index to be downloaded :
     *              SPI1
     *              SPI3
     *              SPI6
     *              SPI12
     *              SPI24
     * @param year
     * @param month

     * @return
     */
    @GET
    @Produces(MediaType.TEXT_PLAIN)
    @Path("/spi_conditions/regions/{country_name}/{spi_type}/{year}/{month}")
    public Response extractImageFromAllBound(@PathParam("country_name") String country_name,
                                             @PathParam("spi_type") String spi_type,
                                           @PathParam("year") String year,
                                           @PathParam("month") String month){


        TDBManager tdb=null;
        String strOut = "";

        try {

            String sqlString=null;

            logger.info("open connection");
            tdb = new TDBManager("jdbc/ssdb");

            sqlString = "select * from postgis.calc_spi_stats_over_region( " +
                    "?,?,?,'"+SPI_RECLASS_ANALISYS+"',?)";

    

            logger.info(sqlString+" "+spi_type+" "+year+" "+month+" ");

            tdb.setPreparedStatementRef(sqlString);
            tdb.setParameter(DBManager.ParameterType.INT,month,1);
            tdb.setParameter(DBManager.ParameterType.INT,year,2);
            tdb.setParameter(DBManager.ParameterType.STRING,spi_type.toLowerCase(),3);
            tdb.setParameter(DBManager.ParameterType.STRING,country_name.toLowerCase(),4);

            tdb.runPreparedQuery();

            strOut = "Regione; Classe; Numero Pixels; Percentuale\n";
            while (tdb.next()) {

                strOut = strOut + tdb.getString(1) + ";" + tdb.getDouble(2) + ";" + tdb.getInteger(3) + ";" + tdb.getDouble(4)+ "\n";

            }



            logger.info("closing connection");
            tdb.closeConnection();
        }catch(Exception e){
            logger.warning(e.getMessage());

            try{
                tdb.closeConnection();
            }catch (Exception ee){
                logger.warning(ee.getMessage());
            }

            return Response.status(500).entity(e.getMessage()).build();
        }

        Response.ResponseBuilder responseBuilder = Response.ok(strOut);

        return responseBuilder.build();
    }


    /**
     * calculate SPI condition over provinces
     * @param spi_type - index to be downloaded :
     *              SPI1
     *              SPI3
     *              SPI6
     *              SPI12
     *              SPI24
     * @param year
     * @param month

     * @return
     */
    @GET
    @Produces(MediaType.TEXT_PLAIN)
    @Path("/spi_conditions/provinces/{spi_type}/{region}/{year}/{month}")
    public Response spi_cond_over_provinces(@PathParam("spi_type") String spi_type,
                                            @PathParam("region") String region,
                                             @PathParam("year") String year,
                                             @PathParam("month") String month){


        TDBManager tdb=null;
        String strOut = "";

        try {

            String sqlString=null;

            logger.info("open connection");
            tdb = new TDBManager("jdbc/ssdb");

            sqlString = "select * from postgis.calc_spi_stats_over_prov( " +
                    "?,?,?,?,'"+SPI_RECLASS_ANALISYS+"')";



            logger.info(sqlString+" "+spi_type+" "+year+" "+month+" ");

            tdb.setPreparedStatementRef(sqlString);
            tdb.setParameter(DBManager.ParameterType.INT,month,1);
            tdb.setParameter(DBManager.ParameterType.INT,year,2);
            tdb.setParameter(DBManager.ParameterType.STRING,spi_type,4);
            tdb.setParameter(DBManager.ParameterType.STRING,region.toLowerCase(),3);

            tdb.runPreparedQuery();

            strOut = "Provincia; Classe; Numero Pixels; Percentuale\n";
            while (tdb.next()) {

                strOut = strOut + tdb.getString(1) + ";" + tdb.getDouble(2) + ";" + tdb.getInteger(3) + ";" + tdb.getDouble(4)+ "\n";

            }



            logger.info("closing connection");
            tdb.closeConnection();
        }catch(Exception e){
            logger.warning(e.getMessage());

            try{
                tdb.closeConnection();
            }catch (Exception ee){
                logger.warning(ee.getMessage());
            }

            return Response.status(500).entity(e.getMessage()).build();
        }

        Response.ResponseBuilder responseBuilder = Response.ok(strOut);

        return responseBuilder.build();
    }

    /**
     * calculate SPI condition over CCI for given EU country
     * @param spi_type - index to be downloaded :
     *              SPI1
     *              SPI3
     *              SPI6
     *              SPI12
     *              SPI24
     * @param year
     * @param month

     * @return
     */
    @GET
    @Produces(MediaType.TEXT_PLAIN)
    @Path("/spi_conditions/cci/eu/{country_name}/{spi_type}/{year}/{month}/{year2}/{month2}")
    public Response spi_cond_over_cci_eu(@PathParam("country_name") String country_name,
                                            @PathParam("spi_type") String spi_type,
                                            @PathParam("year") String year,
                                            @PathParam("month") String month,
                                            @PathParam("year2") String year2,
                                            @PathParam("month2") String month2){


        TDBManager tdb=null;
        String strOut = "";

        try {

            String sqlString=null;

            logger.info("open connection");
            tdb = new TDBManager("jdbc/ssdb");

            sqlString = "select * from postgis.calc_spi_stats_over_landcover( " +
                    "?,?,?,?,?," +
                    "(select the_geom from postgis.eu_boundaries where lower(name_engl) = ?),'"+SPI_RECLASS_CCI+"')";

            logger.info(sqlString+" "+spi_type+" "+year+" "+month+" ");

            tdb.setPreparedStatementRef(sqlString);
            tdb.setParameter(DBManager.ParameterType.INT,month,1);
            tdb.setParameter(DBManager.ParameterType.INT,year,2);
            tdb.setParameter(DBManager.ParameterType.INT,month2,3);
            tdb.setParameter(DBManager.ParameterType.INT,year2,4);
            tdb.setParameter(DBManager.ParameterType.STRING,spi_type,5);
            tdb.setParameter(DBManager.ParameterType.STRING,country_name.toLowerCase(),6);

            tdb.runPreparedQuery();

            strOut = "YEAR; MONTH; CCI_CLASS; SPI_CLASS; OCCURRENCES; \n";
            while (tdb.next()) {

                strOut = strOut + tdb.getInteger(1) + ";" + tdb.getInteger(2) + ";" +
                        tdb.getInteger(3) + ";" + tdb.getDouble(4) + ";" +
                        tdb.getDouble(5) +"\n";

            }

            logger.info("closing connection");
            tdb.closeConnection();
        }catch(Exception e){
            logger.warning(e.getMessage());

            try{
                tdb.closeConnection();
            }catch (Exception ee){
                logger.warning(ee.getMessage());
            }

            return Response.status(500).entity(e.getMessage()).build();
        }

        Response.ResponseBuilder responseBuilder = Response.ok(strOut);

        return responseBuilder.build();
    }

    /**
     * calculate SPI condition over CCI for given EU country
     * @param spi_type - index to be downloaded :
     *              SPI1
     *              SPI3
     *              SPI6
     *              SPI12
     *              SPI24
     * @param year
     * @param month

     * @return
     */
    @GET
    @Produces(MediaType.TEXT_PLAIN)
    @Path("/spi_conditions/cci/regions/{region_name}/{spi_type}/{year}/{month}/{year2}/{month2}")
    public Response spi_cond_over_cci_region(@PathParam("region_name") String region_name,
                                            @PathParam("spi_type") String spi_type,
                                            @PathParam("year") String year,
                                            @PathParam("month") String month,
                                            @PathParam("year2") String year2,
                                            @PathParam("month2") String month2){


        TDBManager tdb=null;
        String strOut = "";

        try {

            String sqlString=null;

            logger.info("open connection");
            tdb = new TDBManager("jdbc/ssdb");

            sqlString = "select * from postgis.calc_spi_stats_over_landcover( " +
                    "?,?,?,?,?," +
                    "(select the_geom from postgis.reg_boundaries where lower(regione) = ?),'"+SPI_RECLASS_CCI+"')";

            logger.info(sqlString+" "+spi_type+" "+year+" "+month+" ");

            tdb.setPreparedStatementRef(sqlString);
            tdb.setParameter(DBManager.ParameterType.INT,month,1);
            tdb.setParameter(DBManager.ParameterType.INT,year,2);
            tdb.setParameter(DBManager.ParameterType.INT,month2,3);
            tdb.setParameter(DBManager.ParameterType.INT,year2,4);
            tdb.setParameter(DBManager.ParameterType.STRING,spi_type,5);
            tdb.setParameter(DBManager.ParameterType.STRING,region_name.toLowerCase(),6);

            tdb.runPreparedQuery();

            strOut = "YEAR; MONTH; CCI_CLASS; SPI_CLASS; OCCURRENCES; \n";
            while (tdb.next()) {

                strOut = strOut + tdb.getInteger(1) + ";" + tdb.getInteger(2) + ";" +
                        tdb.getInteger(3) + ";" + tdb.getDouble(4) + ";" +
                        tdb.getDouble(5) +"\n";

            }

            logger.info("closing connection");
            tdb.closeConnection();
        }catch(Exception e){
            logger.warning(e.getMessage());

            try{
                tdb.closeConnection();
            }catch (Exception ee){
                logger.warning(ee.getMessage());
            }

            return Response.status(500).entity(e.getMessage()).build();
        }

        Response.ResponseBuilder responseBuilder = Response.ok(strOut);

        return responseBuilder.build();
    }

    /**
     * calculate SPI condition over EU countries from given time interval
     * @param spi_type - index to be downloaded :
     *              SPI1
     *              SPI3
     *              SPI6
     *              SPI12
     *              SPI24
     * @param year
     * @param month

     * @return
     */
    @GET
    @Produces(MediaType.TEXT_PLAIN)
    @Path("/spi_conditions/eu/{country}/{spi_type}/{year}/{month}/{year2}/{month2}")
    public Response calcSpiEU2(@PathParam("country") String country,
                                             @PathParam("spi_type") String spi_type,
                                             @PathParam("year") String year,
                                             @PathParam("month") String month,
                                             @PathParam("year2") String year2,
                                             @PathParam("month2") String month2){


        TDBManager tdb=null;
        String strOut = "";

        try {

            String sqlString=null;

            logger.info("open connection");
            tdb = new TDBManager("jdbc/ssdb");

            sqlString = "select * from postgis.calc_spi_stats_over_eu( " +
                    "?,?,?,?,?,'"+SPI_RECLASS_ANALISYS+"',?)";



            logger.info(sqlString+" "+spi_type+" "+year+" "+month+" ");

            tdb.setPreparedStatementRef(sqlString);
            tdb.setParameter(DBManager.ParameterType.INT,month,1);
            tdb.setParameter(DBManager.ParameterType.INT,year,2);
            tdb.setParameter(DBManager.ParameterType.INT,month2,3);
            tdb.setParameter(DBManager.ParameterType.INT,year2,4);
            tdb.setParameter(DBManager.ParameterType.STRING,spi_type.toLowerCase(),5);
            tdb.setParameter(DBManager.ParameterType.STRING,country.toLowerCase(),6);

            tdb.runPreparedQuery();

            strOut = "Country; Anno; Mese; Classe; Numero Pixels; Percentuale\n";
            while (tdb.next()) {

                strOut = strOut + tdb.getString(1) +";" + tdb.getInteger(2) +
                        ";" + tdb.getInteger(3) +
                        ";" + tdb.getDouble(4) + ";" + tdb.getInteger(5) + ";" + tdb.getDouble(6)+ "\n";

            }

            logger.info("closing connection");
            tdb.closeConnection();
        }catch(Exception e){
            logger.warning(e.getMessage());

            try{
                tdb.closeConnection();
            }catch (Exception ee){
                logger.warning(ee.getMessage());
            }

            return Response.status(500).entity(e.getMessage()).build();
        }

        Response.ResponseBuilder responseBuilder = Response.ok(strOut);

        return responseBuilder.build();
    }

    /**
     * calculate SPI condition on regions in given period
     * @param spi_type - index to be downloaded :
     *              SPI1
     *              SPI3
     *              SPI6
     *              SPI12
     *              SPI24
     * @param year
     * @param month

     * @return
     */
    @GET
    @Produces(MediaType.TEXT_PLAIN)
    @Path("/spi_conditions/regions/{country_name}/{region_name}/{spi_type}/{year}/{month}/{year2}/{month2}")
    public Response extractImageFromAllBound(@PathParam("region_name") String region_name,
                                             @PathParam("country_name") String country_name,
                                             @PathParam("spi_type") String spi_type,
                                             @PathParam("year") String year,
                                             @PathParam("month") String month,
                                             @PathParam("year2") String year2,
                                             @PathParam("month2") String month2){


        TDBManager tdb=null;
        String strOut = "";

        try {

            String sqlString=null;

            logger.info("open connection");
            tdb = new TDBManager("jdbc/ssdb");

            sqlString = "select * from postgis.calc_spi_stats_over_region( " +
                    "?,?,?,?,?,'"+SPI_RECLASS_ANALISYS+"',?,?)";



            logger.info(sqlString+" "+spi_type+" "+year+" "+month+" ");

            tdb.setPreparedStatementRef(sqlString);
            tdb.setParameter(DBManager.ParameterType.INT,month,1);
            tdb.setParameter(DBManager.ParameterType.INT,year,2);
            tdb.setParameter(DBManager.ParameterType.INT,month2,3);
            tdb.setParameter(DBManager.ParameterType.INT,year2,4);
            tdb.setParameter(DBManager.ParameterType.STRING,spi_type.toLowerCase(),5);
            tdb.setParameter(DBManager.ParameterType.STRING,country_name.toLowerCase(),6);
            tdb.setParameter(DBManager.ParameterType.STRING,region_name.toLowerCase(),7);

            tdb.runPreparedQuery();

            strOut = "Regione; Anno; Mese; Classe; Numero Pixels; Percentuale\n";
            while (tdb.next()) {

                strOut = strOut + tdb.getString(1) +";" + tdb.getInteger(2) +
                        ";" + tdb.getInteger(3) +
                        ";" + tdb.getDouble(4) + ";" + tdb.getInteger(5) + ";" + tdb.getDouble(6)+ "\n";

            }

            logger.info("closing connection");
            tdb.closeConnection();
        }catch(Exception e){
            logger.warning(e.getMessage());

            try{
                tdb.closeConnection();
            }catch (Exception ee){
                logger.warning(ee.getMessage());
            }

            return Response.status(500).entity(e.getMessage()).build();
        }

        Response.ResponseBuilder responseBuilder = Response.ok(strOut);

        return responseBuilder.build();
    }


    /**
     * calculate SPI condition on regions in given period
     * @param spi_type - index to be downloaded :
     *              SPI1
     *              SPI3
     *              SPI6
     *              SPI12
     *              SPI24
     * @param year
     * @param month

     * @return
     */
    @GET
    @Produces(MediaType.TEXT_PLAIN)
    @Path("/spi_conditions/provinces/{spi_type}/{region}/{year}/{month}/{year2}/{month2}")
    public Response spi_cond_prov(@PathParam("spi_type") String spi_type,
                                  @PathParam("region") String region,
                                             @PathParam("year") String year,
                                             @PathParam("month") String month,
                                             @PathParam("year2") String year2,
                                             @PathParam("month2") String month2){


        TDBManager tdb=null;
        String strOut = "";

        try {

            String sqlString=null;

            logger.info("open connection");
            tdb = new TDBManager("jdbc/ssdb");

            sqlString = "select * from postgis.calc_spi_stats_over_prov( " +
                    "?,?,?,?,?,?,'"+SPI_RECLASS_ANALISYS+"')";



            logger.info(sqlString+" "+spi_type+" "+year+" "+month+" ");

            tdb.setPreparedStatementRef(sqlString);
            tdb.setParameter(DBManager.ParameterType.INT,month,1);
            tdb.setParameter(DBManager.ParameterType.INT,year,2);
            tdb.setParameter(DBManager.ParameterType.INT,month2,3);
            tdb.setParameter(DBManager.ParameterType.INT,year2,4);
            tdb.setParameter(DBManager.ParameterType.STRING,spi_type,6);
            tdb.setParameter(DBManager.ParameterType.STRING,region,5);

            tdb.runPreparedQuery();

            strOut = "Provincia; Anno; Mese; Classe; Numero Pixels; Percentuale\n";
            while (tdb.next()) {

                strOut = strOut + tdb.getString(1) +";" + tdb.getInteger(2) +
                        ";" + tdb.getInteger(3) +
                        ";" + tdb.getDouble(4) + ";" + tdb.getInteger(5) + ";" + tdb.getDouble(6)+ "\n";

            }

            logger.info("closing connection");
            tdb.closeConnection();
        }catch(Exception e){
            logger.warning(e.getMessage());

            try{
                tdb.closeConnection();
            }catch (Exception ee){
                logger.warning(ee.getMessage());
            }

            return Response.status(500).entity(e.getMessage()).build();
        }

        Response.ResponseBuilder responseBuilder = Response.ok(strOut);

        return responseBuilder.build();
    }


    /**
     * method for uploading file to GeoDB
     *
     * @param fileInputStream   - file input stream
     * @param contentDispositionHeader -- header
     * @param day - doy
     * @param month - month
     * @param year - year
     * @return response
     */

    @POST
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @Path("/j_ndvi_up")
    @PermitAll
    public Response upload(@FormDataParam("file") InputStream fileInputStream,
                           @FormDataParam("file") FormDataContentDisposition contentDispositionHeader,
                           @FormDataParam("day") String day,
                           @FormDataParam("month") String month,
                           @FormDataParam("year") String year){

        TDBManager tdb=null;
        String tblname="";

        try{



            tdb = new TDBManager("jdbc/ssdb");





            File file = File.createTempFile("rasterfile", ".nc");



            logger.info("Building temp file "+file.toPath()+"...");

            Files.copy(fileInputStream, file.toPath(), StandardCopyOption.REPLACE_EXISTING);


            logger.info("/usr/bin/importraster.sh" + " " + file.getAbsolutePath() + " ndvi" );

            tblname="ndvi";


            ProcessBuilder builder = new ProcessBuilder("/usr/bin/importraster.sh", "" + file.getAbsolutePath(), tblname);
            final Process process = builder.start();




            InputStream is = process.getInputStream();
            InputStreamReader isr = new InputStreamReader(is);
            BufferedReader br = new BufferedReader(isr);
            String line;

            InputStream is1 = process.getErrorStream();
            InputStreamReader isr1 = new InputStreamReader(is1);
            BufferedReader br1 = new BufferedReader(isr1);
            String line1;




            while ((line = br.readLine()) != null) {


                System.out.println(line);
            }

            while ((line1 = br1.readLine()) != null) {


                System.out.println(line1);
            }



            String sqlString;


            logger.info("update raster table...");

            if(Integer.parseInt(month) < 10){
                month = "0"+month;
            }

            if(Integer.parseInt(day) < 10){
                day = "0"+day;
            }

            sqlString = "select from postgis.import_ndvi_images('"+year+"','"+month+"','"+day+"')";

            tdb.setPreparedStatementRef(sqlString);

            tdb.runPreparedQuery();

            if(tdb.next()){
                System.out.println("done.");
            }


            logger.info("deleting tem file...");
            file.delete();
            logger.info("done.");




            return Response.status(200).entity("Image saved!").build();

        }catch(Exception e){
            logger.warning(e.getMessage());

            try {
                assert tdb != null;
                tdb.setPreparedStatementRef("drop table "+tblname);
                tdb.performInsert();
            } catch (SQLException e1) {
                e1.printStackTrace();
            }

            return Response.status(500).entity(e.getMessage()).build();
        }finally {
            try{
                assert tdb != null;
                tdb.closeConnection();
            }catch (Exception e){
                logger.warning(e.getMessage());
            }
        }
    }



    private static class StreamGobbler implements Runnable {
        private InputStream inputStream;
        private Consumer<String> consumer;

        public StreamGobbler(InputStream inputStream, Consumer<String> consumer) {
            this.inputStream = inputStream;
            this.consumer = consumer;
        }

        @Override
        public void run() {
            new BufferedReader(new InputStreamReader(inputStream)).lines()
                    .forEach(consumer);
        }
    }


}
