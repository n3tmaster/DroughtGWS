package it.cnr.ibimet.dgws;


import it.cnr.ibimet.dbutils.ChartParams;
import it.cnr.ibimet.dbutils.SWH4EConst;
import it.cnr.ibimet.dbutils.TDBManager;
import it.cnr.ibimet.dbutils.TableSchema;
import it.lr.libs.DBManager;


import org.gdal.gdal.Dataset;
import org.gdal.gdal.gdal;
import org.gdal.ogr.DataSource;
import org.gdal.ogr.Driver;
import org.gdal.ogr.ogr;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import javax.imageio.ImageIO;
import javax.ws.rs.*;
import javax.ws.rs.core.Application;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.awt.*;
import java.awt.image.BufferedImage;
import java.awt.image.DataBufferByte;
import java.io.*;
import java.sql.SQLException;
import java.util.*;

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
public class Calculate  extends Application implements SWH4EConst{


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

            sqlString="select ST_asGDALRaster(postgis.calculate_rain_cum("+month+","+year+"),'GTiff') ";

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



    
}
