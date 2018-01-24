package it.cnr.ibimet.dgws;

import it.cnr.ibimet.dbutils.SWH4EConst;
import it.cnr.ibimet.dbutils.TDBManager;

import javax.ws.rs.*;

import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;

import javax.ws.rs.core.Application;
import javax.ws.rs.core.Response;
import java.io.InputStream;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;


/**
 * Created by lerocchi on 03/07/17.
 *
 * Web Services for organizing new data into GeoDB
 * its methods are called when new image are imported into GeoDB in order to create all metadata and logic link between tables
 */
@Path("/organize")
public class OrganizeRaster extends Application implements SWH4EConst {



    @POST
    @Path("/j_raster_save")
    public Response rasterSave(@FormDataParam("table_name") String tname,
                               @FormDataParam("table_temp") String ttemp,
                               @FormDataParam("year") String year,
                               @FormDataParam("dayofyear") String doy){
        TDBManager tdb=null;



        try {
            tdb = new TDBManager("jdbc/ssdb");
            String sqlString=null;



            //Save new image into related spatial table and put new timestamp in acquisition table

            sqlString=" select from postgis.import_lst_images("+year+","+year+","+doy+","+doy+")";

            System.out.println("SQL: "+sqlString);

            tdb.setPreparedStatementRef(sqlString);

            tdb.runPreparedQuery();

            if (tdb.next()) {
                //new image saved. now deleting temporary table...
                System.out.println("new image saved. now deleting temporary table...");

                sqlString=" select from postgis.clean_temp_lst_tables("+year+","+year+","+doy+","+doy+")";

                tdb.setPreparedStatementRef(sqlString);

                tdb.runPreparedQuery();

                if (tdb.next()){
                    System.out.println("ok");
                }else{


                    return Response.status(500).entity("Error during import procedure!").build();

                }
            }else{
                sqlString=" select from postgis.clean_temp_lst_tables("+year+","+year+","+doy+","+doy+")";

                tdb.setPreparedStatementRef(sqlString);

                tdb.runPreparedQuery();
                return Response.status(500).entity("Error during import procedure!").build();

            }


        }catch(SQLException sqle){
            System.out.println("Error  : "+sqle.getMessage());


            try{

                tdb.setPreparedStatementRef(" select from postgis.clean_temp_lst_tables("+year+","+year+","+doy+","+doy+")");

                tdb.runPreparedQuery();
                tdb.closeConnection();
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
                try{


                    tdb.closeConnection();
                }catch (Exception eee){
                    System.out.println("Error "+ee.getMessage());
                }
            }

            return Response.status(500).entity("Error during import procedure!").build();

        }catch(Exception e){
            System.out.println("Error  : "+e.getMessage());


            try{


                tdb.closeConnection();
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
            }

            return Response.status(500).entity("Error during import procedure!").build();

        }finally {
            {
                try{
                    tdb.closeConnection();
                }catch (Exception ee){
                    System.out.println("Error "+ee.getMessage());
                }

            }
        }


        return Response.status(200).entity("Image saved!").build();


    }


    @POST
    @Path("/j_organize_raster/{table_name}/{year}/{doy}")
    public Response extractWholeTiffDMY(@PathParam("table_name") String table_name,
                                        @PathParam("year") String year,
                                        @PathParam("doy") String doy){

        TDBManager tdb=null;



        try {
            tdb = new TDBManager("jdbc/ssdb");
            String sqlString=null;



            //Save new image into related spatial table and put new timestamp in acquisition table

            sqlString=" select from postgis.import_"+table_name+"_images("+year+","+year+","+doy+","+doy+")";

            System.out.println("SQL: "+sqlString);

            tdb.setPreparedStatementRef(sqlString);

            tdb.runPreparedQuery();

            if (tdb.next()) {
                //new image saved. now deleting temporary table...
                System.out.println("new image saved. now deleting temporary table...");

                sqlString=" select from postgis.clean_temp_"+table_name+"_tables("+year+","+year+","+doy+","+doy+")";

                tdb.setPreparedStatementRef(sqlString);

                tdb.runPreparedQuery();

                if (tdb.next()){
                    System.out.println("ok");
                }else{


                    return Response.status(500).entity("Error during import procedure!").build();

                }
            }else{
                sqlString=" select from postgis.clean_temp_"+table_name+"_tables("+year+","+year+","+doy+","+doy+")";

                tdb.setPreparedStatementRef(sqlString);

                tdb.runPreparedQuery();
                return Response.status(500).entity("Error during import procedure!").build();

            }


        }catch(SQLException sqle){
            System.out.println("Error  : "+sqle.getMessage());


            try{

                tdb.setPreparedStatementRef(" select from postgis.clean_temp_"+table_name+"_tables("+year+","+year+","+doy+","+doy+")");

                tdb.runPreparedQuery();
                tdb.closeConnection();
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
                try{


                    tdb.closeConnection();
                }catch (Exception eee){
                    System.out.println("Error "+ee.getMessage());
                }
            }

            return Response.status(500).entity("Error during import procedure!").build();

        }catch(Exception e){
            System.out.println("Error  : "+e.getMessage());


            try{


                tdb.closeConnection();
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
            }

            return Response.status(500).entity("Error during import procedure!").build();

        }finally {
            {
                try{
                    tdb.closeConnection();
                }catch (Exception ee){
                    System.out.println("Error "+ee.getMessage());
                }

            }
        }


        return Response.status(200).entity("Image saved!").build();

    }


    @POST
    @Path("/j_update_spi/{step}/{srid}/{minx}/{miny}/{maxx}/{maxy}")
    public Response updateSPI(@PathParam("step") String step,
                              @PathParam("minx") String minx,
                              @PathParam("miny") String miny,
                              @PathParam("maxx") String maxx,
                              @PathParam("maxy") String maxy,
                              @PathParam("srid") String srid){

        TDBManager tdb=null;
        int width, height;
        int width_parc, height_parc;
        int imgt=7;
        int i=1;
        int n_threads=-1;
        int id_is=-1;
        String polygon = "POLYGON(("+
                        minx+" "+miny+","+
                        minx+" "+maxy+","+
                        maxx+" "+maxy+","+
                        maxx+" "+miny+","+
                        minx+" "+miny+"))";

        Long resultFuture = new Long(0);

        System.out.println("Polygon: "+polygon);
        //TODO: da migliorare
  //      FutureTask futureTask_1,futureTask_2,futureTask_3,futureTask_4,futureTask_5,futureTask_6,futureTask_7,futureTask_8,futureTask_9,futureTask_10;

        try {

            Timestamp timestamp = new Timestamp(System.currentTimeMillis());
            System.out.println("SPI calculation, starting time: "+timestamp);

            tdb = new TDBManager("jdbc/ssdb");
            String sqlString=null;

            if(step.matches("3")){
                imgt=7;
            }else if(step.matches("6")){
                imgt=8;
            }else if(step.matches("12")){
                imgt=9;
            }

            //get the number of tiles and related threads
            sqlString = "select count(*), id_acquisizione " +
                    "   from   postgis.spi" + step + " " +
                    "   where  id_acquisizione = ( select min(id_acquisizione) from postgis.acquisizioni " +
                    "                              where id_imgtype = "+imgt+" ) "+
                    "   and    ST_Intersects(rast, ST_GeomFromText('"+ polygon + "',"+srid+")) "+
                    "   group by 2";

      //      System.out.println("SQL: "+ sqlString);

            tdb.setPreparedStatementRef(sqlString);

            tdb.runPreparedQuery();

            if(tdb.next()){
                n_threads = tdb.getInteger(1);
                id_is = tdb.getInteger(2);
                System.out.println("n_threads: "+n_threads+ " ids: "+id_is);
            }else{

                System.out.println("Nothing was found");

                throw new Exception();
            }
            //get overall extent of dataset and ul coordinates



            sqlString=" select ST_Width(rast), ST_Height(rast), ST_UpperLeftX(rast), ST_UpperLeftY(rast) " +
                    "   from   postgis.spi" + step +
                    "   where  id_acquisizione = "+id_is;


          //  System.out.println("Get overall extent - SQL: "+sqlString);

            tdb.setPreparedStatementRef(sqlString);

            tdb.runPreparedQuery();

            //Launch parallel instances for SPI calculation








            // Create a new ExecutorService with 10 thread to execute and store the Futures. Each one represent one spi thread
            ExecutorService executor = Executors.newFixedThreadPool(n_threads);
            List<FutureTask> taskList = new ArrayList<FutureTask>();
            while (tdb.next()) {


                FutureTask futureTask_n = new FutureTask(new SPIEngineCallable("Thread-"+i,step,tdb.getDouble(3),tdb.getDouble(4),tdb.getInteger(1),tdb.getInteger(2)));

                taskList.add(futureTask_n);

                executor.execute(futureTask_n);

                i++;


            }

            // Wait until all results are available and combine them at the same time

            for (FutureTask futureTask : taskList) {
                resultFuture += (Long)futureTask.get();
            }

            System.out.println("Parallel threads have been finished");

            timestamp = new Timestamp(System.currentTimeMillis());
            System.out.println("ending time: "+timestamp);
            // Shutdown the ExecutorService

            executor.shutdown();

        }catch(SQLException sqle){
            System.out.println("Error  : "+sqle.getMessage());


            try{


                tdb.closeConnection();
            }catch (Exception ee){
                System.out.println("Error ee "+ee.getMessage());
                try{


                    tdb.closeConnection();
                }catch (Exception eee){
                    System.out.println("Error eee "+eee.getMessage());
                }
            }

            return Response.status(500).entity("Error during import procedure!").build();

        }catch(Exception e){
            System.out.println("Error  : "+e.getMessage());


            try{


                tdb.closeConnection();
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
            }

            return Response.status(500).entity("Error during import procedure!").build();

        } finally{
            try{


                tdb.closeConnection();
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
            }

        }



        return Response.status(200).entity("SPI data updated!").build();

    }

}
