package it.cnr.ibimet.dgws;

import it.cnr.ibimet.dbutils.SWH4EConst;
import it.cnr.ibimet.dbutils.TDBManager;

import javax.ws.rs.*;

import it.cnr.ibimet.restutil.HttpURLManager;
import it.lr.libs.DBManager;
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import javax.ws.rs.core.Application;
import javax.ws.rs.core.Response;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.*;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;




/**
 * Created by lerocchi on 03/07/17.
 *
 * Web Services for organizing new data into GeoDB
 * its methods are called when new image are imported into GeoDB in order to create all metadata and logic link between tables
 */
@Path("/organize")
public class OrganizeRaster extends Application implements SWH4EConst {


    public static final String TMP_DIR = System.getProperty("java.io.tmpdir");

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
    @Path("/j_organize_lst/{year}/{doy}/{tile_ref}")
    public Response saveLST( @PathParam("year") String year,
                                        @PathParam("doy") String doy,
                             @PathParam("tile_ref") String tile_ref){

        TDBManager tdb=null;



        try {
            tdb = new TDBManager("jdbc/ssdb");
            String sqlString=null;



            //Save new image into related spatial table and put new timestamp in acquisition table

            sqlString=" select * from postgis.import_lst_image("+year+","+doy+")";

            System.out.println("SQL: "+sqlString);

            tdb.setPreparedStatementRef(sqlString);

            tdb.runPreparedQuery();

            if (tdb.next()) {
                if(tdb.getInteger(1) != -1){
                    //new image saved. now deleting temporary table...
                    System.out.println("new image saved ("+tdb.getInteger(1)+")");

                    sqlString="insert into postgis.tile_references (id_acquisizione, tile_ref) " +
                            "values " +
                            "("+tdb.getInteger(1)+",'"+tile_ref+"')";

                    System.out.print("insert tile ref...");
                    tdb.setPreparedStatementRef(sqlString);
                    tdb.performInsert();

                    System.out.println("ok");
                }


                System.out.print("cleaning temp table...");
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
                System.out.print("closing connection...");
                tdb.closeConnection();
                System.out.println("done");
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
                try{


                    System.out.print("closing connection...");
                    tdb.closeConnection();
                    System.out.println("done");
                }catch (Exception eee){
                    System.out.println("Error "+ee.getMessage());
                }
            }

            return Response.status(500).entity("Error during import procedure!").build();

        }catch(Exception e){
            System.out.println("Error  : "+e.getMessage());


            try{


                System.out.print("closing connection...");
                tdb.closeConnection();
                System.out.println("done");
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
            }

            return Response.status(500).entity("Error during import procedure!").build();

        }finally {
            {
                try{
                    System.out.print("closing connection...");
                    tdb.closeConnection();
                    System.out.println("done");
                }catch (Exception ee){
                    System.out.println("Error "+ee.getMessage());
                }

            }
        }


        return Response.status(200).entity("Image saved!").build();

    }


    @POST
    @Path("/j_organize_evi/{year}/{doy}/{tile_ref}")
    public Response saveEVI( @PathParam("year") String year,
                              @PathParam("doy") String doy,
                              @PathParam("tile_ref") String tile_ref){

        TDBManager tdb=null;



        try {
            tdb = new TDBManager("jdbc/ssdb");
            String sqlString=null;



            //Save new image into related spatial table and put new timestamp in acquisition table

            sqlString=" select * from postgis.import_evi_image("+year+","+doy+")";

            System.out.println("SQL: "+sqlString);

            tdb.setPreparedStatementRef(sqlString);

            tdb.runPreparedQuery();

            if (tdb.next()) {
                if(tdb.getInteger(1) != -1){
                    //new image saved. now deleting temporary table...
                    System.out.println("new image saved ("+tdb.getInteger(1)+")");

                    sqlString="insert into postgis.tile_references (id_acquisizione, tile_ref) " +
                            "values " +
                            "("+tdb.getInteger(1)+",'"+tile_ref+"')";

                    System.out.print("insert tile ref...");
                    tdb.setPreparedStatementRef(sqlString);
                    tdb.performInsert();

                    System.out.println("ok");
                }


                System.out.print("cleaning temp table...");
                sqlString=" select from postgis.clean_temp_evi_tables("+year+","+year+","+doy+","+doy+")";

                tdb.setPreparedStatementRef(sqlString);

                tdb.runPreparedQuery();

                if (tdb.next()){
                    System.out.println("ok");
                }else{


                    return Response.status(500).entity("Error during import procedure!").build();

                }
            }else{
                sqlString=" select from postgis.clean_temp_evi_tables("+year+","+year+","+doy+","+doy+")";

                tdb.setPreparedStatementRef(sqlString);

                tdb.runPreparedQuery();
                return Response.status(500).entity("Error during import procedure!").build();

            }


        }catch(SQLException sqle){
            System.out.println("Error  : "+sqle.getMessage());


            try{

                tdb.setPreparedStatementRef(" select from postgis.clean_temp_evi_tables("+year+","+year+","+doy+","+doy+")");

                tdb.runPreparedQuery();
                System.out.print("closing connection...");
                tdb.closeConnection();
                System.out.println("done");
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
                try{


                    System.out.print("closing connection...");
                    tdb.closeConnection();
                    System.out.println("done");
                }catch (Exception eee){
                    System.out.println("Error "+ee.getMessage());
                }
            }

            return Response.status(500).entity("Error during import procedure!").build();

        }catch(Exception e){
            System.out.println("Error  : "+e.getMessage());


            try{


                System.out.print("closing connection...");
                tdb.closeConnection();
                System.out.println("done");
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
            }

            return Response.status(500).entity("Error during import procedure!").build();

        }finally {
            {
                try{
                    System.out.print("closing connection...");
                    tdb.closeConnection();
                    System.out.println("done");
                }catch (Exception ee){
                    System.out.println("Error "+ee.getMessage());
                }

            }
        }


        return Response.status(200).entity("Image saved!").build();

    }



    @POST
    @Path("/j_organize_ndvi/{year}/{doy}/{tile_ref}")
    public Response saveNDVI( @PathParam("year") String year,
                             @PathParam("doy") String doy,
                             @PathParam("tile_ref") String tile_ref){

        TDBManager tdb=null;



        try {
            tdb = new TDBManager("jdbc/ssdb");
            String sqlString=null;



            //Save new image into related spatial table and put new timestamp in acquisition table

            sqlString=" select * from postgis.import_ndvi_image("+year+","+doy+")";

            System.out.println("SQL: "+sqlString);

            tdb.setPreparedStatementRef(sqlString);

            tdb.runPreparedQuery();

            if (tdb.next()) {
                if(tdb.getInteger(1) != -1){
                    //new image saved. now deleting temporary table...
                    System.out.println("new image saved ("+tdb.getInteger(1)+")");

                    sqlString="insert into postgis.tile_references (id_acquisizione, tile_ref) " +
                            "values " +
                            "("+tdb.getInteger(1)+",'"+tile_ref+"')";

                    System.out.print("insert tile ref...");
                    tdb.setPreparedStatementRef(sqlString);
                    tdb.performInsert();

                    System.out.println("ok");
                }


                System.out.print("cleaning temp table...");
                sqlString=" select from postgis.clean_temp_ndvi_tables("+year+","+year+","+doy+","+doy+")";

                tdb.setPreparedStatementRef(sqlString);

                tdb.runPreparedQuery();

                if (tdb.next()){
                    System.out.println("ok");
                }else{


                    return Response.status(500).entity("Error during import procedure!").build();

                }
            }else{
                sqlString=" select from postgis.clean_temp_ndvi_tables("+year+","+year+","+doy+","+doy+")";

                tdb.setPreparedStatementRef(sqlString);

                tdb.runPreparedQuery();
                return Response.status(500).entity("Error during import procedure!").build();

            }


        }catch(SQLException sqle){
            System.out.println("Error  : "+sqle.getMessage());


            try{

                tdb.setPreparedStatementRef(" select from postgis.clean_temp_ndvi_tables("+year+","+year+","+doy+","+doy+")");

                tdb.runPreparedQuery();
                System.out.print("closing connection...");
                tdb.closeConnection();
                System.out.println("done");
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
                try{


                    System.out.print("closing connection...");
                    tdb.closeConnection();
                    System.out.println("done");
                }catch (Exception eee){
                    System.out.println("Error "+ee.getMessage());
                }
            }

            return Response.status(500).entity("Error during import procedure!").build();

        }catch(Exception e){
            System.out.println("Error  : "+e.getMessage());


            try{


                System.out.print("closing connection...");
                tdb.closeConnection();
                System.out.println("done");
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
            }

            return Response.status(500).entity("Error during import procedure!").build();

        }finally {
            {
                try{
                    System.out.print("closing connection...");
                    tdb.closeConnection();
                    System.out.println("done");
                }catch (Exception ee){
                    System.out.println("Error "+ee.getMessage());
                }

            }
        }


        return Response.status(200).entity("Image saved!").build();

    }




    @GET
    @Path("/j_update_spi/{step}/{nthread}/{rncore}")
    public Response updateSPI(@PathParam("step") String step,
                              @PathParam("nthread") int nthread,
                              @PathParam("rncore") int rncore){


        System.out.println("SPI"+step+" calculation");
        Timestamp timestamp=null;
        ExecutorService executor=null;
        TDBManager tdb=null;
    //    int width, height;
   //     int width_parc, height_parc;
  //      int imgt=7;
        int i=1;

//        int id_is=-1;

        Long resultFuture = new Long(0);

        try {



            timestamp = new Timestamp(System.currentTimeMillis());
            System.out.println("SPI"+step+" calculation, starting time: "+timestamp);

            tdb = new TDBManager("jdbc/ssdb");
            String sqlString=null;




            sqlString="select x0, y0, x1, y1, w_part_out, h_part_out, scalex_out, scaley_out from  postgis.prepare_spi_metadata("+nthread+")";


            //  System.out.println("Get overall extent - SQL: "+sqlString);

            tdb.setPreparedStatementRef(sqlString);

            tdb.runPreparedQuery();

            //Launch parallel instances for SPI calculation

            // Create a new ExecutorService with 10 thread to execute and store the Futures. Each one represent one spi thread
            executor = Executors.newFixedThreadPool(nthread);

 //           executor = Executors.newCachedThreadPool();

            List<FutureTask> taskList = new ArrayList<FutureTask>();
            while (tdb.next()) {

                System.out.println("SPI"+step+" init instance n. "+i);
                FutureTask futureTask_n = new FutureTask(new SPIEngineCallable("SPI"+step+"-Thread-"+i,step,
                        tdb.getDouble(1),tdb.getDouble(2),tdb.getDouble(3),
                        tdb.getDouble(4),tdb.getInteger(5),tdb.getInteger(6),
                        tdb.getDouble(7),tdb.getDouble(8),rncore));

                taskList.add(futureTask_n);

                executor.execute(futureTask_n);

                i++;


            }

            // Wait until all results are available and combine them at the same time

            System.out.println("SPI"+step+" colosing connection...");
            tdb.closeConnection();


            for (FutureTask futureTask : taskList) {
                resultFuture += (Long)futureTask.get();

            }

            System.out.println("Parallel threads have been finished");

            timestamp = new Timestamp(System.currentTimeMillis());
            System.out.println("ending time: "+timestamp);
            // Shutdown the ExecutorService
            System.out.println("Shutting down executor");
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);


            //Save calculated data into spi table
            System.out.println("SPI"+step+" re-opening connection...");
            tdb = new TDBManager("jdbc/ssdb");

            System.out.println("SPI"+step+": deleting old SPI images");
            sqlString="delete from postgis.spi"+step;
            tdb.setPreparedStatementRef(sqlString);
         //   tdb.performInsert();
            if(!tdb.performInsert()){
                System.out.println("SPI"+step+": saving spi data...");

                sqlString="select * from postgis.save_spi_data2("+step+")";
                tdb.setPreparedStatementRef(sqlString);
                tdb.runPreparedQuery();
                if(tdb.next()){
                    System.out.println("SPI"+step+": images saved!");

                    System.out.print("SPI"+step+" cleaning old spitemp data...");
                    sqlString="delete from postgis.spitemp";
                    tdb.setPreparedStatementRef(sqlString);
                    if(!tdb.performInsert()){
                        System.out.println("SPI"+step+" done.");
                    }else{
                        System.out.println("SPI"+step+" error occurred.");
                    }
                }else{
                    System.out.println("SPI"+step+": It was and error during SPI saving");
                }

            }else{
                System.out.println("SPI"+step+": It was and error during SPI import");
            }


            System.out.println("SPI"+step+" colosing connection...");
            tdb.closeConnection();




        }catch(SQLException sqle){
            System.out.println("Error SQL : "+sqle.getMessage());


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

        } catch (InterruptedException e) {
            executor.shutdownNow();
            return Response.status(500).entity("Executor interrupted!").build();
        }catch(Exception e){
            System.out.println("Error generico : "+e.getMessage());


            try{


                tdb.closeConnection();
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
            }

            return Response.status(500).entity("Error during import procedure!").build();

        } finally{
         //   try{


           //     tdb.closeConnection();
           // }catch (Exception ee){
          //      System.out.println("Error "+ee.getMessage());
          //  }

            if (!executor.isTerminated()) {
                System.err.println("Attempt to shutdown executer");
                System.err.println("using shutdownNow()");
            }
            executor.shutdownNow();
            System.out.println("Shutdown complete");
        }






        return Response.status(200).entity("SPI data updated!").build();

    }


    @GET
    @Path("/j_calculate_mc_vci/{nthread}/{year}/{doy}")
    public Response updateVCI2(@PathParam("nthread") int nthread,
                               @PathParam("year") int year,
                                @PathParam("doy") int doy){


        System.out.println("VCI Multithread start");



        MCProcedures mcp=null;
        int retCode = -1;
        try {




            mcp = new MCProcedures("jdbc/ssdb");


            retCode = mcp.perform_vci_calculus(nthread,year,doy);



        }catch(SQLException sqle){
            System.out.println("Error SQL : "+sqle.getMessage());


            try{

                mcp.closeConnection();

            }catch (Exception ee){
                System.out.println("Error ee "+ee.getMessage());
                try{


                    mcp.closeConnection();
                }catch (Exception eee){
                    System.out.println("Error eee "+eee.getMessage());
                }
            }

            return Response.status(500).entity("Error during import procedure!").build();

        } catch (InterruptedException e) {
            mcp.shutdownExecutor();
            return Response.status(500).entity("Executor interrupted!").build();
        }catch(Exception e){
            System.out.println("Error generico : "+e.getMessage());


            try{


                mcp.closeConnection();
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
            }

            return Response.status(500).entity("Error during import procedure!").build();

        } finally{

            if(retCode != 0) {

                mcp.shutdownExecutor();
                System.out.println("Shutdown complete");
            }
        }

        return Response.status(200).entity("VCI Multithread data updated!").build();

    }

    @GET
    @Path("/j_calculate_mc_vhi/{nthread}/{year}/{doy}")
    public Response updateVHI2(@PathParam("nthread") int nthread,
                               @PathParam("year") int year,
                               @PathParam("doy") int doy){


        System.out.println("VHI Multithread start");


        MCProcedures mcp=null;
        int retCode = -1;
        try {




            mcp = new MCProcedures("jdbc/ssdb");

            retCode = mcp.perform_vhi_calculus(nthread,year,doy);



        }catch(SQLException sqle){
            System.out.println("Error SQL : "+sqle.getMessage());


            try{


                mcp.closeConnection();
            }catch (Exception ee){
                System.out.println("Error ee "+ee.getMessage());
                try{


                    mcp.closeConnection();
                }catch (Exception eee){
                    System.out.println("Error eee "+eee.getMessage());
                }
            }

            return Response.status(500).entity("Error during import procedure!").build();

        } catch (InterruptedException e) {
            mcp.getExecutor().shutdownNow();
            return Response.status(500).entity("Executor interrupted!").build();
        }catch(Exception e){
            System.out.println("Error generico : "+e.getMessage());


            try{


                mcp.closeConnection();
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
            }

            return Response.status(500).entity("Error during import procedure!").build();

        } finally{

            if(retCode != 0) {
                mcp.shutdownExecutor();
                System.out.println("Shutdown complete");
            }
        }






        return Response.status(200).entity("VHI Multithread data updated!").build();

    }


    @GET
    @Path("/j_calculate_mc_evci/{nthread}/{year}/{doy}")
    public Response updateEVCI2(@PathParam("nthread") int nthread,
                               @PathParam("year") int year,
                               @PathParam("doy") int doy){


        System.out.println("E-VCI Multithread start");


        MCProcedures mcp=null;
        int retCode = -1;
        try {





            mcp = new MCProcedures("jdbc/ssdb");

            retCode = mcp.perform_evci_calculus(nthread,year,doy);



        }catch(SQLException sqle){
            System.out.println("Error SQL : "+sqle.getMessage());


            try{

                mcp.closeConnection();

            }catch (Exception ee){
                System.out.println("Error ee "+ee.getMessage());
                try{


                    mcp.closeConnection();
                }catch (Exception eee){
                    System.out.println("Error eee "+eee.getMessage());
                }
            }

            return Response.status(500).entity("Error during import procedure!").build();

        } catch (InterruptedException e) {
            mcp.getExecutor().shutdownNow();
            return Response.status(500).entity("Executor interrupted!").build();
        }catch(Exception e){
            System.out.println("Error generico : "+e.getMessage());


            try{

                mcp.closeConnection();

            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
            }

            return Response.status(500).entity("Error during import procedure!").build();

        } finally{

            if(retCode != 0) {
                mcp.shutdownExecutor();
            }
        }






        return Response.status(200).entity("E-VCI Multithread data updated!").build();

    }


    /**
     * Method for update EVI and NDVI images from MODIS
     *
     * @param product       (MOD13Q1)
     * @param collection    (6)
     * @param north
     * @param south
     * @param east
     * @param west
     * @param year_in
     * @param month_in
     * @param day_in
     * @param doy_in
     * @return
     */
    @GET
    @Path("/j_update_evi/{product}/{collection}/{north}/{south}/{east}/{west}/{app_key}{year_in:(/year_in/.+?)?}{month_in:(/month_in/.+?)?}{day_in:(/day_in/.+?)?}{doy_in:(/doy_in/.+?)?}")
    public Response updateEviNdvi(@PathParam("product") String product,
                               @PathParam("collection") String collection,
                               @PathParam("north") String north,
                               @PathParam("south") String south,
                               @PathParam("east") String east,
                               @PathParam("west") String west,
                                  @PathParam("app_key") String app_key,
                               @PathParam("year_in") String year_in,
                               @PathParam("month_in") String month_in,
                               @PathParam("day_in") String day_in,
                               @PathParam("doy_in") String doy_in){


        TDBManager tdb=null;
        String fileList = "",sqlString="";
        ProcessBuilder builder=null;
        HttpURLManager httpMng=new HttpURLManager();
        String year="";
        String month="";
        String day="";
        String doy="";
        Process process=null;
        DocumentBuilder db = null;
        InputSource is = null;
        Document doc=null;
        NodeList nList=null;
        StreamGobbler streamGobbler;
        int exitCode;
        List<String> arguments=null;

        System.out.println("Start");
        System.out.println("app_key: "+app_key);
        try {
            if(year_in.matches("") || year_in == null){
                tdb = new TDBManager("jdbc/ssdb");


                sqlString = "select * from postgis.calculate_last_element_2(?,?)";
                tdb.setPreparedStatementRef(sqlString);
                tdb.setParameter(DBManager.ParameterType.STRING,"NDVI",1);
                tdb.setParameter(DBManager.ParameterType.INT,""+16,2);

                tdb.runPreparedQuery();

                if(tdb.next()){
                    if(tdb.getInteger(7) == 1 && tdb.getInteger(5) < 16){



                        doy="1";
                        month="1";
                        day = "1";
                        year=""+(tdb.getInteger(8));
                    }else{
                        doy=""+(tdb.getInteger(5));
                        day=""+tdb.getInteger(6);
                        month=""+tdb.getInteger(7);

                        year=""+tdb.getInteger(8) ;
                    }


                }

            }else{
                month= month_in.split("/")[2];
                year= year_in.split("/")[2];
                day= day_in.split("/")[2];
                doy= doy_in.split("/")[2];



            }

            if(Integer.parseInt(day)<10)
                day="0"+day;

            if(Integer.parseInt(month)<10)
                month="0"+month;


            System.out.println(""+year+"-"+month+"-"+day+"  "+doy);


            httpMng.setUrl(WS_MODIS_SEARCH4FILES+"start="+year+"-"+month+"-"+day+"&stop="+year+"-"+month+"-"+day+
                    "&coordsOrTiles=tiles&north="+north+"&south="+south+"&east="+east+"&west="+west+"&product="+product+"&collection="+collection);


            db = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            is = new InputSource();
            is.setCharacterStream(new StringReader(httpMng.sendGet()));

            doc = db.parse(is);

            doc.getDocumentElement().normalize();

            System.out.println("Root element :" + doc.getDocumentElement().getNodeName());

            nList = doc.getElementsByTagName("return");

            if(nList.getLength()<1){
                System.out.println("No result");
            }else {
                fileList =  nList.item(0).getTextContent();

                for (int icount = 1; icount < nList.getLength(); icount++) {

                    fileList = fileList + "," + nList.item(icount).getTextContent();
                }
                System.out.println("IDs List: "+fileList);


                httpMng.setUrl(WS_MODIS_GETFILEURL+"fileIds="+fileList);
                is.setCharacterStream(new StringReader(httpMng.sendGet()));

                doc = db.parse(is);

                doc.getDocumentElement().normalize();

                System.out.println("Root element :" + doc.getDocumentElement().getNodeName());

                nList = doc.getElementsByTagName("return");

                if(nList.getLength()<1){
                    System.out.println("No files found");
                }else {


                    for (int icount = 0; icount < nList.getLength(); icount++) {

                        System.out.println("Elem : "+nList.item(icount).getTextContent());

                        builder = new ProcessBuilder();
                        builder.redirectErrorStream(true);  //Redirect error on stdout

                        builder.command("wget","-e", "robots=off","-O",TMP_DIR+"/prod"+icount+".hdf",nList.item(icount).getTextContent(),"--header", "Authorization: Bearer "+app_key);

                        System.out.println("Starting shell procedure");

                        process = builder.start();

                        streamGobbler =
                                new StreamGobbler(process.getInputStream(), System.out::println);
                        Executors.newSingleThreadExecutor().submit(streamGobbler);

                        exitCode = process.waitFor();
                        assert exitCode == 0;


                        //Extracting EVI
                        System.out.println("extracting EVI...");

                        builder.redirectErrorStream(true);  //Redirect error on stdout

                        builder.command("/usr/bin/import_evi.sh",year,doy,""+icount);

                        System.out.println("Starting shell procedure");

                        process = builder.start();


                        streamGobbler =
                                new StreamGobbler(process.getInputStream(), System.out::println);
                        Executors.newSingleThreadExecutor().submit(streamGobbler);

                        exitCode = process.waitFor();
                        assert exitCode == 0;

                    }

                    //Extracting EVI
                    System.out.println("saving EVI...");

                    builder.redirectErrorStream(true);  //Redirect error on stdout

                    builder.command("/usr/bin/save_evi_pgsql.sh",year,doy);

                    System.out.println("Starting shell procedure");

                    process = builder.start();


                    streamGobbler =
                            new StreamGobbler(process.getInputStream(), System.out::println);
                    Executors.newSingleThreadExecutor().submit(streamGobbler);

                    exitCode = process.waitFor();
                    assert exitCode == 0;

                    System.out.println("Calculating VCI..."+doy+"-"+year);

                    sqlString = "select  postgis.calculate_vci_simple(?, ?)";
                    tdb.setPreparedStatementRef(sqlString);
                    tdb.setParameter(DBManager.ParameterType.INT,doy,1);
                    tdb.setParameter(DBManager.ParameterType.INT,year,2);
                    tdb.runPreparedQuery();

                    if(tdb.next()){
                        System.out.println("Success.");
                    }else{
                        System.out.println("Attempt calculate VCI.");
                    }

                    System.out.println("Calculating E-VCI..."+doy+"-"+year);

                    sqlString = "select  postgis.calculate_evci_simple(?, ?)";
                    tdb.setPreparedStatementRef(sqlString);
                    tdb.setParameter(DBManager.ParameterType.INT,doy,1);
                    tdb.setParameter(DBManager.ParameterType.INT,year,2);
                    tdb.runPreparedQuery();

                    if(tdb.next()){
                        System.out.println("Success.");
                    }else{
                        System.out.println("Attempt calculate E-VCI.");
                    }
                    System.out.println("Done.");

                }
            }


        }catch(Exception e){
            System.out.println("Error  : "+e.getMessage());

            try {
                tdb.closeConnection();
            } catch (SQLException ee) {
                ee.printStackTrace();
            }

            return Response.status(500).entity("Error during import procedure!").build();

        }finally {
            try {
                tdb.closeConnection();
            } catch (SQLException e) {
                e.printStackTrace();
            }

        }

        return Response.status(200).entity("Image saved!").build();


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


    /**
     * Method for update LST images from MODIS
     * @param product     (MOD11A2)
     * @param collection  (6)
     * @param north
     * @param south
     * @param east
     * @param west
     * @param year_in
     * @param month_in
     * @param day_in
     * @param doy_in
     * @return
     */
    @GET
    @Path("/j_update_lst/{product}/{collection}/{north}/{south}/{east}/{west}/{app_key}{year_in:(/year_in/.+?)?}{month_in:(/month_in/.+?)?}{day_in:(/day_in/.+?)?}{doy_in:(/doy_in/.+?)?}")
    public Response updateLST(@PathParam("product") String product,
                                  @PathParam("collection") String collection,
                                  @PathParam("north") String north,
                                  @PathParam("south") String south,
                                  @PathParam("east") String east,
                                  @PathParam("west") String west,
                              @PathParam("app_key") String app_key,
                                  @PathParam("year_in") String year_in,
                                  @PathParam("month_in") String month_in,
                                  @PathParam("day_in") String day_in,
                                  @PathParam("doy_in") String doy_in){


        TDBManager tdb=null;
        String fileList = "",sqlString="";
        ProcessBuilder builder=null;
        HttpURLManager httpMng=new HttpURLManager();
        String year="";
        String month="";
        String day="";
        String doy="";
        Process process=null;
        DocumentBuilder db = null;
        InputSource is = null;
        Document doc=null;
        NodeList nList=null;
        StreamGobbler streamGobbler;
        int exitCode;
        List<String> arguments=null;

        System.out.println("Start");
        System.out.println("app_key: "+app_key);


        try {
            tdb = new TDBManager("jdbc/ssdb");
            if(year_in.matches("") || year_in == null){




                ///////

                sqlString = "select * from postgis.calculate_last_element_2(?,?)";
                tdb.setPreparedStatementRef(sqlString);
                tdb.setParameter(DBManager.ParameterType.STRING,"LST",1);
                tdb.setParameter(DBManager.ParameterType.INT,""+8,2);
                tdb.runPreparedQuery();

                if(tdb.next()){
                    if(tdb.getInteger(7) == 1 && tdb.getInteger(5) < 8){



                        doy="1";
                        month="1";
                        day = "1";
                        year=""+(tdb.getInteger(8));
                    }else{
                        doy=""+(tdb.getInteger(5));
                        day=""+tdb.getInteger(6);
                        month=""+tdb.getInteger(7);

                        year=""+tdb.getInteger(8) ;
                    }


                }

            }else{
                GregorianCalendar gregorianCalendar=new GregorianCalendar();

                year= year_in.split("/")[2];

                doy= doy_in.split("/")[2];

               // System.out.println(""+year+"-"+month+"-"+day+"  "+doy);

                gregorianCalendar.set(GregorianCalendar.YEAR,Integer.parseInt(year));
                gregorianCalendar.set(GregorianCalendar.DAY_OF_YEAR,Integer.parseInt(doy));

                month  = String.valueOf((gregorianCalendar.get(GregorianCalendar.MONTH) + 1));
                day    = String.valueOf(gregorianCalendar.get(GregorianCalendar.DAY_OF_MONTH));

                System.out.println("month: "+month+"  day: "+day);

            }

            if(Integer.parseInt(day)<10)
                day="0"+day;

            if(Integer.parseInt(month)<10)
                month="0"+month;


            System.out.println(""+year+"-"+month+"-"+day+"  "+doy);

            System.out.println(WS_MODIS_SEARCH4FILES+"start="+year+"-"+month+"-"+day+"&stop="+year+"-"+month+"-"+day+
                    "&coordsOrTiles=tiles&north="+north+"&south="+south+"&east="+east+"&west="+west+"&product="+product+"&collection="+collection);
            httpMng.setUrl(WS_MODIS_SEARCH4FILES+"start="+year+"-"+month+"-"+day+"&stop="+year+"-"+month+"-"+day+
                    "&coordsOrTiles=tiles&north="+north+"&south="+south+"&east="+east+"&west="+west+"&product="+product+"&collection="+collection);


            db = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            is = new InputSource();
            is.setCharacterStream(new StringReader(httpMng.sendGet()));

            doc = db.parse(is);

            doc.getDocumentElement().normalize();

            System.out.println("Root element :" + doc.getDocumentElement().getNodeName());

            nList = doc.getElementsByTagName("return");

            if(nList.getLength()<1){
                System.out.println("No result");
            }else {
                fileList =  nList.item(0).getTextContent();

                for (int icount = 1; icount < nList.getLength(); icount++) {

                    fileList = fileList + "," + nList.item(icount).getTextContent();
                }
                System.out.println("IDs List: "+fileList);


                httpMng.setUrl(WS_MODIS_GETFILEURL+"fileIds="+fileList);
                is.setCharacterStream(new StringReader(httpMng.sendGet()));

                doc = db.parse(is);

                doc.getDocumentElement().normalize();

                System.out.println("Root element :" + doc.getDocumentElement().getNodeName());

                nList = doc.getElementsByTagName("return");

                if(nList.getLength()<1){
                    System.out.println("No files found");
                }else {


                    for (int icount = 0; icount < nList.getLength(); icount++) {

                        System.out.println("Elem : "+nList.item(icount).getTextContent());

                        builder = new ProcessBuilder();
                        builder.redirectErrorStream(true);  //Redirect error on stdout

                        builder.command("wget","-O",TMP_DIR+"/prodlst"+icount+".hdf",nList.item(icount).getTextContent(),"--header", "Authorization: Bearer "+app_key);

                        System.out.println("Starting shell procedure");
                        process = builder.start();

                        streamGobbler =
                                new StreamGobbler(process.getInputStream(), System.out::println);
                        Executors.newSingleThreadExecutor().submit(streamGobbler);

                        exitCode = process.waitFor();
                        assert exitCode == 0;


                        //Extracting EVI
                        System.out.println("extracting LST...");

                        builder.redirectErrorStream(true);  //Redirect error on stdout

                        builder.command("/usr/bin/import_lst.sh",year,doy,""+icount);

                        System.out.println("Starting shell procedure");

                        process = builder.start();


                        streamGobbler =
                                new StreamGobbler(process.getInputStream(), System.out::println);
                        Executors.newSingleThreadExecutor().submit(streamGobbler);

                        exitCode = process.waitFor();
                        assert exitCode == 0;
                    }

                    System.out.println("saving LST...");

                    builder.redirectErrorStream(true);  //Redirect error on stdout

                    builder.command("/usr/bin/save_lst_pgsql.sh",year,doy);

                    System.out.println("Starting shell procedure");

                    process = builder.start();


                    streamGobbler =
                            new StreamGobbler(process.getInputStream(), System.out::println);
                    Executors.newSingleThreadExecutor().submit(streamGobbler);

                    exitCode = process.waitFor();
                    assert exitCode == 0;

                    System.out.println("Calculating TCI..."+doy+"-"+year);

                    sqlString = "select  postgis.calculate_tci(?, ?)";
                    tdb.setPreparedStatementRef(sqlString);
                    tdb.setParameter(DBManager.ParameterType.INT,doy,1);
                    tdb.setParameter(DBManager.ParameterType.INT,year,2);
                    tdb.runPreparedQuery();

                    if(tdb.next()){
                        System.out.println("Success.");
                    }else{
                        System.out.println("Attempt calculate TCI.");
                    }

                    System.out.println("Done.");

                }
            }


        }catch(Exception e){
            System.out.println("Error  : "+e.getMessage());
            try {
                tdb.closeConnection();
            } catch (SQLException ee) {
                ee.printStackTrace();
            }


            return Response.status(500).entity("Error during import procedure!").build();

        }finally {
            try {
                tdb.closeConnection();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        return Response.status(200).entity("Image saved!").build();


    }



    /**
     * Method for update VHI and E-VHI
     *
     * @param year_in
     * @param month_in
     * @param day_in
     * @param doy_in
     * @return
     */
    @GET
    @Path("/j_update_vhi/{typ}/{nthreads}{year_in:(/year_in/.+?)?}{month_in:(/month_in/.+?)?}{day_in:(/day_in/.+?)?}{doy_in:(/doy_in/.+?)?}")
    public Response updateVHI(@PathParam("typ") String typ,
                              @PathParam("nthreads") int nthreads,
                              @PathParam("year_in") String year_in,
                                  @PathParam("month_in") String month_in,
                                  @PathParam("day_in") String day_in,
                                  @PathParam("doy_in") String doy_in){


        TDBManager tdb=null;

        String year="";
        String month="";
        String day="";
        String doy="";
        String sqlString ="";
        MCProcedures mcp= null;


        int retCode = -1;
        List<String> arguments=null;


        System.out.println("Start");
        try {
            tdb = new TDBManager("jdbc/ssdb");
            System.out.println("Updating vhi... "+typ);
            if(year_in.matches("") || year_in == null){
                tdb = new TDBManager("jdbc/ssdb");


                sqlString = "select * from postgis.calculate_last_element_2(?,?)";
                tdb.setPreparedStatementRef(sqlString);
                tdb.setParameter(DBManager.ParameterType.STRING,"VHI",1);
                tdb.setParameter(DBManager.ParameterType.INT,""+16,2);

                tdb.runPreparedQuery();

                if(tdb.next()){
                    if(tdb.getInteger(7) == 1 && tdb.getInteger(5) < 16){
                        doy="1";
                        month="1";
                        day = "1";
                        year=""+(tdb.getInteger(8));
                    }else{
                        doy=""+(tdb.getInteger(5));
                        day=""+tdb.getInteger(6);
                        month=""+tdb.getInteger(7);
                        year=""+tdb.getInteger(8) ;
                    }


                }
                System.out.println("closing connection..");
                tdb.closeConnection();
            }else{
                month= month_in.split("/")[2];
                year= year_in.split("/")[2];
                day= day_in.split("/")[2];
                doy= doy_in.split("/")[2];
            }

            if(Integer.parseInt(day)<10)
                day="0"+day;

            if(Integer.parseInt(month)<10)
                month="0"+month;



            System.out.println("Calculating VHI..."+doy+"-"+year);

            mcp = new MCProcedures("jdbc/ssdb");
            retCode=mcp.perform_vhi_calculus(nthreads,Integer.parseInt(year),Integer.parseInt(year));

            System.out.println("Done.");



        }catch (InterruptedException e) {
            mcp.shutdownExecutor();
            return Response.status(500).entity("Executor interrupted!").build();
        }catch(Exception e){
            System.out.println("Error  : "+e.getMessage());

            try {
                mcp.closeConnection();
                tdb.closeConnection();
            } catch (SQLException ee) {
                System.out.print( ""+ee.getMessage());
            } catch (Exception eee) {
                System.out.print( eee.getMessage());
            }

            return Response.status(500).entity("Error during import procedure!").build();

        }finally{

            if(retCode != 0) {
                mcp.shutdownExecutor();
                System.out.println("Shutdown complete");
            }
        }


        return Response.status(200).entity("Image saved!").build();


    }


    /**
     * Method for update CHIRPS rainfall images
     * @param year_in
     * @param month_in
     * @return
     */
    @GET
    @Path("/j_update_chirps{year_in:(/year_in/.+?)?}{month_in:(/month_in/.+?)?}")
    public Response updateCHIRPS(@PathParam("year_in") String year_in,
                                 @PathParam("month_in") String month_in){


        TDBManager tdb=null;
        String sqlString="";
        ProcessBuilder builder=null;
        int doy_count;

        Process process=null;
        StreamGobbler streamGobbler;
        int exitCode;
        String tblname;
        String year="", month="";
        int fromdy=0;



        System.out.println("Starting j_update_lst procedure...");
        try {

            tdb = new TDBManager("jdbc/ssdb");


            // System.out.println(""+year+"-"+month+"-"+day+"  "+doy);
            if(year_in.matches("") || year_in == null){

                System.out.print("Checking missing month... ");
                sqlString = "SELECT extract(month from max(dtime)), extract(year from max(dtime)) "+
                        "FROM postgis.acquisizioni "+
                        "WHERE id_imgtype = 1 ";


                tdb.setPreparedStatementRef(sqlString);

                tdb.runPreparedQuery();


                if(tdb.next()){
                    year = ""+tdb.getInteger(2);
                    month =  String.format("%02d",(tdb.getInteger(1) + 1));
                    if(month.matches("13")) {
                        year = ""+(tdb.getInteger(2)+1);
                        month = "01";
                    }
                    System.out.println(" "+month+ " "+year);
                }else {
                    try {
                        tdb.closeConnection();
                    } catch (SQLException ee) {
                        ee.printStackTrace();
                    }


                    return Response.status(500).entity("Attempt to get missing date").build();
                }



            }else {
                System.out.print("Performing custom date: ");
                month= String.format("%02d",Integer.parseInt(month_in.split("/")[2]));
                year= year_in.split("/")[2];

                System.out.println(" "+year+ " "+month);
            }




            LocalDate convertedDate = LocalDate.parse((month+"/1/"+year), DateTimeFormatter.ofPattern("MM/d/yyyy"));
            convertedDate = convertedDate.withDayOfMonth(
                    convertedDate.getMonth().length(convertedDate.isLeapYear()));




            System.out.println("downloading CHIRPS "+convertedDate.getYear()+" "+convertedDate.getMonth()+
                    " with days: "+convertedDate.getDayOfMonth()+" ...");
            builder = new ProcessBuilder();
            builder.redirectErrorStream(true);  //Redirect error on stdout

            builder.command("wget","-O",TMP_DIR+"/chirps.nc", "-c",CHIRPS_GET_RAIN + year +"."+ month + CHIRPS_GET_RAIN2);

            System.out.println("Starting shell procedure");
            process = builder.start();

            streamGobbler =
                    new StreamGobbler(process.getInputStream(), System.out::println);
            Executors.newSingleThreadExecutor().submit(streamGobbler);

            exitCode = process.waitFor();
            assert exitCode == 0;


            System.out.println("checking downloaded file...");
            File ncFile = new File(TMP_DIR+"/chirps.nc");

            if(ncFile.exists()) {

                System.out.println("new CHIRPS file exists!");


                for (int i = 1; i <= convertedDate.getDayOfMonth(); i++) {
                    System.out.println("extracting CHIRPS..." + i);

                    builder.redirectErrorStream(true);  //Redirect error on stdout

                    builder.command("/usr/bin/import_chirps.sh", "" + i);


                    process = builder.start();


                    streamGobbler =
                            new StreamGobbler(process.getInputStream(), System.out::println);
                    Executors.newSingleThreadExecutor().submit(streamGobbler);

                    exitCode = process.waitFor();
                    assert exitCode == 0;

                    //check if there are no more images


                    sqlString = "SELECT EXISTS (" +
                            "   SELECT 1" +
                            "   FROM   information_schema.tables " +
                            "   WHERE  table_schema = 'postgis' " +
                            "   AND    table_name = 'rain_flipped_warped' " +
                            "   );";
                    System.out.print("checking images on db..." + sqlString);
                    tdb.setPreparedStatementRef(sqlString);

                    tdb.runPreparedQuery();


                    if (tdb.next()) {
                        if (tdb.getBoolean(1)) {


                            System.out.println("Exists! check if exists dtime");

                            sqlString = "SELECT EXISTS (" +
                                    "   select id_acquisizione " +
                                    "   from   postgis.acquisizioni " +
                                    "   where  dtime = to_timestamp('" + year + " " + month + " " + i + "', 'YYYY MM DD')" +
                                    "   and    id_imgtype = 1);";
                            tdb.setPreparedStatementRef(sqlString);

                            tdb.runPreparedQuery();


                            if (tdb.next()) {
                                if (!tdb.getBoolean(1)) {
                                    System.out.println("Doesn't exist! create new acquisizione");

                                    sqlString = "insert into postgis.acquisizioni " +
                                            "(dtime, id_imgtype) " +
                                            "values " +
                                            "(to_timestamp('" + year + " " + month + " " + i + "', 'YYYY MM DD'),1)";

                                    System.out.println(sqlString);

                                    tdb.setPreparedStatementRef(sqlString);
                                    tdb.performInsert();

                                } else {
                                    System.out.println("Exists! use it!");

                                }
                            }


                            System.out.println("get new id_acquisizione");

                            sqlString = "select id_acquisizione " +
                                    "from postgis.acquisizioni " +
                                    "where id_imgtype = 1 " +
                                    "and extract(year from dtime)=" + year + " " +
                                    "and extract(month from dtime)=" + month + " " +
                                    "and extract(day from dtime)=" + i + " ";

                            System.out.println(sqlString);
                            tdb.setPreparedStatementRef(sqlString);
                            tdb.runPreparedQuery();

                            if (tdb.next()) {

                                System.out.println("insert new image");


                                sqlString = "insert into postgis.precipitazioni " +
                                        "(rast, id_acquisizione) " +
                                        "select rast," + tdb.getInteger(1) + " from " +
                                        "postgis.rain_flipped_warped";

                                System.out.println(sqlString);
                                tdb.setPreparedStatementRef(sqlString);

                                tdb.performInsert();

                            } else {
                                System.out.println("SOMETHING WAS WRONG: there isn't any id_acquisizione for this image");
                            }
                            sqlString = "drop table postgis.rain_flipped_warped";


                            System.out.println("erase temp table");
                            tdb.setPreparedStatementRef(sqlString);

                            tdb.performInsert();

                        }
                    }

                }

                System.out.println("deleting CHIRPS...");

                builder.redirectErrorStream(true);  //Redirect error on stdout

                builder.command("rm","-f", TMP_DIR+"/chirps.nc" );

                System.out.println("Starting shell procedure");

                process = builder.start();


                streamGobbler =
                        new StreamGobbler(process.getInputStream(), System.out::println);
                Executors.newSingleThreadExecutor().submit(streamGobbler);

                exitCode = process.waitFor();
                assert exitCode == 0;
            }


            System.out.println(sqlString);
            tdb.setPreparedStatementRef(sqlString);
            tdb.performInsert();

            System.out.println("downloading preliminary CHIRPS  ...");
            builder = new ProcessBuilder();
            builder.redirectErrorStream(true);  //Redirect error on stdout

            builder.command("wget","-O",TMP_DIR+"/chirps_preliminary.nc", "-c",CHIRPS_GET_PRELIMINARY_RAIN + year  + CHIRPS_GET_RAIN2);

            System.out.println("Starting shell procedure");
            process = builder.start();

            streamGobbler =
                    new StreamGobbler(process.getInputStream(), System.out::println);
            Executors.newSingleThreadExecutor().submit(streamGobbler);

            exitCode = process.waitFor();
            assert exitCode == 0;


            System.out.println("Get last CHIRPS doy");

            sqlString = "SELECT extract(doy from (max(dtime) + interval '1' day)) FROM postgis.acquisizioni where id_imgtype = 1";
            System.out.println(sqlString);
            tdb.setPreparedStatementRef(sqlString);
            tdb.runPreparedQuery();

            if(tdb.next()){
                fromdy = tdb.getInteger(1);
            }



            System.out.println("Deleting pre_rains that will not be used");
            sqlString = "DELETE FROM postgis.pre_rains";
            tdb.setPreparedStatementRef(sqlString);
            tdb.performInsert();
            sqlString = "DELETE FROM postgis.acquisizioni where id_imgtype = (select id_imgtype from postgis.imgtypes where imgtype = 'PRAIN')";
            tdb.setPreparedStatementRef(sqlString);
            tdb.performInsert();




            for(int i=fromdy; i<=366; i++){

                System.out.println("extracting CHIRPS..."+i);

                builder.redirectErrorStream(true);  //Redirect error on stdout

                builder.command("/usr/bin/import_preliminary_chirps.sh",""+i);



                process = builder.start();


                streamGobbler =
                        new StreamGobbler(process.getInputStream(), System.out::println);
                Executors.newSingleThreadExecutor().submit(streamGobbler);

                exitCode = process.waitFor();
                assert exitCode == 0;

                //check if there are no more images


                sqlString = "SELECT EXISTS (" +
                        "   SELECT 1" +
                        "   FROM   information_schema.tables " +
                        "   WHERE  table_schema = 'postgis' " +
                        "   AND    table_name = 'prel_rain_flipped_warped' " +
                        "   );";
                System.out.print("checking images on db..."+sqlString );
                tdb.setPreparedStatementRef(sqlString);

                tdb.runPreparedQuery();


                if(tdb.next()){
                    if(tdb.getBoolean(1)){



                        System.out.println("Exists! check if exists dtime");

                        sqlString = "SELECT EXISTS (" +
                                "   select id_acquisizione " +
                                "   from   postgis.acquisizioni "+
                                "   where  dtime = to_timestamp('"+year+"  "+i+"', 'YYYY DDD')" +
                                "   and    id_imgtype = 18);";
                        tdb.setPreparedStatementRef(sqlString);

                        tdb.runPreparedQuery();



                        if(tdb.next()) {
                            if (!tdb.getBoolean(1)) {
                                System.out.println("Doesn't exist! create new acquisizione");

                                sqlString = "insert into postgis.acquisizioni "+
                                        "(dtime, id_imgtype) "+
                                        "values "+
                                        "(to_timestamp('"+year+" "+i+"', 'YYYY DDD'),18)";

                                System.out.println(sqlString);

                                tdb.setPreparedStatementRef(sqlString);
                                tdb.performInsert();

                            }else{
                                System.out.println("Exists! use it!");

                            }
                        }


                        System.out.println("get new id_acquisizione");

                        sqlString = "select id_acquisizione "+
                                "from postgis.acquisizioni "+
                                "where id_imgtype = 18 "+
                                "and extract(year from dtime)="+year+" "+
                                "and extract(doy from dtime)="+i+" ";

                        System.out.println(sqlString);
                        tdb.setPreparedStatementRef(sqlString);
                        tdb.runPreparedQuery();

                        if(tdb.next()){

                            System.out.println("insert new image");


                            sqlString="insert into postgis.pre_rains "+
                                    "(rast, id_acquisizione) "+
                                    "select rast,"+tdb.getInteger(1)+" from "+
                                    "postgis.prel_rain_flipped_warped";

                            System.out.println(sqlString);
                            tdb.setPreparedStatementRef(sqlString);

                            tdb.performInsert();

                        }else {
                            System.out.println("SOMETHING WAS WRONG: there isn't any id_acquisizione for this image");
                        }
                        sqlString="drop table postgis.prel_rain_flipped_warped";


                        System.out.println("erase temp table");
                        tdb.setPreparedStatementRef(sqlString);

                        tdb.performInsert();

                    }
                }

            }
            System.out.println("deleting preliminary CHIRPS...");

            builder.redirectErrorStream(true);  //Redirect error on stdout

            builder.command("rm","-f", TMP_DIR+"/chirps_preliminary.nc" );

            System.out.println("Starting shell procedure");

            process = builder.start();


            streamGobbler =
                    new StreamGobbler(process.getInputStream(), System.out::println);
            Executors.newSingleThreadExecutor().submit(streamGobbler);

            exitCode = process.waitFor();
            assert exitCode == 0;





            System.out.println("DONE.");

        }catch(Exception e){
            System.out.println("Error  : "+e.getMessage());
            try {
                tdb.closeConnection();
            } catch (SQLException ee) {
                ee.printStackTrace();
            }


            return Response.status(500).entity(e.getMessage()).build();

        }finally {
            try {
                tdb.closeConnection();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        return Response.status(200).entity("Image saved!").build();


    }

    /**
     * Method for update LST images from MODIS
     * versione per la gestione a singoli tiles
     * @param product     (MOD11A2)
     * @param collection  (6)
     * @param tile_y   -  north tile
     * @param tile_x   -  west tile
     * @param year_in
     * @param month_in
     * @param day_in
     * @param doy_in
     * @return
     */
    @GET
    @Path("/j_update_lst2/{product}/{collection}/{tile_y}/{tile_x}/{skip_tci}/{app_key}{year_in:(/year_in/.+?)?}{month_in:(/month_in/.+?)?}{day_in:(/day_in/.+?)?}{doy_in:(/doy_in/.+?)?}")
    public Response updateLST2(@PathParam("product") String product,
                              @PathParam("collection") String collection,
                              @PathParam("tile_y") String tile_y,
                              @PathParam("tile_x") String tile_x,
                               @PathParam("skip_tci") String skip_tci,
                              @PathParam("app_key") String app_key,
                               @PathParam("year_in") String year_in,
                              @PathParam("month_in") String month_in,
                              @PathParam("day_in") String day_in,
                              @PathParam("doy_in") String doy_in){


        TDBManager tdb=null;
        String fileList = "",sqlString="";
        ProcessBuilder builder=null;
        HttpURLManager httpMng=new HttpURLManager();
        String year="";
        String month="";
        String day="";
        String doy="";
        Process process=null;
        DocumentBuilder db = null;
        InputSource is = null;
        Document doc=null;
        NodeList nList=null;
        StreamGobbler streamGobbler;
        int exitCode;
        List<String> arguments=null;

        System.out.println("Start");
        System.out.println("app_key: "+app_key);
        try {
            tdb = new TDBManager("jdbc/ssdb");
            if(year_in.matches("") || year_in == null){




                ///////

                sqlString = "select * from postgis.calculate_last_element_2(?,?,?)";
                tdb.setPreparedStatementRef(sqlString);
                tdb.setParameter(DBManager.ParameterType.STRING,"LST",1);
                tdb.setParameter(DBManager.ParameterType.INT,""+8,2);
                tdb.setParameter(DBManager.ParameterType.STRING,tile_y+"-"+tile_x,3);

                tdb.runPreparedQuery();

                if(tdb.next()){
                    if(tdb.getInteger(7) == 1 && tdb.getInteger(5) < 8){



                        doy="1";
                        month="1";
                        day = "1";
                        year=""+(tdb.getInteger(8));
                    }else{
                        doy=""+(tdb.getInteger(5));
                        day=""+tdb.getInteger(6);
                        month=""+tdb.getInteger(7);

                        year=""+tdb.getInteger(8) ;
                    }


                }

            }else{
                GregorianCalendar gregorianCalendar=new GregorianCalendar();

                year= year_in.split("/")[2];

                doy= doy_in.split("/")[2];

                // System.out.println(""+year+"-"+month+"-"+day+"  "+doy);

                gregorianCalendar.set(GregorianCalendar.YEAR,Integer.parseInt(year));
                gregorianCalendar.set(GregorianCalendar.DAY_OF_YEAR,Integer.parseInt(doy));

                month  = String.valueOf((gregorianCalendar.get(GregorianCalendar.MONTH) + 1));
                day    = String.valueOf(gregorianCalendar.get(GregorianCalendar.DAY_OF_MONTH));

                System.out.println("month: "+month+"  day: "+day);

            }

            if(Integer.parseInt(day)<10)
                day="0"+day;

            if(Integer.parseInt(month)<10)
                month="0"+month;


            System.out.println(""+year+"-"+month+"-"+day+"  "+doy);

            System.out.println(WS_MODIS_SEARCH4FILES+"start="+year+"-"+month+"-"+day+"&stop="+year+"-"+month+"-"+day+
                    "&coordsOrTiles=tiles&north="+tile_y+"&south="+tile_y+"&east="+tile_x+"&west="+tile_x+"&product="+product+"&collection="+collection);

            httpMng.setUrl(WS_MODIS_SEARCH4FILES+"start="+year+"-"+month+"-"+day+"&stop="+year+"-"+month+"-"+day+
                    "&coordsOrTiles=tiles&north="+tile_y+"&south="+tile_y+"&east="+tile_x+"&west="+tile_x+"&product="+product+"&collection="+collection);


            db = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            is = new InputSource();
            is.setCharacterStream(new StringReader(httpMng.sendGet()));

            doc = db.parse(is);

            doc.getDocumentElement().normalize();

            System.out.println("Root element :" + doc.getDocumentElement().getNodeName());

            nList = doc.getElementsByTagName("return");

            if(nList.getLength()<1){
                System.out.println("No result");
            }else {
                fileList =  nList.item(0).getTextContent();

                for (int icount = 1; icount < nList.getLength(); icount++) {

                    fileList = fileList + "," + nList.item(icount).getTextContent();
                }
                System.out.println("IDs List: "+fileList);


                httpMng.setUrl(WS_MODIS_GETFILEURL+"fileIds="+fileList);
                is.setCharacterStream(new StringReader(httpMng.sendGet()));

                doc = db.parse(is);

                doc.getDocumentElement().normalize();

                System.out.println("Root element :" + doc.getDocumentElement().getNodeName());

                nList = doc.getElementsByTagName("return");

                if(nList.getLength()<1){
                    System.out.println("No files found");
                }else {


                    for (int icount = 0; icount < nList.getLength(); icount++) {

                        System.out.println("Elem : "+nList.item(icount).getTextContent());

                        builder = new ProcessBuilder();
                        builder.redirectErrorStream(true);  //Redirect error on stdout

                        builder.command("wget","-O",TMP_DIR+"/prodlst"+icount+".hdf",nList.item(icount).getTextContent(),"--header", "Authorization: Bearer "+app_key);

                        System.out.println("Starting shell procedure");
                        process = builder.start();

                        streamGobbler =
                                new StreamGobbler(process.getInputStream(), System.out::println);
                        Executors.newSingleThreadExecutor().submit(streamGobbler);

                        exitCode = process.waitFor();
                        assert exitCode == 0;


                        //Extracting EVI
                        System.out.println("extracting LST...");

                        builder.redirectErrorStream(true);  //Redirect error on stdout

                        builder.command("/usr/bin/import_lst.sh",year,doy,""+icount);

                        System.out.println("Starting shell procedure");

                        process = builder.start();


                        streamGobbler =
                                new StreamGobbler(process.getInputStream(), System.out::println);
                        Executors.newSingleThreadExecutor().submit(streamGobbler);

                        exitCode = process.waitFor();
                        assert exitCode == 0;
                    }

                    System.out.println("saving LST...");

                    builder.redirectErrorStream(true);  //Redirect error on stdout

                    builder.command("/usr/bin/save_lst_pgsql.sh",year,doy,tile_y+"-"+tile_x);

                    System.out.println("Starting shell procedure");

                    process = builder.start();


                    streamGobbler =
                            new StreamGobbler(process.getInputStream(), System.out::println);
                    Executors.newSingleThreadExecutor().submit(streamGobbler);

                    exitCode = process.waitFor();
                    assert exitCode == 0;

                    if(skip_tci.matches("1")) {
                        System.out.print("Calculating TCI..." + doy + "-" + year);

                        sqlString = "select count(*) from postgis.calculate_tci(?, ?)";
                        tdb.setPreparedStatementRef(sqlString);
                        tdb.setParameter(DBManager.ParameterType.INT, doy, 1);
                        tdb.setParameter(DBManager.ParameterType.INT, year, 2);
                        tdb.runPreparedQuery();

                        if (tdb.next()) {
                            System.out.println("Success.");


                        /*    System.out.print("Updating tile reference...");
                            sqlString = "insert into postgis.tile_references(id_acquisizione, tile_ref) " +
                                    "select id_acquisizione, ? " +
                                    "from postgis.acquisizioni inner join postgis.imgtypes using (id_imgtype) " +
                                    "where extract(doy from dtime)=? " +
                                    "and   extract(year from dtime)=? " +
                                    "and   imgtype = ?";
                            tdb.setPreparedStatementRef(sqlString);
                            tdb.setParameter(DBManager.ParameterType.STRING, tile_y + "-" + tile_x, 1);
                            tdb.setParameter(DBManager.ParameterType.INT, doy, 2);
                            tdb.setParameter(DBManager.ParameterType.INT, year, 3);
                            tdb.setParameter(DBManager.ParameterType.STRING, "TCI", 4);
                            tdb.performInsert();
*/
                        } else {
                            System.out.println("Attempt calculate TCI.");
                        }

                        System.out.println("Done.");
                    }
                }
            }


        }catch(Exception e){
            System.out.println("Error  : "+e.getMessage());
            try {
                tdb.closeConnection();
            } catch (SQLException ee) {
                ee.printStackTrace();
            }


            return Response.status(500).entity("Error during import procedure!").build();

        }finally {
            try {
                tdb.closeConnection();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        return Response.status(200).entity("Image saved!").build();


    }






    /**
     * Method for update EVI and NDVI images from MODIS
     *
     * @param product       (MOD13Q1)
     * @param collection    (6)
     * @param tile_y
     * @param tile_x
     * @param skip_vci
     * @param year_in
     * @param month_in
     * @param day_in
     * @param doy_in
     * @return
     */
    @GET
    @Path("/j_update_evi2/{product}/{collection}/{tile_y}/{tile_x}/{skip_vci}/{skip_evci}/{nthreads}/{app_key}{year_in:(/year_in/.+?)?}{month_in:(/month_in/.+?)?}{day_in:(/day_in/.+?)?}{doy_in:(/doy_in/.+?)?}")
    public Response updateEviNdvi2(@PathParam("product") String product,
                                  @PathParam("collection") String collection,
                                   @PathParam("tile_y") String tile_y,
                                   @PathParam("tile_x") String tile_x,
                                   @PathParam("skip_vci") String skip_vci,
                                   @PathParam("skip_evci") String skip_evci,
                                   @PathParam("nthreads") String nthreads,
                                  @PathParam("app_key") String app_key,
                                  @PathParam("year_in") String year_in,
                                  @PathParam("month_in") String month_in,
                                  @PathParam("day_in") String day_in,
                                  @PathParam("doy_in") String doy_in){


        TDBManager tdb=null;
        String fileList = "",sqlString="";
        ProcessBuilder builder=null;
        HttpURLManager httpMng=new HttpURLManager();
        String year="";
        String month="";
        String day="";
        String doy="";
        Process process=null;
        DocumentBuilder db = null;
        InputSource is = null;
        Document doc=null;
        NodeList nList=null;
        MCProcedures mcp=null;
        StreamGobbler streamGobbler;
        int exitCode;
        List<String> arguments=null;

        int retCode=-1;
        System.out.println("J_UPDATE_EVI2:  Start");
        System.out.println("J_UPDATE_EVI2:  app_key: "+app_key);
        try {
            if(year_in.matches("") || year_in == null){
                tdb = new TDBManager("jdbc/ssdb");


                sqlString = "select * from postgis.calculate_last_element_2(?,?,?)";
                tdb.setPreparedStatementRef(sqlString);
                tdb.setParameter(DBManager.ParameterType.STRING,"NDVI",1);
                tdb.setParameter(DBManager.ParameterType.INT,""+16,2);
                tdb.setParameter(DBManager.ParameterType.STRING,tile_y+"-"+tile_x,3);


                tdb.runPreparedQuery();

                if(tdb.next()){
                    if(tdb.getInteger(7) == 1 && tdb.getInteger(5) < 16){



                        doy="1";
                        month="1";
                        day = "1";
                        year=""+(tdb.getInteger(8));
                    }else{
                        doy=""+(tdb.getInteger(5));
                        day=""+tdb.getInteger(6);
                        month=""+tdb.getInteger(7);

                        year=""+tdb.getInteger(8) ;
                    }


                }
                System.out.println("J_UPDATE_EVI2: closing connection..");
                tdb.closeConnection();

            }else{
                month= month_in.split("/")[2];
                year= year_in.split("/")[2];
                day= day_in.split("/")[2];
                doy= doy_in.split("/")[2];
            }

            if(Integer.parseInt(day)<10)
                day="0"+day;

            if(Integer.parseInt(month)<10)
                month="0"+month;


            System.out.println(""+year+"-"+month+"-"+day+"  "+doy);


            httpMng.setUrl(WS_MODIS_SEARCH4FILES+"start="+year+"-"+month+"-"+day+"&stop="+year+"-"+month+"-"+day+
                    "&coordsOrTiles=tiles&north="+tile_y+"&south="+tile_y+"&east="+tile_x+"&west="+tile_x+"&product="+product+"&collection="+collection);


            db = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            is = new InputSource();
            is.setCharacterStream(new StringReader(httpMng.sendGet()));

            doc = db.parse(is);

            doc.getDocumentElement().normalize();

            System.out.println("Root element :" + doc.getDocumentElement().getNodeName());

            nList = doc.getElementsByTagName("return");

            if(nList.getLength()<1){
                System.out.println("No result");
                retCode = 0;
            }else {
                fileList =  nList.item(0).getTextContent();

                for (int icount = 1; icount < nList.getLength(); icount++) {

                    fileList = fileList + "," + nList.item(icount).getTextContent();
                }
                System.out.println("IDs List: "+fileList);


                httpMng.setUrl(WS_MODIS_GETFILEURL+"fileIds="+fileList);
                is.setCharacterStream(new StringReader(httpMng.sendGet()));

                doc = db.parse(is);

                doc.getDocumentElement().normalize();

                System.out.println("Root element :" + doc.getDocumentElement().getNodeName());

                nList = doc.getElementsByTagName("return");

                if(nList.getLength()<1){
                    System.out.println("No files found");
                }else {


                    for (int icount = 0; icount < nList.getLength(); icount++) {

                        System.out.println("Elem : "+nList.item(icount).getTextContent());

                        builder = new ProcessBuilder();
                        builder.redirectErrorStream(true);  //Redirect error on stdout

                        builder.command("wget","-e", "robots=off","-O",TMP_DIR+"/prod"+icount+".hdf",nList.item(icount).getTextContent(),"--header", "Authorization: Bearer "+app_key);

                        System.out.println("Starting shell procedure");

                        process = builder.start();

                        streamGobbler =
                                new StreamGobbler(process.getInputStream(), System.out::println);
                        Executors.newSingleThreadExecutor().submit(streamGobbler);

                        exitCode = process.waitFor();
                        assert exitCode == 0;


                        //Extracting EVI
                        System.out.println("extracting EVI...");

                        builder.redirectErrorStream(true);  //Redirect error on stdout

                        builder.command("/usr/bin/import_evi.sh",year,doy,""+icount);

                        System.out.println("Starting shell procedure");

                        process = builder.start();


                        streamGobbler =
                                new StreamGobbler(process.getInputStream(), System.out::println);
                        Executors.newSingleThreadExecutor().submit(streamGobbler);

                        exitCode = process.waitFor();
                        assert exitCode == 0;

                    }

                    //Extracting EVI
                    System.out.println("saving EVI...");

                    builder.redirectErrorStream(true);  //Redirect error on stdout

                    builder.command("/usr/bin/save_evi_pgsql.sh",year,doy,tile_y+"-"+tile_x);

                    System.out.println("Starting shell procedure");

                    process = builder.start();


                    streamGobbler =
                            new StreamGobbler(process.getInputStream(), System.out::println);
                    Executors.newSingleThreadExecutor().submit(streamGobbler);

                    exitCode = process.waitFor();
                    assert exitCode == 0;

                    if(skip_vci.matches("1")) {



                        System.out.println("Calculating VCI..."+doy+"-"+year+" - N threads: "+nthreads);

                        mcp = new MCProcedures("jdbc/ssdb");

                        retCode = mcp.perform_vci_calculus(Integer.parseInt(nthreads),
                                Integer.parseInt(year),
                                Integer.parseInt(doy));


                        if(retCode != 0) {
                            mcp.closeConnection();
                            mcp.shutdownExecutor();
                            System.out.println("Shutdown complete");
                        }

                        if(skip_evci.matches("1")) {
                            retCode = -1;
                            retCode = mcp.perform_evci_calculus(Integer.parseInt(nthreads),
                                    Integer.parseInt(year),
                                    Integer.parseInt(doy));
                        }else{
                            retCode = 0;
                        }




                    }else{
                        retCode = 0;
                    }

                    System.out.println("Done.");

                }
            }


        }catch (InterruptedException e) {
            mcp.shutdownExecutor();
            return Response.status(500).entity("Executor interrupted!").build();
        }catch(Exception e){
            System.out.println("Error  : "+e.getMessage());

            try {
                mcp.closeConnection();
                tdb.closeConnection();
            } catch (SQLException ee) {
               System.out.print( ""+ee.getMessage());
            } catch (Exception eee) {
                System.out.print( eee.getMessage());
            }

            return Response.status(500).entity("Error during import procedure!").build();

        }finally{

            if(retCode != 0) {
                mcp.shutdownExecutor();
                System.out.println("Shutdown complete");
            }
        }

        return Response.status(200).entity("Image saved!").build();


    }



}
