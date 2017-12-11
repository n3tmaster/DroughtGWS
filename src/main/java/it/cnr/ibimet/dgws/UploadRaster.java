package it.cnr.ibimet.dgws;

import it.cnr.ibimet.dbutils.SWH4EConst;
import it.cnr.ibimet.dbutils.TDBManager;

import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;

import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.client.SyncInvoker;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.sql.SQLException;
import java.util.GregorianCalendar;

/**
 * Created by lerocchi on 03/07/17.
 *
 * Web Services for upload data to the GeoDB
 */

@Path("/upload")
public class UploadRaster extends Application implements SWH4EConst {


    /**
     * method for uploading file to GeoDB
     *
     * @param fileInputStream
     * @param contentDispositionHeader
     * @param day
     * @param month
     * @param year
     * @return
     */

    @POST
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @Path("/j_rainfall_up")
    public Response upload(@FormDataParam("file") InputStream fileInputStream,
                           @FormDataParam("file") FormDataContentDisposition contentDispositionHeader,
                           @FormDataParam("day") String day,
                           @FormDataParam("month") String month,
                           @FormDataParam("year") String year){

        TDBManager tdb=null;
        String tblname="";

        try{



            //byte[] rasterdata = IOUtils.toByteArray(fileInputStream);




            tdb = new TDBManager("jdbc/ssdb");





            File file = File.createTempFile("rasterfile", ".tiff");



            System.out.println("Building temp file "+file.toPath()+"...");

            Files.copy(fileInputStream, file.toPath(), StandardCopyOption.REPLACE_EXISTING);

            if(Integer.parseInt(month)<10) {
                System.out.println("/usr/bin/importraster.sh" + " " + file.getAbsolutePath() + " prec_" + year + "_0" + month);

                tblname="prec_" + year + "_0" + month;
            }else{
                System.out.println("/usr/bin/importraster.sh" + " " + file.getAbsolutePath() + " prec_" + year + "_" + month);

                tblname="prec_" + year + "_" + month;
            }

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


            String sqlString ="";


            System.out.print("update raster table...");


            sqlString = "select from postgis.import_rainfall_images("+year+","+year+","+month+","+month+")";

            tdb.setPreparedStatementRef(sqlString);

            tdb.runPreparedQuery();

            if(tdb.next()){
                System.out.print("deleting temp table...");

                sqlString = "select from postgis.clean_temp_rainfall_tables("+year+","+year+","+month+","+month+")";

                tdb.setPreparedStatementRef(sqlString);

                tdb.runPreparedQuery();

                if(tdb.next()){
                    System.out.println("done.");
                }

            }


            System.out.print("deleting tem file...");
            file.delete();
            System.out.println("done.");




            return Response.status(200).entity("Image saved!").build();

        }catch(Exception e){
            System.out.println("Error  : "+e.getMessage());

            try {
                tdb.setPreparedStatementRef("drop table "+tblname);
                tdb.performInsert();
            } catch (SQLException e1) {
                e1.printStackTrace();
            }

            return Response.status(500).entity(e.getMessage()).build();
        }finally {
            try{
                tdb.closeConnection();
            }catch (Exception e){
                System.out.println("Error "+e.getMessage());
            }
        }
        
    }



    @POST
    @Path("/chirps/{year}/{attachmentName}")
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    public Response upload_big(@PathParam("year") String year,
                               @PathParam("attachmentName") String attachmentName,
                               InputStream attachmentInputStream){

        TDBManager tdb=null;

        File file = null;
        String sqlString ="";
        int doy_start=1;
        ProcessBuilder builder;
        int id_acquisizione;
        try{



            //byte[] rasterdata = IOUtils.toByteArray(fileInputStream);




            tdb = new TDBManager("jdbc/ssdb");





            file = File.createTempFile("rasterfile", ".nc");



            System.out.println("Building temp file "+file.toPath()+"...");

            Files.copy(attachmentInputStream, file.toPath(), StandardCopyOption.REPLACE_EXISTING);


            sqlString = "select max(extract(doy from dtime)) " +
                    "from postgis.acquisizioni " +
                    "where extract(year from dtime)="+year+" "+
                    "and   id_imgtype=1";

            System.out.println(sqlString);

            tdb.setPreparedStatementRef(sqlString);

            tdb.runPreparedQuery();
            if(tdb.next()){

                System.out.println("esistono gia alcune img: "+tdb.getInteger(1));
                doy_start = tdb.getInteger(1);
            }else{
                doy_start = 0;
                System.out.println("per questo anno non esiste ancora niente, inizio da zero");
            }


            for(int i=(doy_start+1); i<366; i++){
                System.out.println("/usr/bin/import_chirps.sh" + " " + file.getAbsolutePath() + " "+ i + " "+file.getParent());

                builder = new ProcessBuilder("/usr/bin/import_chirps.sh", "" + file.getAbsolutePath(), ""+i, ""+file.getParent());
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

                //check if there are no more images
                System.out.print("checking images on db..." );

                sqlString = "SELECT EXISTS (" +
                        "   SELECT 1" +
                        "   FROM   information_schema.tables " +
                        "   WHERE  table_schema = 'postgis' " +
                        "   AND    table_name = 'rain_flipped_warped' " +
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
                                "   where  dtime = to_timestamp('"+year+" "+i+"', 'YYYY DDD')" +
                                "   );";
                        tdb.setPreparedStatementRef(sqlString);

                        tdb.runPreparedQuery();



                        if(tdb.next()) {
                            if (!tdb.getBoolean(1)) {
                                System.out.println("Doesn't exist! create new acquisizione");

                                sqlString = "insert into postgis.acquisizioni "+
                                        "(dtime, id_imgtype) "+
                                        "values "+
                                        "(to_timestamp('"+year+" "+i+"', 'YYYY DDD'),1)";

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
                                "where id_imgtype = 1 "+
                                "and extract(year from dtime)="+year+" "+
                                "and extract(doy from dtime)="+i+" ";

                        System.out.println(sqlString);
                        tdb.setPreparedStatementRef(sqlString);
                        tdb.runPreparedQuery();

                        if(tdb.next()){
                            id_acquisizione = tdb.getInteger(1);

                            System.out.println("insert new image");


                            sqlString="insert into postgis.precipitazioni "+
                                    "(rast, id_acquisizione) "+
                                    "select rast,"+id_acquisizione+" from "+
                                    "postgis.rain_flipped_warped";

                            System.out.println(sqlString);
                            tdb.setPreparedStatementRef(sqlString);

                            tdb.performInsert();

                            sqlString="drop table postgis.rain_flipped_warped";


                            System.out.println("erase temp table");
                            tdb.setPreparedStatementRef(sqlString);

                            tdb.performInsert();

                        }


                    }else{
                        System.out.println("Does not exist!");
                        i=367;
                    }
                }
            }




            return Response.status(200).entity("Image saved!").build();

        }catch(Exception e){
            System.out.println("Error  : "+e.getMessage());

            try {
                tdb.setPreparedStatementRef("drop table postgis.rain_flipped_warped");
                tdb.performInsert();
            } catch (SQLException e1) {
                e1.printStackTrace();
            }

            return Response.status(500).entity(e.getMessage()).build();
        }finally {
            System.out.print("deleting temp file...");
            file.delete();
            System.out.println("done.");
            try{
                tdb.closeConnection();
            }catch (Exception e){
                System.out.println("Error "+e.getMessage());
            }
        }
    }


}
