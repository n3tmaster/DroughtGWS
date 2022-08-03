package it.cnr.ibimet.dgws;

import it.cnr.ibimet.dbutils.ErrorCode;
import it.cnr.ibimet.dbutils.SWH4EConst;
import it.cnr.ibimet.dbutils.TDBManager;
import it.cnr.ibimet.dbutils.WSExceptions;
import it.lr.libs.DBManager;

import javax.ws.rs.core.Response;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.Date;
import java.util.Locale;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.logging.Logger;

public class Procedures implements SWH4EConst, ReclassConst {
    private TDBManager tdb;
    private String sqlQuery;
    static Logger logger = Logger.getLogger(String.valueOf(Procedures.class));

    public Procedures(){


    }

    public Procedures(TDBManager tdb){
        this.tdb=tdb;


    }

    /**
     * Calculate Polygon Area in KM
     * @param polygon   - Ploygon in WKT format
     * @param srid      - SRID of gived WKT
     * @return  calculated area of the polygon in KM
     * @throws SQLException
     * @throws Exception
     */
    public double calcPolygonArea(String polygon, String srid) throws SQLException, Exception{
        double retCode = 0;

        System.out.println("Procedures: caclPolygonArea");



        tdb.setPreparedStatementRef("select (St_area(ST_GeographyFromText('SRID="+srid+";"+polygon+"'),true)/(1000000))");
        tdb.runPreparedQuery();
        if(tdb.next()){
            retCode = tdb.getDouble(1);
        }else{
            retCode = -1;
        }

        return (retCode);
    }


    /**
     * import SPI image procedure
     *
     *
     * @return retCode - 0 : ok
     *                  -1 : error
     * @throws SQLException
     */
    public void import_spi_images(TDBManager tdb, String topath, String dtime_from, String dtime_to, int step) throws SQLException, InterruptedException, IOException {

        ProcessBuilder builder=null;
        Process process=null;
        Procedures.StreamGobbler streamGobbler;
        String sqlString;
        int year_from, month_from, year_to, month_to;
        int numBands=0;
        int exitCode;
        int ids,id_imgtype;


        logger.info("work from "+dtime_from+" to "+dtime_to);

        tdb.setPreparedStatementRef("select extract(year from age('"+dtime_to+"'::timestamp,'"+dtime_from+"'::timestamp))*12 + extract(month from age('"+dtime_to+"'::timestamp,'"+dtime_to.substring(0,4)+"-01-01'::timestamp)), " +
                "extract(year from '"+dtime_from+"'::timestamp),extract(month from '"+dtime_from+"'::timestamp)," +
                "extract(year from '"+dtime_to+"'::timestamp),extract(month from '"+dtime_to+"'::timestamp), id_imgtype from postgis.imgtypes where imgtype = 'SPI"+step+"'");
        tdb.runPreparedQuery();
        if(tdb.next()){
            numBands = tdb.getInteger(1);
            year_from = tdb.getInteger(2);
            month_from = tdb.getInteger(3);
            year_to = tdb.getInteger(4);
            month_to = tdb.getInteger(5);
            id_imgtype = tdb.getInteger(6);
            logger.info("Dataset to be processed: "+numBands+" - "+year_from+" "+month_from+" - "+year_to+" "+month_to);

            builder = new ProcessBuilder();
            builder.redirectErrorStream(true);

            tdb.setPreparedStatementRef("delete from postgis.spi"+step+" where id_acquisizione IN (select id_acquisizione from postgis.acquisizioni inner join postgis.imgtypes using (id_imgtype) where imgtype = ?)");
            tdb.setParameter(DBManager.ParameterType.STRING,"SPI"+step,1);
            tdb.performInsert();

            for(int icount=1; icount <= (numBands); icount++){
                logger.info("Processing "+icount+" band");

                logger.info("run bash script");
                logger.info("/usr/bin/importspi.sh "+icount+" "+topath+" "+System.getProperty("java.io.tmpdir")+" "+step); //TODO: da togliere dopo test


                builder.command("/usr/bin/importspi.sh", ""+icount, topath,System.getProperty("java.io.tmpdir"), ""+step);

                process = builder.start();

                streamGobbler =
                        new Procedures.StreamGobbler(process.getInputStream(), System.out::println);
                Executors.newSingleThreadExecutor().submit(streamGobbler);

                exitCode = process.waitFor();
                assert exitCode == 0;

                logger.info("insert new acquisition id");

                tdb.setPreparedStatementRef("INSERT INTO postgis.acquisizioni " +
                        "(dtime,id_imgtype) " +
                        "VALUES " +
                        "((date_trunc('month','"+year_from+"-"+month_from+"-01'::timestamp) + interval '"+icount+" month - 1 day')::date," +
                        id_imgtype+") " +
                        "ON CONFLICT (dtime, id_imgtype) DO UPDATE SET resorted = EXCLUDED.resorted " +
                        "RETURNING id_acquisizione");
                tdb.runPreparedQuery();
                if(tdb.next()){
                    logger.info("IDS: "+tdb.getInteger(1));
                    ids = tdb.getInteger(1);


                    tdb.setPreparedStatementRef("INSERT INTO postgis.spi"+step+" " +
                            "(id_acquisizione, rast) " +
                            "VALUES " +
                            "("+ids+",(ST_MapAlgebra((SELECT rast FROM postgis.temp_spi"+step+"),1,(select rast from postgis.mask),1, '([rast1.val] * [rast2.val])')))");
                    tdb.performInsert();

                    logger.info("Image saved");

                    tdb.setPreparedStatementRef("DELETE FROM postgis.temp_spi"+step);
                    tdb.performInsert();

                    logger.info("temp table cleaned");

                    builder.command("/bin/rm",System.getProperty("java.io.tmpdir")+"/spi"+step+"_"+icount+".tiff" );
                    process = builder.start();

                    streamGobbler =
                            new Procedures.StreamGobbler(process.getInputStream(), System.out::println);
                    Executors.newSingleThreadExecutor().submit(streamGobbler);

                    exitCode = process.waitFor();
                    assert exitCode == 0;

                    builder.command("/bin/rm",System.getProperty("java.io.tmpdir")+"/spi"+step+"_"+icount+".nc");
                    process = builder.start();

                    streamGobbler =
                            new Procedures.StreamGobbler(process.getInputStream(), System.out::println);
                    Executors.newSingleThreadExecutor().submit(streamGobbler);

                    exitCode = process.waitFor();
                    assert exitCode == 0;

                    builder.command("/bin/rm",System.getProperty("java.io.tmpdir")+"/spi"+step+"_o_"+icount+".nc");
                    process = builder.start();

                    streamGobbler =
                            new Procedures.StreamGobbler(process.getInputStream(), System.out::println);
                    Executors.newSingleThreadExecutor().submit(streamGobbler);

                    exitCode = process.waitFor();
                    assert exitCode == 0;

                    logger.info("temp file deleted");
                }else{
                    logger.warning("there are some problems on acquisizioni");
                }
            }

        }else{
            logger.warning("Error on calculating bands number");
        }
    }


    /**
     * Calculate Polygon Area in KM
     * @param polygon   - Ploygon in WKT format
     * @return  calculated area of the polygon in KM
     * @throws SQLException
     * @throws Exception
     */
    public double calcPolygonArea(String polygon) throws SQLException{
        double retCode = 0;

        System.out.println("Procedures: caclPolygonArea");



        tdb.setPreparedStatementRef("select (St_area(ST_Transform("+polygon+",3003))/(1000000))");
        tdb.runPreparedQuery();
        if(tdb.next()){
            retCode = tdb.getDouble(1);
        }else{
            retCode = -1;
        }

        return (retCode);
    }

    public byte[] extractClassifiedImage(String image_type,
                                         String year,
                                         String month,
                                         String day,
                                         String doy,
                                         String polygon, String srid_out) throws SQLException, WSExceptions{
        String sqlString=null;
        byte[] imgOut;
        boolean checkArea=false;
        String reclass_param="", legend_param="", rast_out="";


        logger.info("IMG_TYPE: "+image_type+ " - "+year+" "+month+" "+day+" "+doy);
        if(image_type.toLowerCase().matches("tci") ) {
            reclass_param = TCI_RECLASS;
            legend_param = TCI_LEGEND;
            if (polygon.matches("") || polygon == null){
                throw new WSExceptions(ErrorCode.POLYGON_IS_MANDATORY_STR,new ErrorCode(ErrorCode.POLYGON_IS_MANDATORY));

            }else{
                rast_out = "ST_Reclass(ST_Clip(ST_Union(rast),1," +
                        "ST_Transform(" +polygon+
                        ","+DBSRID+"),-999.0,true),1,'"+reclass_param+"','8BUI',-999.0) ";
                checkArea = true;  //polygon area will be checked
            }
        }else if(image_type.toLowerCase().matches("vci") || image_type.matches("evci") ){
            reclass_param = VCI_RECLASS;
            legend_param  = TCI_LEGEND;
            if(polygon.matches("") || polygon == null) {
                throw new WSExceptions(ErrorCode.POLYGON_IS_MANDATORY_STR,new ErrorCode(ErrorCode.POLYGON_IS_MANDATORY));
            }else{
                rast_out = "ST_Reclass(ST_Clip(ST_Union(rast),1," +
                        "ST_Transform(" + polygon +
                        ","+DBSRID+"),-999.0,true),1,'"+reclass_param+"','8BUI',-999.0) ";
                checkArea = true;  //polygon area will be checked
            }
        }else if(image_type.toLowerCase().substring(0,3).matches("spi") ){
            reclass_param = SPI_RECLASS;
            legend_param  = SPI_LEGEND;
            if(polygon.matches("") || polygon == null)
                throw new WSExceptions(ErrorCode.POLYGON_IS_MANDATORY_STR,new ErrorCode(ErrorCode.POLYGON_IS_MANDATORY));
            else
                rast_out = "ST_Reclass(ST_Clip(ST_Union(rast),1,ST_Transform("+polygon+","+DBSRID+"),true),1,'"+reclass_param+"','8BUI') ";

        }else if(image_type.toLowerCase().matches("vhi")|| image_type.matches("evhi")){
            reclass_param = VHI_RECLASS;
            legend_param  = VHI_LEGEND;
            if(polygon.matches("") || polygon == null) {
                throw new WSExceptions(ErrorCode.POLYGON_IS_MANDATORY_STR,new ErrorCode(ErrorCode.POLYGON_IS_MANDATORY));
            }else{
                rast_out = "ST_Reclass(ST_Clip(ST_Union(rast),1,ST_Transform("+polygon+","+DBSRID+"),true),1,'"+reclass_param+"','8BUI') ";
                checkArea = true;  //polygon area will be checked
            }
        }else if(image_type.toLowerCase().matches("landuse")){
            legend_param  = LANDUSE_LEGEND;
            if(polygon.matches("") || polygon == null) {
                throw new WSExceptions(ErrorCode.POLYGON_IS_MANDATORY_STR,new ErrorCode(ErrorCode.POLYGON_IS_MANDATORY));
            }else{
                rast_out = "ST_Clip(ST_Union(rast),1,ST_Transform("+polygon+","+DBSRID+"),true) ";
                checkArea = true;  //polygon area will be checked
            }
        }else{
            Response.status(Response.Status.OK).entity(WRONG_IMAGE_TYPE).build();

        }


        if(image_type.matches("cru") || image_type.matches("ecad")) {
            if(polygon.matches("") || polygon == null) {
                throw new WSExceptions(ErrorCode.POLYGON_IS_MANDATORY_STR,new ErrorCode(ErrorCode.POLYGON_IS_MANDATORY));
            }else{
                rast_out = "postgis.calculate_seasonal_forecast_spi3(" +
                        "ST_Transform("+polygon+","+DBSRID+"),"+year+","+doy+",'"+image_type+"')";

                legend_param  = CRUD_LEGEND;

                sqlString="select ST_asPNG(ST_ColorMap("+rast_out+",1,'"+legend_param+"','EXACT')) ";
                checkArea = false;  //polygon area will be checked
            }
        }else{
            sqlString="select ST_asPNG(ST_ColorMap("+rast_out+",1,'"+legend_param+"','EXACT')) " +
                    "from postgis."+image_type+" as a inner join postgis.acquisizioni as b using (id_acquisizione) "+
                    "where extract('year' from b.dtime) = "+year+" "+
                    "and   extract('month' from b.dtime) = "+month+" "+
                    "and   extract('day' from b.dtime) = "+day+" "+
                    "and   ST_Intersects(rast,"+polygon+")";
        }






        if(checkArea){
            System.out.println("J_GET_WHOLE_PNG: Checking area");
            //checking area
            double thisArea = calcPolygonArea(polygon);

            if(thisArea > MAX_POLYGON_AREA){
                System.out.println("AREA: "+thisArea);
                throw new WSExceptions(ErrorCode.POLYGON_TOO_BIG_STR,new ErrorCode(ErrorCode.POLYGON_TOO_BIG));
            }
        }

        System.out.println("J_GET_WHOLE_PNG:" +sqlString);

        tdb.setPreparedStatementRef(sqlString);

        tdb.runPreparedQuery();

        if (tdb.next()) {
            imgOut = tdb.getPStmt().getResultSet().getBytes(1);
            System.out.println("Image Readed length: "+imgOut.length);
        }else{
           imgOut = null;

        }
        return imgOut;
    }

    /**
     * extract image from polygon
     * @param image_type
     * @param year
     * @param month
     * @param day
     * @param polygon
     * @return
     */
    public byte[] extractImageTiff(String image_type,
                                   String year,
                                   String month,
                                   String day,
                                   String polygon, String srid_out) throws SQLException, WSExceptions, Exception{
        String sqlString;
        byte[] imgOut;
        if(polygon.matches("") || polygon == null){
            throw new WSExceptions(ErrorCode.POLYGON_IS_MANDATORY_STR,new ErrorCode(ErrorCode.POLYGON_IS_MANDATORY));

        }else{

            sqlString="select * from ST_asGDALRaster(extract_image('"+image_type+"','"+year+"-"+month+"-"+day+"'::timestamp," +
                    "ST_Transform("+polygon+","+srid_out+")),'GTiff') ";
        }

        //checking area

        double thisArea = calcPolygonArea(polygon);

        if(thisArea > MAX_POLYGON_AREA){
            System.out.println("AREA: "+thisArea);
            throw new WSExceptions(ErrorCode.POLYGON_TOO_BIG_STR,new ErrorCode(ErrorCode.POLYGON_TOO_BIG));
        }


        System.out.println("SQL : "+sqlString);
        tdb.setPreparedStatementRef(sqlString);

        tdb.runPreparedQuery();

        if (tdb.next()) {
            imgOut = tdb.getPStmt().getResultSet().getBytes(1);
            System.out.println("Image Readed length: "+imgOut.length);
        }else{
            imgOut = null;
       }
       return imgOut;

    }
    /**
     * extract image from polygon
     * @param image_type
     * @param year
     * @param month
     * @param day
     * @return
     */
    public byte[] extractImageSPI(String image_type,
                                   String year,
                                   String month,
                                   String day) throws SQLException, WSExceptions, Exception{
        String sqlString;
        byte[] imgOut;


            sqlString="select * from ST_asGDALRaster((select rast from postgis."+image_type+"" +
                    " inner join postgis.acquisizioni using (id_acquisizione) " +
                    "where dtime = '"+year+"-"+month+"-"+day+"'::timestamp),'GTiff') ";


        //checking area



        logger.info("SQL : "+sqlString);
        tdb.setPreparedStatementRef(sqlString);

        tdb.runPreparedQuery();

        if (tdb.next()) {
            imgOut = tdb.getPStmt().getResultSet().getBytes(1);
            logger.info("Image Readed length: "+imgOut.length);
        }else{
            imgOut = null;
        }
        return imgOut;

    }

    /**
     * Calculate TCI image and store it in the DB
     * returns true if the calculus will be completed otherwise returns false
     * @param year
     * @param doy
     * @return
     * @throws SQLException
     * @throws WSExceptions
     */
     public boolean perform_tci_calculus(String year,
                                 String doy)  throws SQLException, WSExceptions
     {
         String sqlString;

         System.out.print("Calculating TCI..." + doy + "-" + year);

         sqlString = "select count(*) from postgis.calculate_tci(?, ?)";
         tdb.setPreparedStatementRef(sqlString);
         tdb.setParameter(DBManager.ParameterType.INT, doy, 1);
         tdb.setParameter(DBManager.ParameterType.INT, year, 2);
         tdb.runPreparedQuery();

         if (tdb.next()) {
             System.out.println("Success.");

             return true;
         } else {
             System.out.println("Attempt calculate TCI.");
             return false;
         }


     }

    /**
     * calculate ET real based on given polygon and timestamp
     * @param geo_in - polygon (WKT version)
     * @param srid - SRID for given polygon
     * @param dtime - timestamp for ETR calculation
     * @return  - Result image in GeoTIFF format
     * @throws Exception
     */
    public byte[] calculate_ETa(String geo_in, String srid, String dtime) throws Exception
    {

        byte[] imgOut=null;


        String sqlString="SELECT * FROM postgis.ST_AsGDALRaster(postgis.calculate_real_et( ST_GeometryFromText('"+geo_in+"',"+srid+")," +
                "'"+dtime+"'::timestamp),'GTiff')";

        logger.info(sqlString);
        tdb.setPreparedStatementRef(sqlString);
        tdb.runPreparedQuery();


        if (tdb.next()) {
            logger.info("ETa calculated");
            imgOut = tdb.getPStmt().getResultSet().getBytes(1);
            logger.info("Image Readed length: "+imgOut.length);
        }

        return imgOut;
    }


    /**
     * Create new acquisition if doesn't exist and returning ID.
     * @param imgtype   - imgtype
     * @param year     - year
     * @param doy   - doy
     * @return  - ID of new acquisition or ID of existing acquisition.
     * @throws SQLException
     */
    public int create_acquisition(String imgtype, String year, String doy) throws SQLException
    {
        logger.info("create new entry for "+ imgtype + " "+year+ " " + doy);
        int retCode = -1;
        String sqlString="INSERT INTO postgis.acquisizioni (dtime, id_imgtype) " +
                "VALUES " +
                "(to_timestamp('"+year+" "+doy+"', 'YYYY DDD'), (SELECT id_imgtype FROM postgis.imgtypes WHERE imgtype = '"+imgtype+"' )) "+
                "ON CONFLICT ON CONSTRAINT acquisizione_unique " +
                "DO UPDATE SET dtime=EXCLUDED.dtime " +
                "RETURNING id_acquisizione";



        logger.info(sqlString);
        tdb.setPreparedStatementRef(sqlString);
        tdb.runPreparedQuery();


        if (tdb.next()) {

            retCode = tdb.getInteger(1);

            logger.info("Acquisition ID: "+retCode);
        }

        return retCode;
    }

    public int create_etr(String ids, String polygon, String year, String doy) throws SQLException
    {
        logger.info("calculaate ETR ");
        int retCode = -1;
        String sqlString="SELECT * FROM postgis.calculate_save_etr("+polygon+",to_timestamp('"+year+" "+doy+"', 'YYYY DDD')::timestamp,"+ids+")";


        logger.info(sqlString);
        tdb.setPreparedStatementRef(sqlString);
        tdb.runPreparedQuery();


        if (tdb.next()) {

            retCode = tdb.getInteger(1);

            if(retCode == 0){
                logger.info("ETR image saved");
            }else{
                logger.info("error occurred");
            }
        }

        return retCode;
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
