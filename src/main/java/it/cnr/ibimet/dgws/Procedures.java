package it.cnr.ibimet.dgws;

import it.cnr.ibimet.dbutils.ErrorCode;
import it.cnr.ibimet.dbutils.SWH4EConst;
import it.cnr.ibimet.dbutils.TDBManager;
import it.cnr.ibimet.dbutils.WSExceptions;
import it.lr.libs.DBManager;

import javax.ws.rs.core.Response;
import java.sql.SQLException;
import java.util.Locale;
import java.util.logging.Logger;

public class Procedures implements SWH4EConst, ReclassConst {
    private TDBManager tdb;
    private String sqlQuery;
    static Logger logger = Logger.getLogger(String.valueOf(Procedures.class));

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
}
