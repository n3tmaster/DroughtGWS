package it.cnr.ibimet.dgws;

import it.cnr.ibimet.dbutils.SWH4EConst;
import it.cnr.ibimet.dbutils.TDBManager;
import it.lr.libs.DBManager;

import java.sql.SQLException;

public class Procedures implements SWH4EConst {
    private TDBManager tdb;
    private String sqlQuery;

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


}
