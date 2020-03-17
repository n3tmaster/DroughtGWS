package it.cnr.ibimet.dgws;

import it.cnr.ibimet.dbutils.SWH4EConst;
import it.cnr.ibimet.dbutils.TDBManager;
import it.lr.libs.DBManager;

import java.sql.SQLException;

/**
 * Created by lerocchi on 16/11/17.
 */public class SPIEngine  implements SWH4EConst{


    SPIEngine(){ }

    public static long run_bric_spi(String name, String step,
                                    String polyin,
                                    double x_start, double y_start,
                                    double x_end, double y_end,
                                    int w, int h, double scalex, double scaley, int nthreads) {

        String threadName;
        TDBManager tdb=null;
        threadName = name;

        String sqlOut="select dmonth, dyear, w, h, pxval from postgis.extract_rainfall_dump('"+polyin+"',0)";

        System.out.println(threadName + "-" + sqlOut);
        try {
            tdb = new TDBManager("jdbc/ssdb");
            tdb.setPreparedStatementRef("insert into postgis.spitemp (dmonth, dyear, pxval, dxo, dyo, dw, dh,dscalex,dscaley) select * from postgis.spi_mc_calculation(?,?,"+nthreads+","+x_start+","+y_start+","+w+","+h+","+scalex+","+scaley+")");
            tdb.setParameter(DBManager.ParameterType.INT, ""+step, 1);
            tdb.setParameter(DBManager.ParameterType.STRING, sqlOut, 2);

            tdb.performInsert();

            System.out.println(threadName + "- done");

        } catch (Exception e) {
            e.printStackTrace();

            System.out.println(threadName + " interrupted: "+e.getMessage());

        } finally{
            try{

                System.out.println(threadName + " closing connection.");
                tdb.closeConnection();
            }catch (Exception eee){
                System.out.println("Error "+eee.getMessage());
            }
        }
        System.out.println(threadName + " exiting.");

        return 1;

    }





}