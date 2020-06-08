package it.cnr.ibimet.dgws;

import it.cnr.ibimet.dbutils.SWH4EConst;
import it.cnr.ibimet.dbutils.TDBManager;

public class RasterEngine  implements SWH4EConst {


    RasterEngine(){ }

    public static long run_bric_vci(String name,
                                    String polyin,
                                    double x_start, double y_start,
                                    double x_end, double y_end,
                                    int year, int doy) {

        String threadName;
        TDBManager tdb=null;
        threadName = name;


        try {
            tdb = new TDBManager("jdbc/ssdb");
           // System.out.println(name + "select * from postgis.calculate_vci2(" +
           //         doy +","+year+","+
           //         "ST_GeomFromText('"+polyin+"',4326))");
            System.out.println(threadName+" start with "+polyin);
            tdb.setPreparedStatementRef("select * from postgis.calculate_vci2(" +
                    doy +","+year+","+
                    "ST_GeomFromText('"+polyin+"',4326))");

            tdb.runPreparedQuery();
            if(tdb.next()){
                System.out.println(threadName+" Result: ok");
            }
            System.out.println(threadName + " closing connection.");
            tdb.closeConnection();


            System.out.println(threadName + "- done");

        } catch (Exception e) {
            e.printStackTrace();

            System.out.println(threadName + " interrupted: "+e.getMessage());
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

    public static long run_bric_evci(String name,
                                    String polyin,
                                    double x_start, double y_start,
                                    double x_end, double y_end,
                                    int year, int doy) {

        String threadName;
        TDBManager tdb=null;
        threadName = name;


        try {
            tdb = new TDBManager("jdbc/ssdb");
            // System.out.println(name + "select * from postgis.calculate_vci2(" +
            //         doy +","+year+","+
            //         "ST_GeomFromText('"+polyin+"',4326))");
            System.out.println(threadName+" start with "+polyin);
            tdb.setPreparedStatementRef("select * from postgis.calculate_evci2(" +
                    doy +","+year+","+
                    "ST_GeomFromText('"+polyin+"',4326))");

            tdb.runPreparedQuery();
            if(tdb.next()){
                System.out.println(threadName+" Result: ok");
            }
            System.out.println(threadName + " closing connection.");
            tdb.closeConnection();


            System.out.println(threadName + "- done");

        } catch (Exception e) {
            e.printStackTrace();

            System.out.println(threadName + " interrupted: "+e.getMessage());
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


    public static long run_bric_vhi(String name,
                                    String polyin,
                                    double x_start, double y_start,
                                    double x_end, double y_end,
                                    int year, int doy) {

        String threadName;
        TDBManager tdb=null;
        threadName = name;


        try {
            tdb = new TDBManager("jdbc/ssdb");
            // System.out.println(name + "select * from postgis.calculate_vci2(" +
            //         doy +","+year+","+
            //         "ST_GeomFromText('"+polyin+"',4326))");
            System.out.println(threadName+" start with "+polyin);
            tdb.setPreparedStatementRef("select * from postgis.calculate_vhi2(" +
                    doy +","+year+","+
                    "ST_GeomFromText('"+polyin+"',4326))");

            tdb.runPreparedQuery();
            if(tdb.next()){
                System.out.println(threadName+" Result: ok");
            }
            System.out.println(threadName + " closing connection.");
            tdb.closeConnection();


            System.out.println(threadName + "- done");

        } catch (Exception e) {
            e.printStackTrace();

            System.out.println(threadName + " interrupted: "+e.getMessage());
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