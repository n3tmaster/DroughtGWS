package it.cnr.ibimet.dgws;

import it.cnr.ibimet.dbutils.SWH4EConst;
import it.cnr.ibimet.dbutils.TDBManager;

import java.sql.SQLException;

/**
 * Created by lerocchi on 16/11/17.
 */
public class SPIEngine  implements SWH4EConst{


    SPIEngine(
    ){



    }

    public static long run_bric_spi(String name, String step,
                            double x_start, double y_start,int x_col_end, int y_row_end) {

         String threadName;

         int x_col_start=-9999, y_row_start=-9999;  //x and y col/row for start procedure on original images

         TDBManager tdb=null;

        threadName = name;



     //   System.out.println(threadName + " " + step +  " : coord_x: "+x_start+" - coord_y: "+y_start+ " - col_end: "+x_col_end+" - row_end: "+y_row_end );

        try {
            tdb = new TDBManager("jdbc/ssdb");
            tdb.setPreparedStatementRef("SELECT ST_WorldToRasterCoordX(rast, ST_GeomFromText('POINT("+x_start+" "+y_start+")', 4326)), " +
                    "ST_WorldToRasterCoordY(rast, ST_GeomFromText('POINT("+x_start+" "+y_start+")', 4326)) " +
                    "FROM postgis.precipitazioni LIMIT 1");

            tdb.runPreparedQuery();

            if(tdb.next()){
                x_col_start = tdb.getInteger(1);
                y_row_start = tdb.getInteger(2);

                String sqlString=null;
                System.out.println(threadName+" starting from : "+x_col_start+" "+y_row_start);
                double completed_perc = (y_row_end * x_col_end);

                for(int y=y_row_start; y<=y_row_end; y++){
                    for(int x=x_col_start; x<=x_col_end; x++){

                        sqlString="select from postgis.prepare_spi_data("+step+","+x+","+y+","+x_start+","+y_start+")";

                        System.out.println(threadName+" - ("+x+","+y+")");


                        tdb.setPreparedStatementRef(sqlString);

                        tdb.runPreparedQuery();

                        System.out.println(threadName+" - "+(((x_col_end*y)+x)/completed_perc*100)+" %");
                    }
                }


            }else{
                System.out.println("NO ROW_COL START FOUND");
            }










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