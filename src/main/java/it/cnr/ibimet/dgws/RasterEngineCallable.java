package it.cnr.ibimet.dgws;

import java.util.concurrent.Callable;

public class RasterEngineCallable implements Callable {
private String name;

private double x_start;
private double y_start;
private int year;
private int doy;
private double x_end;
private double y_end;

private String poly_in;
private String imgtype;


public RasterEngineCallable(String name,
                            double x_start, double y_start,
                            double x_end, double y_end,
                            String imgtype, int year, int doy){
        this.name = name;

        this.x_start = x_start;
        this.y_start = y_start;
        this.x_end = x_end;
        this.y_end = y_end;
        this.imgtype = imgtype;
        this.doy = doy;
        this.year = year;
        this.poly_in = "POLYGON(("+x_start+" "+y_start+","
        + x_start +" "+ y_end +","
        + x_end + " "+ y_end +","
        + x_end + " "+ y_start +","
        + x_start+" "+y_start
        +"))";


        }

        @Override
        public Long call() throws Exception {

            if(imgtype.toLowerCase().matches("vci")){
                return RasterEngine.run_bric_vci(name,poly_in,x_start,
                        y_start,x_end,y_end,
                        year,doy);
            }else if(imgtype.toLowerCase().matches("vci")){
                return RasterEngine.run_bric_evci(name,poly_in,x_start,
                        y_start,x_end,y_end,
                        year,doy);
            }else if(imgtype.toLowerCase().matches("vci")){
                return RasterEngine.run_bric_vhi(name,poly_in,x_start,
                        y_start,x_end,y_end,
                        year,doy);
            }
            return -1L;
        }


}
