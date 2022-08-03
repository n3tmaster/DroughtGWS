package it.cnr.ibimet.dgws;

import java.util.concurrent.Callable;

/**
 * Created by lerocchi on 22/11/17.
 */
public class SPIEngineCallable implements Callable {
    private String name;
    private String step;
    private double x_start;
    private double y_start;

    private double x_end;
    private double y_end;
    private int w;
    private int h;

    private double scalex;
    private double scaley;
    private int nthreads;
    private String poly_in;


    public SPIEngineCallable(String name, String step,
                             double x_start, double y_start,double x_end, double y_end,
                             int w, int h, double scalex, double scaley,int nthreads){
        this.name = name;
        this.step = step;
        this.x_start = x_start;
        this.y_start = y_start;
        this.x_end = x_end;
        this.y_end = y_end;
        this.w = w;
        this.h = h;
        this.scalex=scalex;
        this.scaley=scaley;
        this.nthreads=nthreads;
        this.poly_in = "POLYGON(("+x_start+" "+y_start+","
                + x_start +" "+ y_end +","
                + x_end + " "+ y_end +","
                + x_end + " "+ y_start +","
                + x_start+" "+y_start
                +"))";


    }

    @Override
    public Long call() throws Exception {

        return SPIEngine.run_bric_spi(name,step,poly_in,x_start,y_start,x_end,y_end,w,h,scalex,scaley,nthreads);

    }


}
