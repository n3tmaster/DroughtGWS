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
    private int x_col_end;
    private int y_row_end;


    public SPIEngineCallable(String name, String step,
                             double x_start, double y_start,int x_col_end, int y_row_end){
        this.name = name;
        this.step = step;
        this.x_start = x_start;
        this.y_start = y_start;
        this.x_col_end = x_col_end;
        this.y_row_end = y_row_end;


    }

    @Override
    public Long call() throws Exception {

        return SPIEngine.run_bric_spi(name,step,x_start,y_start,x_col_end,y_row_end);

    }


}
