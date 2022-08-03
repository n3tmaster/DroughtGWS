/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.cnr.ibimet.dbutils;

import it.lr.libs.DBManager.ParameterType;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * ChartParams -
 * class for dynamic parameter management for Chart Bean
 * @author lerocchi
 */
public class ChartParams {
    public static final String NUMERIC = "numeric";
    private List<String> param, param_name, chart_type, aggregation_type,unit, min_limit, max_limit;
    private TDBManager dsm;
    private int id_mobile_station;
    private String lang;
    private boolean minmax;


    public ChartParams(){
        this.lang="it";
        this.param = new ArrayList<String>();
        this.param_name = new ArrayList<String>();
        this.chart_type = new ArrayList<String>();
        this.aggregation_type = new ArrayList<String>();
        this.unit = new ArrayList<String>();
        this.min_limit = new ArrayList<String>();
        this.max_limit = new ArrayList<String>();
    }

    public ChartParams(TDBManager dsm, int id_mobile_station){
        this();
        this.dsm = dsm;
        this.id_mobile_station = id_mobile_station;


    }

    public ChartParams(TDBManager dsm, int id_mobile_station, String lang){
        this(dsm,id_mobile_station);
        this.lang = lang;
        this.minmax=false;
    }

    public ChartParams(TDBManager dsm, int id_mobile_station, String lang, boolean minmax){
        this(dsm,id_mobile_station,lang);
        this.minmax = minmax;

    }

    public List<String> getUnit() {
        return unit;
    }

    public void setUnit(List<String> unit) {
        this.unit = unit;
    }

    public List<String> getMin_limit() {
        return min_limit;
    }

    public void setMin_limit(List<String> min_limit) {
        this.min_limit = min_limit;
    }

    public List<String> getMax_limit() {
        return max_limit;
    }

    public void setMax_limit(List<String> max_limit) {
        this.max_limit = max_limit;
    }

    public List<String> getParam() {
        return param;
    }

    public void setParam(List<String> param) {
        this.param = param;
    }

    public List<String> getParam_name() {
        return param_name;
    }

    public void setParam_name(List<String> param_name) {
        this.param_name = param_name;
    }

    public List<String> getChart_type() {
        return chart_type;
    }

    public void setChart_type(List<String> chart_type) {
        this.chart_type = chart_type;
    }

    public void getTableParams(){
        try {
            dsm.setPreparedStatementRef("select param, chart_type, param_name_"+this.lang+",aggregation_type, unit, min_limit, max_limit from chart_params where id_mobile_station=? and chart_type is not null order by chart_type");
            dsm.setParameter(ParameterType.INT, ""+this.id_mobile_station, 1);

            dsm.runPreparedQuery();

            while(dsm.next()){
                param.add(dsm.getString(1));
                chart_type.add(dsm.getString(2));
                param_name.add(dsm.getString(3));
                aggregation_type.add(dsm.getString(4));
                unit.add(dsm.getString(5));
                min_limit.add(""+dsm.getDouble(6));
                max_limit.add(""+dsm.getDouble(7));
                if(minmax){

                    param.add(dsm.getString(1));
                    chart_type.add(dsm.getString(2));
                    param_name.add("MIN");
                    aggregation_type.add("min");
                    unit.add(dsm.getString(5));
                    min_limit.add(""+dsm.getDouble(6));
                    max_limit.add(""+dsm.getDouble(7));

                    param.add(dsm.getString(1));
                    chart_type.add(dsm.getString(2));
                    param_name.add("MAX");
                    aggregation_type.add("max");
                    unit.add(dsm.getString(5));
                    min_limit.add(""+dsm.getDouble(6));
                    max_limit.add(""+dsm.getDouble(7));
                }
            }

        } catch (SQLException ex) {
            Logger.getLogger(ChartParams.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    public List<String> getAggregation_type() {
        return aggregation_type;
    }

    public void setAggregation_type(List<String> aggregation_type) {
        this.aggregation_type = aggregation_type;
    }

    public int getParamNumbers(){
        return param.size();
    }

    public String getSQL_SelectStr(){
        String sqlOut = "";


        for(int i=0; i<param.size(); i++){
            sqlOut = sqlOut + aggregation_type.get(i)+"("+param.get(i)+"),";
        }

        return sqlOut;
    }




    //Retrieve Select SQL statement without aggregation and decimal round
    /**
     *
     * @param index  -  index for element in array
     * @param cast_type - type of cast
     * @param prefix - prefix for field
     * @param digits - number of digits
     * @return
     */
    public String getSQL_SelectStr(int index, String cast_type, String prefix, int digits){
        String sqlOut = "";


        sqlOut =  "round(cast("+prefix+param.get(index)+" as " + cast_type + "),"+digits+")";

        return sqlOut;
    }

    /**
     *
     * @param index  -  index for element in array
     * @param cast_type - type of cast
     * @param prefix - prefix for field
     * @param digits - number of digits
     * @return
     */
    public String getSQL_SelectStr(int index, String cast_type, String prefix, int digits, String  colName){
        String sqlOut = "";

        if(minmax){
            sqlOut =  "round(cast(min("+prefix+param.get(index)+") as " + cast_type + "),"+digits+") as min"+colName+",";
            sqlOut += "round(cast(max("+prefix+param.get(index)+") as " + cast_type + "),"+digits+") as max"+colName+",";
            sqlOut += "round(cast(avg("+prefix+param.get(index)+") as " + cast_type + "),"+digits+") as avg"+colName;
        }else{
            sqlOut =  "round(cast("+prefix+param.get(index)+" as " + cast_type + "),"+digits+")";
        }


        return sqlOut;
    }

    /**
     *
     * @param index  -  index for element in array
     * @param cast_type - type of cast
     * @param prefix - prefix for field
     * @param digits - number of digits
     * @param stat - create statistical operations
     * @return
     */
    public String getSQL_SelectStr(int index, String cast_type, String prefix, int digits, String  colName, boolean stat){
        String sqlOut = "";

        if(stat){
            sqlOut =  "round(cast(min("+prefix+param.get(index)+") as " + cast_type + "),"+digits+") as min"+colName+",";
            sqlOut += "round(cast(max("+prefix+param.get(index)+") as " + cast_type + "),"+digits+") as max"+colName+",";
            sqlOut += "round(cast(avg("+prefix+param.get(index)+") as " + cast_type + "),"+digits+") as avg"+colName;
        }else{
            sqlOut =  "round(cast("+prefix+param.get(index)+" as " + cast_type + "),"+digits+")";
        }


        return sqlOut;
    }


    public String getSQL_WhereStr(){
        String sqlOut = "";
        int h=0;
        if(this.minmax){
            h=3;
        }

        for(int i=0, j=0; i<max_limit.size(); i++, j+=h){
            sqlOut = sqlOut + " AND "+param.get(j)+" between "+min_limit.get(i) + " AND " + max_limit.get(i);
        }

        return sqlOut;
    }

    public String getSQL_WhereStr(int index){
        return " AND " + param.get(index) +" between "+min_limit.get(index) + " AND " + max_limit.get(index);
    }
}
