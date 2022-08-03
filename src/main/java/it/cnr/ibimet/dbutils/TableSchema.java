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
 * classe per la descrizione di una tabella
 *
 * @author lerocchi
 */
public class TableSchema implements SWH4EConst {
    private String tableName;
    private TDBManager dsm;
    private List<String> columnList;
    private List<String> columnType;
    private String tipoLayer;


    public TableSchema(){

    }

    public TableSchema(TDBManager dsm){
        this.dsm=dsm;
    }



    public TableSchema(TDBManager dsm,String tableName) throws Exception{
        this(dsm);
        this.tableName=tableName;
        this.columnList=new ArrayList<String>();
        this.columnType=new ArrayList<String>();
        retrieveColumnList();
        retrieveLayerType();
    }

    public List<String> getColumnType() {
        return columnType;
    }

    public void setColumnType(List<String> columnType) {
        this.columnType = columnType;
    }

    public String getTipoLayer() {
        return tipoLayer;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public List<String> getColumnList() {
        return columnList;
    }

    public void setColumnList(List<String> columnList) {
        this.columnList = columnList;
    }

    private void retrieveColumnList(){
        try{
            dsm.setPreparedStatementRef("select column_name, data_type from information_schema.columns where table_name=?");
            dsm.setParameter(ParameterType.STRING, this.tableName, 1);
            dsm.runPreparedQuery();

            while(dsm.next()){
                //skippo the_geom e gid
                if(!dsm.getString(1).equals(THE_GEOM_COLUMN) && !dsm.getString(1).equals(GID)){
                    columnList.add(dsm.getString(1));
                    columnType.add(dsm.getString(2));
                }
            }

        } catch (SQLException ex) {
            Logger.getLogger(TableSchema.class.getName()).log(Level.SEVERE, null, ex);
        }
    }


    /**
     * Retrieve colunmn type
     *
     * @param columnName
     * @return columnType or -1 if it doesn't exist
     */
    public String getColumnType(String columnName){

        for(int i=0; i < columnList.size() ; i++){

            if(columnList.get(i).compareToIgnoreCase(columnName)==0){
                //  System.out.println("sono in getColumnType: " + columnList.get(i)+ " - "+ columnName + " - Type: "+columnType.get(i));

                return columnType.get(i);
            }
        }
        return "-1";
    }


    public String getCompleteColumnList(){
        String rstOut="";
        boolean primo=true;
        for(String questoElem : columnList){
            if(primo){
                primo=false;
                rstOut="\""+questoElem+"\"";
            }else{
                rstOut+=",\""+questoElem+"\"";
            }

        }
        return rstOut;
    }

    //salva il tipo layer
    private void retrieveLayerType() throws Exception{

        String questoObj="-1";


        dsm.setPreparedStatementRef(GET_LAYER_TYPE);

        dsm.setParameter(ParameterType.STRING, this.tableName, 1);
        dsm.runPreparedQuery();

        if(dsm.next()){
            questoObj= dsm.getString(2);
        }

        this.tipoLayer = questoObj;
    }
}
