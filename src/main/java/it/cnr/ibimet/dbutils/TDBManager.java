package it.cnr.ibimet.dbutils;


import it.lr.libs.DBInterface;
import it.lr.libs.DBManager;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.sql.DataSource;
import java.sql.*;
import java.util.Calendar;
import java.util.GregorianCalendar;

//import sun.jdbc.*;
public class TDBManager extends DBManager implements DBInterface{


    private  String dburl;
    private  String dbuser;
    private  String dbpwd;
    private  String driverName;




    private String questoDS;


    private Context context;
    private Context envCtx;
    private DataSource ds;


    /**
     * usa il context ed apre al volo
     * @param questoDS
     * @throws Exception
     */
    public  TDBManager(String questoDS) throws Exception{

        context = new InitialContext();
        envCtx=(Context)context.lookup("java:comp/env");
        this.questoDS=questoDS;
        ds =(DataSource)envCtx.lookup(questoDS);
        System.out.println("TDBManager - main - lookup");
        this.dbConnection=ds.getConnection();
        System.out.println("TDBManager - main - connessione fatta");


    }


    public  TDBManager(String driverName, String dburl, String dbuser, String dbpwd) throws Exception{
        this.driverName=driverName;
        this.dburl=dburl;
        this.dbuser=dbuser;
        this.dbpwd=dbpwd;

        //	Class.forName(driverName);


    }

    public static Timestamp Gregorian2Timestamp(GregorianCalendar
                                                        gregorianCalendar) {
        return (gregorianCalendar == null ? null : new
                Timestamp(gregorianCalendar.getTimeInMillis()));
    }

    public static GregorianCalendar Timestamp2Gregorian(Timestamp timestamp) {
        GregorianCalendar result = null;
        if (timestamp != null) {
            result = new GregorianCalendar();

            result.setTimeInMillis(timestamp.getTime());
        }
        return (result);
    }


    public ResultSet performQuery(String queryStr) throws SQLException {
        this.dbPStmt=this.dbConnection.prepareStatement(queryStr);

        return this.dbPStmt.executeQuery();
    }

    public void commit() throws SQLException{
        dbConnection.commit();
    }

    public boolean performInsert() throws SQLException {

        return dbPStmt.execute();
    }


    public boolean performDelete(String deleteStr) throws SQLException {

        return false;
    }


    public void setPreparedStatementRef(String queryStr) throws SQLException {
        this.dbPStmt=this.dbConnection.prepareStatement(queryStr);

    }


    public void setParameter(boolean param, int index)
            throws SQLException {
        this.dbPStmt.setBoolean(index, param);
    }


    public void setParameter(ParameterType pType, String param, int index)
            throws SQLException {
        switch(pType){
            case DOUBLE:

                this.dbPStmt.setDouble(index, Double.parseDouble(param));
                break;

            case FLOAT:

                this.dbPStmt.setFloat(index, Float.parseFloat(param));
                break;
            case INT:

                this.dbPStmt.setInt(index, Integer.parseInt(param));
                break;
            case STRING:
                this.dbPStmt.setString(index, param);
                break;

        }

    }

    public void setParameterNull(int index) throws SQLException{
        this.dbPStmt.setNull(index, Types.NULL);
    }

    public void setParameter(ParameterType pType, GregorianCalendar param, int index)
            throws SQLException {
        switch(pType){
            case DATE:

                this.dbPStmt.setTimestamp(index, Gregorian2Timestamp(param));
                break;

        }

    }


    public void openConnection() throws SQLException {
        try{
            Class.forName(driverName);
            dbConnection = DriverManager.getConnection(dburl,dbuser,dbpwd );
            dbConnection.setAutoCommit(false);
        }catch(Exception e){
            System.out.println(e.getMessage());
            e.printStackTrace();
        }

    }


    public void closeConnection() throws SQLException {
        this.dbConnection.close();

    }

    public PreparedStatement getPStmt(){
        return dbPStmt;
    }

    //Eseguo query e riempio l'RS
    public void runPreparedQuery() throws SQLException{

        dbResultSet=dbPStmt.executeQuery();

    }

    //Effettuo il next
    public boolean next() throws SQLException{
        return dbResultSet.next();
    }

    //Restituisco il singolo campo
    public int getHour(int index)
            throws SQLException {

        GregorianCalendar qC =Timestamp2Gregorian( dbResultSet.getTimestamp(index));

        //	System.out.println("Questa ora: "+qC.get(Calendar.HOUR_OF_DAY)+" : "+qC.get(Calendar.MINUTE));

        return qC.get(Calendar.HOUR_OF_DAY);

    }
    public String getString(int index)
            throws SQLException {

        return dbResultSet.getString(index);

    }

    public String getString(String fieldName)
            throws SQLException {

        return dbResultSet.getString(fieldName);

    }
    public int getInteger(int index)
            throws SQLException {

        return dbResultSet.getInt(index);

    }

    public boolean getBoolean(int index)
            throws SQLException {

        return dbResultSet.getBoolean(index);

    }

    public GregorianCalendar getData(int index) throws SQLException{


        return Timestamp2Gregorian(dbResultSet.getTimestamp(index));
    }

    public double getDouble(int index)
            throws SQLException {

        return dbResultSet.getDouble(index);

    }

    public float getFloat(int index)
            throws SQLException {

        return dbResultSet.getFloat(index);

    }

    public ResultSet getRS() throws SQLException{
        dbResultSet=dbPStmt.executeQuery();
        return this.dbResultSet;
    }


    public Connection getConnectionRef() throws SQLException {
        //prima chiudo eventuali recordset e preparastmt
        if(this.dbPStmt!=null){
            if(!dbPStmt.isClosed()){
                if(!dbResultSet.isClosed())
                    dbResultSet.close();
                dbPStmt.close();
            }
        }
        return null;
    }

}
