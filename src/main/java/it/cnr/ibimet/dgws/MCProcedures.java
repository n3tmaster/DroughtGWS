package it.cnr.ibimet.dgws;

import it.cnr.ibimet.dbutils.SWH4EConst;
import it.cnr.ibimet.dbutils.TDBManager;
import it.lr.libs.DBManager;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

public class MCProcedures implements SWH4EConst {
    private TDBManager tdb;
    private String db_context;
    private ExecutorService executor;
    private Timestamp startTime, endTime;

    public MCProcedures(String db_context) throws Exception {

        this.db_context = db_context;
    }

    public MCProcedures(TDBManager tdb ) {
        this.tdb = tdb;
    }

    public MCProcedures(TDBManager tdb,ExecutorService executor ) {
        this.tdb = tdb;
        this.executor = executor;
    }

    public ExecutorService getExecutor() {
        return executor;
    }

    public void setExecutor(ExecutorService executor) {
        this.executor = executor;
    }

    public Timestamp getStartTime() {
        return startTime;
    }

    public void setStartTime(Timestamp startTime) {
        this.startTime = startTime;
    }

    public Timestamp getEndTime() {
        return endTime;
    }

    public void setEndTime(Timestamp endTime) {
        this.endTime = endTime;
    }

    public boolean closeConnection() throws Exception{
        tdb.closeConnection();

        return true;
    }

    public boolean shutdownExecutor() {
        if (!executor.isTerminated()) {
            System.err.println("Attempt to shutdown executer");
            System.err.println("using shutdownNow()");
        }
        executor.shutdownNow();

        return true;
    }
    /**
     * perform VCI calculation in multi threads
     *
     * @param nthread
     * @param year
     * @param doy
     * @return
     * @throws SQLException
     * @throws Exception
     * @throws InterruptedException
     *
     * Returns : 0 OK
     */
    public int perform_vci_calculus( int nthread, int year, int doy) throws
            SQLException,Exception,InterruptedException{

        System.out.println("VCI Multithread start");

        int i=1;
        double xmin=0.0, xmax=0.0, ymin=0.0, ymax=0.0;
        double w=0.0, h=0.0,h_part=0.0,w_part=0.0;
        double icount, jcount;
        Long resultFuture = new Long(0);

        startTime = new Timestamp(System.currentTimeMillis());
        System.out.println("VCI calculation, starting time: "+startTime);

        tdb = new TDBManager(db_context);
        String sqlString=null;

        sqlString =  sqlString = "select st_xmin(st_extent(convex_total)), st_ymin(st_extent(convex_total)), "+
                "st_xmax(st_extent(convex_total)), st_ymax(st_extent(convex_total)) "+
                "from "+
                "(select st_union(st_convexhull(rast)) as convex_total "+
                "from postgis.ndvi inner join postgis.acquisizioni using (id_acquisizione) "+
                "where dtime = (select dtime from postgis.acquisizioni inner join postgis.imgtypes using (id_imgtype) "+
                "where imgtype = 'NDVI' and extract(doy from dtime)=? and extract(year from dtime)=? )) as foo";




        tdb.setPreparedStatementRef(sqlString);
        tdb.setParameter(DBManager.ParameterType.INT,""+doy,1);
        tdb.setParameter(DBManager.ParameterType.INT,""+year,2);


        tdb.runPreparedQuery();

        if(tdb.next()){
            xmin = tdb.getDouble(1);
            ymin = tdb.getDouble(2);
            xmax = tdb.getDouble(3);
            ymax = tdb.getDouble(4);

            w = xmax - xmin;
            h = ymax - ymin;
            w_part = w / (nthread / 2);
            h_part = h / (nthread / 2);
        }
        System.out.println("xmin: " + xmin + " ymin: "+ ymin + " xmax: "+xmax+ " ymax: "+ymax);
        System.out.println("h: "+h + " w: "+w);
        System.out.println("h_part: "+h_part + " w_part: "+w_part);



        //checking acquisition entry
        System.out.print("VCI MC: Checking acquisition entry...");
        sqlString = "select id_acquisizione " +
                "from   postgis.acquisizioni inner join postgis.imgtypes using (id_imgtype) " +
                "where  imgtype = 'VCI' " +
                "and    extract(doy from dtime) = ? " +
                "and    extract(year from dtime) = ?";

        tdb.setPreparedStatementRef(sqlString);
        tdb.setParameter(DBManager.ParameterType.INT,""+doy,1);
        tdb.setParameter(DBManager.ParameterType.INT,""+year,2);

        tdb.runPreparedQuery();
        if(tdb.next()){
            System.out.println("VCI MC: exists at "+doy+" "+year);
        }else{
            System.out.println("VCI MC: creating acquisition entry at "+doy+" "+year);
            sqlString = "INSERT INTO postgis.acquisizioni (dtime, id_imgtype)" +
                    " VALUES (to_timestamp('"+year+" "+doy+"', 'YYYY DDD')," +
                    "(select id_imgtype from postgis.imgtypes where imgtype='VCI'))";
            tdb.setPreparedStatementRef(sqlString);
            tdb.performInsert();
        }


        //deleting old vci
        System.out.println("deleting old vci "+year+" "+doy);
        sqlString = "delete from postgis.vci where id_acquisizione = " +
                "(select id_acquisizione from postgis.acquisizioni " +
                "inner join postgis.imgtypes using (id_imgtype) " +
                "where imgtype = 'VCI' and extract(year from dtime) = "+year+" " +
                "and extract(doy from dtime) = "+doy+ " )";
        tdb.setPreparedStatementRef(sqlString);
        tdb.performInsert();

        //Launch parallel instances for SPI calculation

        // Create a new ExecutorService with 10 thread to execute and store the Futures. Each one represent one spi thread
        executor = Executors.newFixedThreadPool((int)((nthread*nthread)+5));


        // Wait until all results are available and combine them at the same time

        System.out.println("VCI -  colosing connection...");
        tdb.closeConnection();

        List<FutureTask> taskList = new ArrayList<FutureTask>();
        i=1;
        for (icount = xmin; icount <= xmax; icount+=w_part){
            for (jcount = ymin; jcount <= ymax; jcount+=h_part){

                System.out.println("VCI init instance n. "+i);
                FutureTask futureTask_n = new FutureTask(new RasterEngineCallable("VCI Thread-"+i,
                        icount,jcount,(icount+w_part),(jcount+h_part),"vci",year,doy));

                taskList.add(futureTask_n);

                executor.execute(futureTask_n);
                i++;
            }
        }


        for (FutureTask futureTask : taskList) {
            resultFuture += (Long)futureTask.get();

        }


        System.out.println("Parallel threads have been finished");


        // Shutdown the ExecutorService
        System.out.println("Shutting down executor");
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);


        //Save calculated data into spi table
        System.out.println("VCI re-opening connection...");
        tdb = new TDBManager(db_context);

        System.out.println("VCI: deleting duplicated VCI tiles");

        sqlString="select * from postgis.deduplicate_vci("+doy+","+year+")";
        tdb.setPreparedStatementRef(sqlString);
        tdb.runPreparedQuery();
        if(tdb.next()){
            System.out.println("VCI deduplicated");


        }


        System.out.println("VCI closing connection...");
        tdb.closeConnection();


        if (!executor.isTerminated()) {
            System.err.println("Attempt to shutdown executer");
            System.err.println("using shutdownNow()");
        }
        executor.shutdownNow();
        endTime = new Timestamp(System.currentTimeMillis());
        System.out.println("ending time: "+endTime);


        return 0;


    }


    /**
     * perform E-VCI calculation in multi threads
     *
     * @param nthread
     * @param year
     * @param doy
     * @return
     * @throws SQLException
     * @throws Exception
     * @throws InterruptedException
     *
     * Returns : 0 OK
     */
    public int perform_evci_calculus(int nthread, int year, int doy) throws
            SQLException,Exception,InterruptedException{

        System.out.println("VCI Multithread start");

        int i=1;
        double xmin=0.0, xmax=0.0, ymin=0.0, ymax=0.0;
        double w=0.0, h=0.0,h_part=0.0,w_part=0.0;
        double icount, jcount;
        Long resultFuture = new Long(0);





        startTime = new Timestamp(System.currentTimeMillis());
        System.out.println("VCI calculation, starting time: "+startTime);

        tdb = new TDBManager(db_context);
        String sqlString=null;

        sqlString = "select st_xmin(st_extent(convex_total)), st_ymin(st_extent(convex_total)), "+
                "st_xmax(st_extent(convex_total)), st_ymax(st_extent(convex_total)) "+
                "from "+
                "(select st_union(st_convexhull(rast)) as convex_total "+
                "from postgis.evi inner join postgis.acquisizioni using (id_acquisizione) "+
                "where dtime = (select dtime from postgis.acquisizioni inner join postgis.imgtypes using (id_imgtype) "+
                "where imgtype = 'EVI' and extract(doy from dtime)=? and extract(year from dtime)=? )) as foo";


        System.out.println("VCI MC: "+sqlString);

        tdb.setPreparedStatementRef(sqlString);
        tdb.setParameter(DBManager.ParameterType.INT,""+doy,1);
        tdb.setParameter(DBManager.ParameterType.INT,""+year,2);


        tdb.runPreparedQuery();

        if(tdb.next()){
            xmin = tdb.getDouble(1);
            ymin = tdb.getDouble(2);
            xmax = tdb.getDouble(3);
            ymax = tdb.getDouble(4);

            w = xmax - xmin;
            h = ymax - ymin;
            w_part = w / (nthread / 2);
            h_part = h / (nthread / 2);
        }
        System.out.println("xmin: " + xmin + " ymin: "+ ymin + " xmax: "+xmax+ " ymax: "+ymax);
        System.out.println("h: "+h + " w: "+w);
        System.out.println("h_part: "+h_part + " w_part: "+w_part);


        //deleting old vci
        System.out.println("deleting old e-vci "+year+" "+doy);
        sqlString = "delete from postgis.evci where id_acquisizione = " +
                "(select id_acquisizione from postgis.acquisizioni " +
                "inner join postgis.imgtypes using (id_imgtype) " +
                "where imgtype = 'EVCI' and extract(year from dtime) = "+year+" " +
                "and extract(doy from dtime) = "+doy+ " )";
        tdb.setPreparedStatementRef(sqlString);
        tdb.performInsert();

        //Launch parallel instances for SPI calculation

        // Create a new ExecutorService with 10 thread to execute and store the Futures. Each one represent one spi thread
        executor = Executors.newFixedThreadPool((int)((nthread*nthread)+5));

        //           executor = Executors.newCachedThreadPool();
        // Wait until all results are available and combine them at the same time

        System.out.println("E-VCI -  closing connection...");
        tdb.closeConnection();

        List<FutureTask> taskList = new ArrayList<FutureTask>();
        i=1;
        for (icount = xmin; icount <= xmax; icount+=w_part){
            for (jcount = ymin; jcount <= ymax; jcount+=h_part){

                System.out.println("E-VCI init instance n. "+i);
                FutureTask futureTask_n = new FutureTask(new RasterEngineCallable("E-VCI Thread-"+i,
                        icount,jcount,(icount+w_part),(jcount+h_part),"evci",year,doy));

                taskList.add(futureTask_n);

                executor.execute(futureTask_n);
                i++;
            }
        }


        for (FutureTask futureTask : taskList) {
            resultFuture += (Long)futureTask.get();

        }


        System.out.println("Parallel threads have been finished");


        // Shutdown the ExecutorService
        System.out.println("Shutting down executor");
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);


        //Save calculated data into spi table
        System.out.println("E-VCI re-opening connection...");
        tdb = new TDBManager(db_context);

        System.out.println("E-VCI: deleting duplicated E-VCI tiles");

        sqlString="select * from postgis.deduplicate_evci("+doy+","+year+")";
        tdb.setPreparedStatementRef(sqlString);
        tdb.runPreparedQuery();
        if(tdb.next()){
            System.out.println("E-VCI deduplicated");
        }


        System.out.println("E-VCI closing connection...");
        tdb.closeConnection();


        if (!executor.isTerminated()) {
            System.err.println("Attempt to shutdown executer");
            System.err.println("using shutdownNow()");
        }
        executor.shutdownNow();
        endTime = new Timestamp(System.currentTimeMillis());
        System.out.println("ending time: "+endTime);
        return 0;


    }


    public int perform_vhi_calculus(int nthread, int year, int doy) throws
            SQLException,Exception,InterruptedException{

        System.out.println("VHI Multithread start");

        int i=1;
        double xmin=0.0, xmax=0.0, ymin=0.0, ymax=0.0;
        double w=0.0, h=0.0,h_part=0.0,w_part=0.0;
        double icount, jcount;
        Long resultFuture = new Long(0);






        startTime = new Timestamp(System.currentTimeMillis());
        System.out.println("VHI calculation, starting time: "+startTime);

        tdb = new TDBManager(db_context);
        String sqlString=null;

        sqlString = "select st_xmin(st_extent(convex_total)), st_ymin(st_extent(convex_total)), "+
                "st_xmax(st_extent(convex_total)), st_ymax(st_extent(convex_total)) "+
                "from "+
                "(select st_union(st_convexhull(rast)) as convex_total "+
                "from postgis.vci inner join postgis.acquisizioni using (id_acquisizione) "+
                "where dtime = (select dtime from postgis.acquisizioni inner join postgis.imgtypes using (id_imgtype) "+
                "where imgtype = 'VCI' and extract(doy from dtime)=? and extract(year from dtime)=? )) as foo";




        tdb.setPreparedStatementRef(sqlString);
        tdb.setParameter(DBManager.ParameterType.INT,""+doy,1);
        tdb.setParameter(DBManager.ParameterType.INT,""+year,2);

        tdb.runPreparedQuery();

        if(tdb.next()){
            xmin = tdb.getDouble(1);
            ymin = tdb.getDouble(2);
            xmax = tdb.getDouble(3);
            ymax = tdb.getDouble(4);

            w = xmax - xmin;
            h = ymax - ymin;
            w_part = w / (nthread / 2);
            h_part = h / (nthread / 2);
        }else{
            System.out.println("ERROR: nothing found ... VHI closing connection...");
            tdb.closeConnection();


            if (!executor.isTerminated()) {
                System.err.println("Attempt to shutdown executer");
                System.err.println("using shutdownNow()");
            }
            executor.shutdownNow();
            endTime = new Timestamp(System.currentTimeMillis());
            System.out.println("ending time: "+endTime);

            return -1;
        }

        System.out.println("xmin: " + xmin + " ymin: "+ ymin + " xmax: "+xmax+ " ymax: "+ymax);
        System.out.println("h: "+h + " w: "+w);
        System.out.println("h_part: "+h_part + " w_part: "+w_part);


        //checking acquisition entry
        System.out.print("VHI: Checking acquisition entry...");
        sqlString = "select id_acquisizione " +
                "from   postgis.acquisizioni inner join postgis.imgtypes using (id_imgtype) " +
                "where  imgtype = 'VHI' " +
                "and    extract(doy from dtime) = ? " +
                "and    extract(year from dtime) = ?";

        tdb.setPreparedStatementRef(sqlString);
        tdb.setParameter(DBManager.ParameterType.INT,""+doy,1);
        tdb.setParameter(DBManager.ParameterType.INT,""+year,2);

        tdb.runPreparedQuery();
        if(tdb.next()){
            System.out.println("VHI exists at "+doy+" "+year);
        }else{
            System.out.println("VHI: creating acquisition entry at "+doy+" "+year);
            sqlString = "INSERT INTO postgis.acquisizioni (dtime, id_imgtype)" +
                    " VALUES (to_timestamp('"+year+" "+doy+"', 'YYYY DDD')," +
                    "(select id_imgtype from postgis.imgtypes where imgtype='VHI'))";
            tdb.setPreparedStatementRef(sqlString);
            tdb.performInsert();
        }


        //deleting old vci
        System.out.println("deleting old vhi"+year+" "+doy);
        sqlString = "delete from postgis.vhi where id_acquisizione = " +
                "(select id_acquisizione from postgis.acquisizioni " +
                "inner join postgis.imgtypes using (id_imgtype) " +
                "where imgtype = 'VHI' and extract(year from dtime) = "+year+" " +
                "and extract(doy from dtime) = "+doy+ " )";
        tdb.setPreparedStatementRef(sqlString);
        tdb.performInsert();


        //Launch parallel instances for SPI calculation

        // Create a new ExecutorService with 10 thread to execute and store the Futures. Each one represent one spi thread
        executor = Executors.newFixedThreadPool((int)((nthread*nthread)+5));

        //           executor = Executors.newCachedThreadPool();
        // Wait until all results are available and combine them at the same time

        System.out.println("VHI -  closing connection...");
        tdb.closeConnection();

        List<FutureTask> taskList = new ArrayList<FutureTask>();
        i=1;
        for (icount = xmin; icount <= xmax; icount+=w_part){
            for (jcount = ymin; jcount <= ymax; jcount+=h_part){

                System.out.println("VHI init instance n. "+i);
                FutureTask futureTask_n = new FutureTask(new RasterEngineCallable("VHI Thread-"+i,
                        icount,jcount,(icount+w_part),(jcount+h_part),"vhi",year,doy));

                taskList.add(futureTask_n);

                executor.execute(futureTask_n);
                i++;
            }
        }
        System.out.println("VHI MC : max pool - "+((nthread*nthread)+5)+" tot assigned: "+i);

        for (FutureTask futureTask : taskList) {
            resultFuture += (Long)futureTask.get();

        }


        System.out.println("Parallel threads have been finished");

        // Shutdown the ExecutorService
        System.out.println("Shutting down executor");
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);


        //Save calculated data into spi table
        System.out.println("VHI re-opening connection...");
        tdb = new TDBManager(db_context);

        System.out.println("VHI: deleting duplicated VHI tiles");

        sqlString="select * from postgis.deduplicate_vhi("+doy+","+year+")";
        tdb.setPreparedStatementRef(sqlString);
        tdb.runPreparedQuery();
        if(tdb.next()){
            System.out.println("VHI deduplicated");
        }


        System.out.println("VHI closing connection...");
        tdb.closeConnection();


        if (!executor.isTerminated()) {
            System.err.println("Attempt to shutdown executer");
            System.err.println("using shutdownNow()");
        }
        executor.shutdownNow();
        endTime = new Timestamp(System.currentTimeMillis());
        System.out.println("ending time: "+endTime);

        return 0;


    }




}
