package com.alibaba.datax.common.statistics;

import org.junit.Assert;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.List;

/**
 * Created by liqiang on 15/8/26.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class PerfRecordTest {
    private static Logger LOG = LoggerFactory.getLogger(PerfRecordTest.class);
    private final int TGID = 1;


    @Before
    public void setUp() throws Exception {
        Field instance=PerfTrace.class.getDeclaredField("instance");
        instance.setAccessible(true);
        instance.set(null,null);
    }

    public boolean hasRecordInList(List<PerfRecord> perfRecordList,PerfRecord perfRecord){
        if(perfRecordList==null || perfRecordList.size()==0){
            return false;
        }

        for(PerfRecord perfRecord1:perfRecordList){
           if(perfRecord.equals(perfRecord1)){
               return true;
           }
        }

        return false;
    }

    @Test
    public void test001PerfRecordEquals() throws Exception {
        PerfTrace.getInstance(true, 1001, 1, 0, true);

        PerfRecord initPerfRecord = new PerfRecord(TGID, 1, PerfRecord.PHASE.WRITE_TASK_INIT);
        initPerfRecord.start();
        Thread.sleep(50);
        initPerfRecord.end();

        PerfRecord initPerfRecord2 = initPerfRecord.copy();

        Assert.assertTrue(initPerfRecord.equals(initPerfRecord2));

        PerfRecord initPerfRecord3 = new PerfRecord(TGID, 1, PerfRecord.PHASE.READ_TASK_DESTROY);
        initPerfRecord3.start();
        Thread.sleep(1050);
        initPerfRecord3.end();

        Assert.assertTrue(!initPerfRecord.equals(initPerfRecord3));

        PerfRecord initPerfRecord4 = new PerfRecord(TGID, 1, PerfRecord.PHASE.WRITE_TASK_INIT);
        initPerfRecord4.start();
        Thread.sleep(2050);
        initPerfRecord4.end();

        System.out.println(initPerfRecord4.toString());
        System.out.println(initPerfRecord.toString());

        Assert.assertTrue(!initPerfRecord.equals(initPerfRecord4));

        PerfRecord initPerfRecord5 = new PerfRecord(TGID, 1, PerfRecord.PHASE.WRITE_TASK_INIT);
        initPerfRecord5.start();
        Thread.sleep(50);
        initPerfRecord5.end();

        initPerfRecord5.addCount(100);
        initPerfRecord5.addSize(200);

        Assert.assertTrue(!initPerfRecord.equals(initPerfRecord5));

        PerfRecord initPerfRecord6 = initPerfRecord.copy();
        initPerfRecord6.addCount(1001);
        initPerfRecord6.addSize(1001);

        Assert.assertTrue(initPerfRecord.equals(initPerfRecord6));

    }

    @Test
    public void test002Normal() throws Exception {

        PerfTrace.getInstance(true, 1001, 1, 0, true);

        PerfRecord initPerfRecord = new PerfRecord(TGID, 1, PerfRecord.PHASE.WRITE_TASK_INIT);
        initPerfRecord.start();
        Thread.sleep(1050);
        initPerfRecord.end();

        Assert.assertTrue(initPerfRecord.getAction().name().equals("end"));
        Assert.assertTrue(initPerfRecord.getElapsedTimeInNs() >= 1050000000);
        Assert.assertTrue(PerfTrace.getInstance().getPerfRecordMaps().get(PerfRecord.PHASE.WRITE_TASK_INIT).getTotalCount() == 1);
        Assert.assertTrue(hasRecordInList(PerfTrace.getInstance().getWaitingReportList(), initPerfRecord));


        LOG.debug("task writer starts to do prepare ...");
        PerfRecord preparePerfRecord = new PerfRecord(TGID, 1, PerfRecord.PHASE.WRITE_TASK_PREPARE);
        preparePerfRecord.start();
        Thread.sleep(1020);
        preparePerfRecord.end();

        Assert.assertTrue(preparePerfRecord.getAction().name().equals("end"));
        Assert.assertTrue(preparePerfRecord.getElapsedTimeInNs() >= 1020000000);
        Assert.assertTrue(PerfTrace.getInstance().getPerfRecordMaps().get(PerfRecord.PHASE.WRITE_TASK_PREPARE).getTotalCount() == 1);
        Assert.assertTrue(hasRecordInList(PerfTrace.getInstance().getWaitingReportList(), preparePerfRecord));


        LOG.debug("task writer starts to write ...");
        PerfRecord dataPerfRecord = new PerfRecord(TGID, 1, PerfRecord.PHASE.READ_TASK_DATA);
        dataPerfRecord.start();

        Thread.sleep(1200);
        dataPerfRecord.addCount(1001);
        dataPerfRecord.addSize(1002);
        dataPerfRecord.end();

        Assert.assertTrue(dataPerfRecord.getAction().name().equals("end"));
        Assert.assertTrue(dataPerfRecord.getElapsedTimeInNs() >= 1020000000);
        Assert.assertTrue(dataPerfRecord.getCount() == 1001);
        Assert.assertTrue(dataPerfRecord.getSize() == 1002);
        Assert.assertTrue(PerfTrace.getInstance().getPerfRecordMaps().get(PerfRecord.PHASE.READ_TASK_DATA).getTotalCount() == 1);
        Assert.assertTrue(hasRecordInList(PerfTrace.getInstance().getWaitingReportList(), dataPerfRecord));


        PerfRecord destoryPerfRecord = new PerfRecord(TGID, 1, PerfRecord.PHASE.READ_TASK_DESTROY);
        destoryPerfRecord.start();

        Thread.sleep(250);
        destoryPerfRecord.end();

        Assert.assertTrue(destoryPerfRecord.getAction().name().equals("end"));
        Assert.assertTrue(destoryPerfRecord.getElapsedTimeInNs() >= 250000000);
        Assert.assertTrue(PerfTrace.getInstance().getPerfRecordMaps().get(PerfRecord.PHASE.READ_TASK_DESTROY).getTotalCount() == 1);
        Assert.assertTrue(hasRecordInList(PerfTrace.getInstance().getWaitingReportList(), destoryPerfRecord));

        PerfRecord waitTimePerfRecord = new PerfRecord(TGID, 1, PerfRecord.PHASE.WAIT_READ_TIME);
        waitTimePerfRecord.start();

        Thread.sleep(250);
        waitTimePerfRecord.end();

        Assert.assertTrue(waitTimePerfRecord.getAction().name().equals("end"));
        Assert.assertTrue(waitTimePerfRecord.getElapsedTimeInNs() >= 250000000);
        Assert.assertTrue(PerfTrace.getInstance().getPerfRecordMaps().get(PerfRecord.PHASE.WAIT_READ_TIME).getTotalCount() == 1);
        Assert.assertTrue(hasRecordInList(PerfTrace.getInstance().getWaitingReportList(), waitTimePerfRecord));



        PerfRecord initPerfRecord2 = new PerfRecord(TGID, 2, PerfRecord.PHASE.WRITE_TASK_INIT);
        initPerfRecord2.start();
        Thread.sleep(50);
        initPerfRecord2.end();

        Assert.assertTrue(initPerfRecord2.getAction().name().equals("end"));
        Assert.assertTrue(initPerfRecord2.getElapsedTimeInNs() >= 50000000);
        Assert.assertTrue(PerfTrace.getInstance().getPerfRecordMaps().get(PerfRecord.PHASE.WRITE_TASK_INIT).getTotalCount() == 2);
        Assert.assertTrue(hasRecordInList(PerfTrace.getInstance().getWaitingReportList(), initPerfRecord2));

        LOG.debug("task writer starts to do prepare ...");
        PerfRecord preparePerfRecord2 = new PerfRecord(TGID, 2, PerfRecord.PHASE.WRITE_TASK_PREPARE);
        preparePerfRecord2.start();
        Thread.sleep(20);
        preparePerfRecord2.end();
        LOG.debug("task writer starts to write ...");

        Assert.assertTrue(preparePerfRecord2.getAction().name().equals("end"));
        Assert.assertTrue(preparePerfRecord2.getElapsedTimeInNs() >= 20000000);
        Assert.assertTrue(PerfTrace.getInstance().getPerfRecordMaps().get(PerfRecord.PHASE.WRITE_TASK_PREPARE).getTotalCount() == 2);
        Assert.assertTrue(hasRecordInList(PerfTrace.getInstance().getWaitingReportList(), preparePerfRecord2));


        PerfRecord dataPerfRecor2 = new PerfRecord(TGID, 2, PerfRecord.PHASE.READ_TASK_DATA);
        dataPerfRecor2.start();

        Thread.sleep(2200);
        dataPerfRecor2.addCount(2001);
        dataPerfRecor2.addSize(2002);
        dataPerfRecor2.end();

        Assert.assertTrue(dataPerfRecor2.getAction().name().equals("end"));
        Assert.assertTrue(dataPerfRecor2.getElapsedTimeInNs() >= 2200000000L);
        Assert.assertTrue(dataPerfRecor2.getCount() == 2001);
        Assert.assertTrue(dataPerfRecor2.getSize() == 2002);
        Assert.assertTrue(PerfTrace.getInstance().getPerfRecordMaps().get(PerfRecord.PHASE.READ_TASK_DATA).getTotalCount() == 2);
        Assert.assertTrue(hasRecordInList(PerfTrace.getInstance().getWaitingReportList(), dataPerfRecor2));


        PerfRecord destoryPerfRecord2 = new PerfRecord(TGID, 2, PerfRecord.PHASE.READ_TASK_DESTROY);
        destoryPerfRecord2.start();

        Thread.sleep(1250);
        destoryPerfRecord2.end();

        Assert.assertTrue(destoryPerfRecord2.getAction().name().equals("end"));
        Assert.assertTrue(destoryPerfRecord2.getElapsedTimeInNs() >= 1250000000);
        Assert.assertTrue(PerfTrace.getInstance().getPerfRecordMaps().get(PerfRecord.PHASE.READ_TASK_DESTROY).getTotalCount() == 2);
        Assert.assertTrue(hasRecordInList(PerfTrace.getInstance().getWaitingReportList(), destoryPerfRecord2));

        PerfRecord waitPerfRecord2 = new PerfRecord(TGID, 2, PerfRecord.PHASE.WAIT_READ_TIME);
        waitPerfRecord2.start();

        Thread.sleep(1250);
        waitPerfRecord2.end();

        Assert.assertTrue(waitPerfRecord2.getAction().name().equals("end"));
        Assert.assertTrue(waitPerfRecord2.getElapsedTimeInNs() >= 1250000000);
        Assert.assertTrue(PerfTrace.getInstance().getPerfRecordMaps().get(PerfRecord.PHASE.WAIT_READ_TIME).getTotalCount() == 2);
        Assert.assertTrue(hasRecordInList(PerfTrace.getInstance().getWaitingReportList(), waitPerfRecord2));


        PerfTrace.getInstance().addTaskDetails(1, " ");
        PerfTrace.getInstance().addTaskDetails(1, "task 1 some thing abcdf");
        PerfTrace.getInstance().addTaskDetails(2,"before char");
        PerfTrace.getInstance().addTaskDetails(2,"task 2 some thing abcdf");

        Assert.assertTrue(PerfTrace.getInstance().getTaskDetails().get(1).equals("task 1 some thing abcdf"));
        Assert.assertTrue(PerfTrace.getInstance().getTaskDetails().get(2).equals("before char,task 2 some thing abcdf"));
        System.out.println(PerfTrace.getInstance().summarizeNoException());
    }
    @Test
    public void test003Disable() throws Exception {

        PerfTrace.getInstance(true, 1001, 1, 0, false);

        PerfRecord initPerfRecord = new PerfRecord(TGID, 1, PerfRecord.PHASE.WRITE_TASK_INIT);
        initPerfRecord.start();
        Thread.sleep(1050);
        initPerfRecord.end();

        Assert.assertTrue(initPerfRecord.getDatetime().equals("null time"));
        Assert.assertTrue(initPerfRecord.getElapsedTimeInNs() == -1);
        Assert.assertTrue(PerfTrace.getInstance().getPerfRecordMaps().get(PerfRecord.PHASE.WRITE_TASK_INIT) == null);


        LOG.debug("task writer starts to do prepare ...");
        PerfRecord preparePerfRecord = new PerfRecord(TGID, 1, PerfRecord.PHASE.WRITE_TASK_PREPARE);
        preparePerfRecord.start();
        Thread.sleep(1020);
        preparePerfRecord.end();
        LOG.debug("task writer starts to write ...");

        Assert.assertTrue(preparePerfRecord.getDatetime().equals("null time"));
        Assert.assertTrue(preparePerfRecord.getElapsedTimeInNs() == -1);
        Assert.assertTrue(PerfTrace.getInstance().getPerfRecordMaps().get(PerfRecord.PHASE.WRITE_TASK_PREPARE) == null);


        PerfRecord dataPerfRecord = new PerfRecord(TGID, 1, PerfRecord.PHASE.READ_TASK_DATA);
        dataPerfRecord.start();

        Thread.sleep(1200);
        dataPerfRecord.addCount(1001);
        dataPerfRecord.addSize(1001);
        dataPerfRecord.end();

        Assert.assertTrue(dataPerfRecord.getDatetime().equals("null time"));
        Assert.assertTrue(dataPerfRecord.getElapsedTimeInNs() == -1);
        Assert.assertTrue(PerfTrace.getInstance().getPerfRecordMaps().get(PerfRecord.PHASE.READ_TASK_DATA) == null);

        PerfRecord waitPerfRecor1 = new PerfRecord(TGID, 1, PerfRecord.PHASE.WAIT_WRITE_TIME);
        waitPerfRecor1.start();

        Thread.sleep(2200);
        waitPerfRecor1.end();

        Assert.assertTrue(waitPerfRecor1.getDatetime().equals("null time"));
        Assert.assertTrue(waitPerfRecor1.getElapsedTimeInNs() == -1);
        Assert.assertTrue(PerfTrace.getInstance().getPerfRecordMaps().get(PerfRecord.PHASE.WAIT_WRITE_TIME) == null);


        PerfRecord initPerfRecord2 = new PerfRecord(TGID, 2, PerfRecord.PHASE.WRITE_TASK_INIT);
        initPerfRecord2.start();
        Thread.sleep(50);
        initPerfRecord2.end();

        Assert.assertTrue(initPerfRecord2.getDatetime().equals("null time"));
        Assert.assertTrue(initPerfRecord2.getElapsedTimeInNs() == -1);
        Assert.assertTrue(PerfTrace.getInstance().getPerfRecordMaps().get(PerfRecord.PHASE.WRITE_TASK_INIT) == null);

        LOG.debug("task writer starts to do prepare ...");
        PerfRecord preparePerfRecord2 = new PerfRecord(TGID, 2, PerfRecord.PHASE.WRITE_TASK_PREPARE);
        preparePerfRecord2.start();
        Thread.sleep(20);
        preparePerfRecord2.end();
        LOG.debug("task writer starts to write ...");

        Assert.assertTrue(preparePerfRecord2.getDatetime().equals("null time"));
        Assert.assertTrue(preparePerfRecord2.getElapsedTimeInNs() == -1);
        Assert.assertTrue(PerfTrace.getInstance().getPerfRecordMaps().get(PerfRecord.PHASE.WRITE_TASK_PREPARE) == null);



        PerfRecord dataPerfRecor2 = new PerfRecord(TGID, 2, PerfRecord.PHASE.READ_TASK_DATA);
        dataPerfRecor2.start();

        Thread.sleep(2200);
        dataPerfRecor2.addCount(2001);
        dataPerfRecor2.addSize(2001);
        dataPerfRecor2.end();

        Assert.assertTrue(dataPerfRecor2.getDatetime().equals("null time"));
        Assert.assertTrue(dataPerfRecor2.getElapsedTimeInNs() == -1);
        Assert.assertTrue(PerfTrace.getInstance().getPerfRecordMaps().get(PerfRecord.PHASE.READ_TASK_DATA) == null);

        PerfRecord waitPerfRecor2 = new PerfRecord(TGID, 2, PerfRecord.PHASE.WAIT_WRITE_TIME);
        waitPerfRecor2.start();

        Thread.sleep(2200);
        waitPerfRecor2.end();

        Assert.assertTrue(waitPerfRecor2.getDatetime().equals("null time"));
        Assert.assertTrue(waitPerfRecor2.getElapsedTimeInNs() == -1);
        Assert.assertTrue(PerfTrace.getInstance().getPerfRecordMaps().get(PerfRecord.PHASE.WAIT_WRITE_TIME) == null);

        PerfTrace.getInstance().addTaskDetails(1, "task 1 some thing abcdf");
        PerfTrace.getInstance().addTaskDetails(2, "task 2 some thing abcdf");

        Assert.assertTrue(PerfTrace.getInstance().getTaskDetails().size()==0);
        System.out.println(PerfTrace.getInstance().summarizeNoException());
    }

    @Test
    public void test004Normal2() throws Exception {
        int priority = 0;
        try {
            priority = Integer.parseInt(System.getenv("SKYNET_PRIORITY"));
        }catch (NumberFormatException e){
            LOG.warn("prioriy set to 0, because NumberFormatException, the value is: "+System.getProperty("PROIORY"));
        }

        System.out.println("priority====" + priority);

        PerfTrace.getInstance(false, 1001001001001L, 1, 0, true);

        PerfRecord initPerfRecord = new PerfRecord(TGID, 10000001, PerfRecord.PHASE.WRITE_TASK_INIT);
        initPerfRecord.start();
        Thread.sleep(1050);
        initPerfRecord.end();

        Assert.assertTrue(initPerfRecord.getAction().name().equals("end"));
        Assert.assertTrue(initPerfRecord.getElapsedTimeInNs() >= 1050000000);
        Assert.assertTrue(PerfTrace.getInstance().getPerfRecordMaps().get(PerfRecord.PHASE.WRITE_TASK_INIT).getTotalCount() == 1);
        Assert.assertTrue(hasRecordInList(PerfTrace.getInstance().getWaitingReportList(), initPerfRecord));


        LOG.debug("task writer starts to do prepare ...");
        PerfRecord preparePerfRecord = new PerfRecord(TGID, 10000001, PerfRecord.PHASE.WRITE_TASK_PREPARE);
        preparePerfRecord.start();
        Thread.sleep(1020);
        preparePerfRecord.end();

        Assert.assertTrue(preparePerfRecord.getAction().name().equals("end"));
        Assert.assertTrue(preparePerfRecord.getElapsedTimeInNs() >= 1020000000);
        Assert.assertTrue(PerfTrace.getInstance().getPerfRecordMaps().get(PerfRecord.PHASE.WRITE_TASK_PREPARE).getTotalCount() == 1);
        Assert.assertTrue(hasRecordInList(PerfTrace.getInstance().getWaitingReportList(), preparePerfRecord));

        LOG.debug("task wait time  ...");
        PerfRecord waitPerfRecord = new PerfRecord(TGID, 10000001, PerfRecord.PHASE.WAIT_WRITE_TIME);
        waitPerfRecord.start();
        Thread.sleep(1030);
        waitPerfRecord.end();

        Assert.assertTrue(waitPerfRecord.getAction().name().equals("end"));
        Assert.assertTrue(waitPerfRecord.getElapsedTimeInNs() >= 1030000000);
        Assert.assertTrue(PerfTrace.getInstance().getPerfRecordMaps().get(PerfRecord.PHASE.WAIT_WRITE_TIME).getTotalCount() == 1);
        Assert.assertTrue(hasRecordInList(PerfTrace.getInstance().getWaitingReportList(), waitPerfRecord));



        LOG.debug("task writer starts to write ...");

        PerfRecord dataPerfRecord = new PerfRecord(TGID, 10000001, PerfRecord.PHASE.READ_TASK_DATA);
        dataPerfRecord.start();

        Thread.sleep(1200);
        dataPerfRecord.addCount(1001);
        dataPerfRecord.addSize(1002);
        dataPerfRecord.end();

        Assert.assertTrue(dataPerfRecord.getAction().name().equals("end"));
        Assert.assertTrue(dataPerfRecord.getElapsedTimeInNs() >= 1020000000);
        Assert.assertTrue(dataPerfRecord.getCount() == 1001);
        Assert.assertTrue(dataPerfRecord.getSize() == 1002);
        Assert.assertTrue(PerfTrace.getInstance().getPerfRecordMaps().get(PerfRecord.PHASE.READ_TASK_DATA).getTotalCount() == 1);
        Assert.assertTrue(hasRecordInList(PerfTrace.getInstance().getWaitingReportList(), dataPerfRecord));


        PerfRecord initPerfRecord2 = new PerfRecord(TGID, 10000002, PerfRecord.PHASE.WRITE_TASK_INIT);
        initPerfRecord2.start();
        Thread.sleep(50);
        initPerfRecord2.end();

        Assert.assertTrue(initPerfRecord2.getAction().name().equals("end"));
        Assert.assertTrue(initPerfRecord2.getElapsedTimeInNs() >= 50000000);
        Assert.assertTrue(PerfTrace.getInstance().getPerfRecordMaps().get(PerfRecord.PHASE.WRITE_TASK_INIT).getTotalCount() == 2);
        Assert.assertTrue(hasRecordInList(PerfTrace.getInstance().getWaitingReportList(), initPerfRecord2));

        LOG.debug("task wait time  ...");
        PerfRecord waitPerfRecord2 = new PerfRecord(TGID, 10000002, PerfRecord.PHASE.WAIT_WRITE_TIME);
        waitPerfRecord2.start();
        Thread.sleep(2030);
        waitPerfRecord2.end();

        Assert.assertTrue(waitPerfRecord2.getAction().name().equals("end"));
        Assert.assertTrue(waitPerfRecord2.getElapsedTimeInNs() >= 2030000000);
        Assert.assertTrue(PerfTrace.getInstance().getPerfRecordMaps().get(PerfRecord.PHASE.WAIT_WRITE_TIME).getTotalCount() == 2);
        Assert.assertTrue(hasRecordInList(PerfTrace.getInstance().getWaitingReportList(), waitPerfRecord2));


        LOG.debug("task writer starts to do prepare ...");
        PerfRecord preparePerfRecord2 = new PerfRecord(TGID, 10000002, PerfRecord.PHASE.WRITE_TASK_PREPARE);
        preparePerfRecord2.start();
        Thread.sleep(20);
        preparePerfRecord2.end();

        Assert.assertTrue(preparePerfRecord2.getAction().name().equals("end"));
        Assert.assertTrue(preparePerfRecord2.getElapsedTimeInNs() >= 20000000);
        Assert.assertTrue(PerfTrace.getInstance().getPerfRecordMaps().get(PerfRecord.PHASE.WRITE_TASK_PREPARE).getTotalCount() == 2);
        Assert.assertTrue(hasRecordInList(PerfTrace.getInstance().getWaitingReportList(), preparePerfRecord2));


        LOG.debug("task writer starts to write ...");

        PerfRecord dataPerfRecor2 = new PerfRecord(TGID, 10000002, PerfRecord.PHASE.READ_TASK_DATA);
        dataPerfRecor2.start();

        Thread.sleep(2200);
        dataPerfRecor2.addCount(2001);
        dataPerfRecor2.addSize(2002);
        dataPerfRecor2.end();

        Assert.assertTrue(dataPerfRecor2.getAction().name().equals("end"));
        Assert.assertTrue(dataPerfRecor2.getElapsedTimeInNs() >= 2200000000L);
        Assert.assertTrue(dataPerfRecor2.getCount() == 2001);
        Assert.assertTrue(dataPerfRecor2.getSize() == 2002);
        Assert.assertTrue(PerfTrace.getInstance().getPerfRecordMaps().get(PerfRecord.PHASE.READ_TASK_DATA).getTotalCount() == 2);
        Assert.assertTrue(hasRecordInList(PerfTrace.getInstance().getWaitingReportList(), dataPerfRecor2));


        PerfTrace.getInstance().addTaskDetails(10000001, "task 100000011 some thing abcdf");
        PerfTrace.getInstance().addTaskDetails(10000002, "task 100000012 some thing abcdf");
        PerfTrace.getInstance().addTaskDetails(10000004, "task 100000012 some thing abcdf?123?345");
        PerfTrace.getInstance().addTaskDetails(10000005, "task 100000012 some thing abcdf?456");
        PerfTrace.getInstance().addTaskDetails(10000006, "[task 100000012? some thing abcdf?456");

        Assert.assertTrue(PerfTrace.getInstance().getTaskDetails().get(10000001).equals("task 100000011 some thing abcdf"));
        Assert.assertTrue(PerfTrace.getInstance().getTaskDetails().get(10000002).equals("task 100000012 some thing abcdf"));

        PerfRecord.addPerfRecord(TGID, 10000003, PerfRecord.PHASE.TASK_TOTAL, System.currentTimeMillis(), 12300123L * 1000L * 1000L);
        PerfRecord.addPerfRecord(TGID, 10000004, PerfRecord.PHASE.TASK_TOTAL, System.currentTimeMillis(), 22300123L * 1000L * 1000L);
        PerfRecord.addPerfRecord(TGID, 10000005, PerfRecord.PHASE.SQL_QUERY, System.currentTimeMillis(), 4L);
        PerfRecord.addPerfRecord(TGID, 10000006, PerfRecord.PHASE.RESULT_NEXT_ALL, System.currentTimeMillis(), 3000L);
        PerfRecord.addPerfRecord(TGID, 10000006, PerfRecord.PHASE.ODPS_BLOCK_CLOSE, System.currentTimeMillis(), 2000000L);

        System.out.println(PerfTrace.getInstance().summarizeNoException());


    }

}