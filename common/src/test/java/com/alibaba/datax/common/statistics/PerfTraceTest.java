package com.alibaba.datax.common.statistics;

import com.alibaba.datax.common.statistics.PerfRecord.PHASE;
import org.junit.Test;

import java.util.Random;

/**
 * Created by liqiang on 16/1/28.
 */
public class PerfTraceTest {
    private Random random = new Random();
    private boolean[] isend = new boolean[]{true,false};
    private PHASE[] phases = new PHASE[]{PHASE.TASK_TOTAL ,PHASE.READ_TASK_INIT ,
            PHASE.READ_TASK_PREPARE ,PHASE.READ_TASK_DATA ,PHASE.READ_TASK_POST ,PHASE.READ_TASK_DESTROY
            ,PHASE.WRITE_TASK_INIT ,PHASE.WRITE_TASK_PREPARE ,PHASE.WRITE_TASK_DATA ,PHASE.WRITE_TASK_POST
            ,PHASE.WRITE_TASK_DESTROY ,PHASE.SQL_QUERY ,PHASE.RESULT_NEXT_ALL ,PHASE.ODPS_BLOCK_CLOSE
            ,PHASE.WAIT_READ_TIME ,PHASE.WAIT_WRITE_TIME};
    @Test
    public void testMultiThread() throws Exception {
       final PerfTrace perfTrace = PerfTrace.getInstance(true, 1001, 1, 0, true);

        Thread[] threads = new Thread[41];
        for(int i=0;i<40;i++){
            threads[i] = new Thread(new Runnable() {
                @Override
                public void run() {
                    for(int j=0;j<200;j++) {
                        PerfRecord perfRecord = new PerfRecord(random.nextInt(10), random.nextInt(128), phases[random.nextInt(15)]);
                        perfRecord.start();
                        if (isend[random.nextInt(2)]) {
                            perfRecord.end();
                        }
                        perfTrace.tracePerfRecord(perfRecord);
                        try {
                            Thread.sleep(500);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }

                }
            });
            threads[i].setName("test"+i);
        }

        threads[40] = new Thread(new Runnable() {
            @Override
            public void run() {
                int i = 0;
                while(true) {
                    perfTrace.getReports("job");
                    i++;
                    System.out.println("dddddddddddddddddddd dddddddddd get Report job done. " + i);
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        });
        threads[40].setName("40_report");

        for(Thread t:threads){
            t.start();
        }

        for(int i=0;i<40;i++){
            threads[i].join();
        }

        System.out.println("ooooooooooooooooooooooooooooooooooooooooooooooooo");
        System.out.println("ooooooooooooooooooooooooooooooooooooooooooooooooo");

    }
}