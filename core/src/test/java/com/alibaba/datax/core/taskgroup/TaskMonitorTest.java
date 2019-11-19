package com.alibaba.datax.core.taskgroup;

import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.CommunicationTool;
import com.alibaba.datax.dataxservice.face.domain.enums.State;
import org.junit.Assert;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by liqiang on 15/7/26.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TaskMonitorTest {



    TaskMonitor taskMonitor;// = TaskMonitor.getInstance();
    private ConcurrentHashMap<Integer, TaskMonitor.TaskCommunication> tasks;

    @Before
    public void setUp() throws Exception {
        Class clazz = Class.forName("com.alibaba.datax.core.taskgroup.TaskMonitor");
        Constructor c =clazz.getDeclaredConstructor();
        c.setAccessible(true);
        taskMonitor = (TaskMonitor)c.newInstance();
        Field tasks = taskMonitor.getClass().getDeclaredField("tasks");
        tasks.setAccessible(true);
        this.tasks = (ConcurrentHashMap<Integer, TaskMonitor.TaskCommunication>) tasks.get(taskMonitor);

    }

    @Test
    public void testNormal() throws Exception {

        //register task
        long ttl = System.currentTimeMillis();

        Communication communication1 = new Communication();

        taskMonitor.registerTask(1, communication1);

        TaskMonitor.TaskCommunication taskCommunication1 = taskMonitor.getTaskCommunication(1);

        Assert.assertEquals(taskCommunication1.getLastAllReadRecords(), 0L);
        Assert.assertEquals(this.tasks.size(), 1);

        Assert.assertTrue(taskCommunication1.getLastUpdateComunicationTS() >= ttl);
        Assert.assertTrue(taskCommunication1.getTtl() >= ttl);

        // report 没有任何变化的communication

        long oldTS = taskCommunication1.getLastUpdateComunicationTS();
        long oldTTL = taskCommunication1.getTtl();
        Thread.sleep(1000);

        taskMonitor.report(1, communication1);

        TaskMonitor.TaskCommunication taskCommunication1_1 = taskMonitor.getTaskCommunication(1);

        Assert.assertEquals(taskCommunication1_1.getLastAllReadRecords(), 0L);
        Assert.assertEquals(taskCommunication1_1.getLastUpdateComunicationTS(), oldTS);
        Assert.assertTrue(taskCommunication1_1.getTtl() > oldTTL);

        // report 已经finish的communication
        Communication communication2 = new Communication();
        communication2.setState(State.KILLED);

        taskMonitor.registerTask(2, communication2);
        Assert.assertEquals(this.tasks.size(), 1);


        // report 另一个communication
        Communication communication3 = new Communication();
        taskMonitor.registerTask(3, communication3);

        Assert.assertEquals(this.tasks.size(), 2);
        System.out.println(this.tasks);

        //report communication

        ttl = System.currentTimeMillis();

        communication1.setLongCounter(CommunicationTool.READ_SUCCEED_RECORDS, 100);
        communication3.setLongCounter(CommunicationTool.READ_FAILED_RECORDS, 10);

        taskMonitor.report(1, communication1);
        taskMonitor.report(3, communication3);

        taskCommunication1 = taskMonitor.getTaskCommunication(1);

        Assert.assertEquals(taskCommunication1.getLastAllReadRecords(), 100L);
        Assert.assertEquals(this.tasks.size(), 2);

        Assert.assertTrue(taskCommunication1.getLastUpdateComunicationTS() >= ttl);
        Assert.assertTrue(taskCommunication1.getTtl() >= ttl);

        TaskMonitor.TaskCommunication taskCommunication3 = taskMonitor.getTaskCommunication(3);

        Assert.assertEquals(taskCommunication3.getLastAllReadRecords(), 10L);
        Assert.assertEquals(this.tasks.size(), 2);

        Assert.assertTrue(taskCommunication3.getLastUpdateComunicationTS() >= ttl);
        Assert.assertTrue(taskCommunication3.getTtl() >= ttl);

        //继续report
        ttl = System.currentTimeMillis();

        communication1.setLongCounter(CommunicationTool.READ_SUCCEED_RECORDS, 1001);
        communication3.setLongCounter(CommunicationTool.READ_FAILED_RECORDS, 101);

        taskMonitor.report(1, communication1);
        taskMonitor.report(3, communication3);

        taskCommunication1 = taskMonitor.getTaskCommunication(1);

        Assert.assertEquals(taskCommunication1.getLastAllReadRecords(), 1001L);
        Assert.assertEquals(this.tasks.size(), 2);

        Assert.assertTrue(taskCommunication1.getLastUpdateComunicationTS() >= ttl);
        Assert.assertTrue(taskCommunication1.getTtl() >= ttl);

        taskCommunication3 = taskMonitor.getTaskCommunication(3);

        Assert.assertEquals(taskCommunication3.getLastAllReadRecords(), 101L);
        Assert.assertEquals(this.tasks.size(), 2);

        Assert.assertTrue(taskCommunication3.getLastUpdateComunicationTS() >= ttl);
        Assert.assertTrue(taskCommunication3.getTtl() >= ttl);

        // 设置EXPIRED_TIME
        Field EXPIRED_TIME = taskMonitor.getClass().getDeclaredField("EXPIRED_TIME");
        EXPIRED_TIME.setAccessible(true);
        EXPIRED_TIME.set(null, 1000);

        Thread.sleep(2000);

        //超时没有变更
        taskMonitor.report(1, communication1);

        System.out.println(communication1.getCounter());
        System.out.println(communication1.getThrowable());
        System.out.println(communication1.getThrowableMessage());
        System.out.println(communication1.getState());

        Assert.assertTrue(communication1.getThrowableMessage().contains("任务hung住，Expired"));
        Assert.assertEquals(communication1.getState(), State.FAILED);

        // communicatio1 已经fail， communication3 在超时后进行变更，update正常
        ttl = System.currentTimeMillis();

        communication1.setLongCounter(CommunicationTool.READ_SUCCEED_RECORDS, 2001);
        communication3.setLongCounter(CommunicationTool.READ_FAILED_RECORDS, 201);

        taskMonitor.report(1, communication1);
        taskMonitor.report(3, communication3);


        taskCommunication1 = taskMonitor.getTaskCommunication(1);

        Assert.assertEquals(taskCommunication1.getLastAllReadRecords(), 1001L);
        Assert.assertEquals(this.tasks.size(), 2);

        Assert.assertTrue(communication1.getThrowableMessage().contains("任务hung住，Expired"));
        Assert.assertEquals(communication1.getState(), State.FAILED);

        taskCommunication3 = taskMonitor.getTaskCommunication(3);

        Assert.assertEquals(taskCommunication3.getLastAllReadRecords(), 201L);
        Assert.assertEquals(this.tasks.size(), 2);

        Assert.assertTrue(taskCommunication3.getLastUpdateComunicationTS() >= ttl);
        Assert.assertTrue(taskCommunication3.getTtl() >= ttl);


        //remove 1
        taskMonitor.removeTask(1);
        Assert.assertEquals(this.tasks.size(), 1);

        //remove 3
        taskMonitor.removeTask(3);
        Assert.assertEquals(this.tasks.size(), 0);

        // 没有register communication3 直接report
        ttl = System.currentTimeMillis();

        communication3.setLongCounter(CommunicationTool.READ_FAILED_RECORDS, 301);

        taskMonitor.report(3, communication3);

        taskCommunication3 = taskMonitor.getTaskCommunication(3);

        Assert.assertEquals(taskCommunication3.getLastAllReadRecords(), 301L);
        Assert.assertEquals(this.tasks.size(), 1);

        Assert.assertTrue(taskCommunication3.getLastUpdateComunicationTS() >= ttl);
        Assert.assertTrue(taskCommunication3.getTtl() >= ttl);
    }
}
