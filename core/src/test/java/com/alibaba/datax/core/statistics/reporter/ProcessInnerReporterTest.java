package com.alibaba.datax.core.statistics.reporter;

import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.LocalTGCommunicationManager;
import com.alibaba.datax.core.statistics.container.report.ProcessInnerReporter;
import com.alibaba.datax.core.util.ReflectUtil;
import com.alibaba.datax.dataxservice.face.domain.enums.State;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ConcurrentHashMap;


public class ProcessInnerReporterTest {

    @Test
    public void testReportJobCommunication() {
        Long jobId = 0L;
        Communication communication = new Communication();

        ProcessInnerReporter processInnerReporter = new ProcessInnerReporter();
        processInnerReporter.reportJobCommunication(jobId,communication);
        System.out.println("this function do noting");
    }

    @Test
    public void testReportTGCommunication() throws NoSuchFieldException, IllegalAccessException {
        Integer taskGroupId = 1;
        Communication communication = new Communication();
        communication.setState(State.SUBMITTING);

        ConcurrentHashMap<Integer,Communication> map = new ConcurrentHashMap<Integer, Communication>();
        map.put(taskGroupId,communication);

        ReflectUtil.setField(new LocalTGCommunicationManager(),"taskGroupCommunicationMap",map);
        ProcessInnerReporter processInnerReporter = new ProcessInnerReporter();

        Communication updateCommunication = new Communication();
        updateCommunication.setState(State.WAITING);
        processInnerReporter.reportTGCommunication(taskGroupId,updateCommunication);
        Assert.assertEquals(map.get(taskGroupId).getState(),State.WAITING);
    }
}
