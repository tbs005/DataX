package com.alibaba.datax.core.statistics.collector;

import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.LocalTGCommunicationManager;
import com.alibaba.datax.core.statistics.container.collector.ProcessInnerCollector;
import com.alibaba.datax.core.util.ReflectUtil;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by hongjiao.hj on 2014/12/21.
 */
public class ProcessInnerCollectorTest {
    @Test
    public void testCollectFromTaskGroup() throws NoSuchFieldException, IllegalAccessException {
        Integer taskGroupId_1 = 1;
        Integer taskGroupId_2 = 2;
        Communication communication_1 = new Communication();
        communication_1.setLongCounter("totalBytes",888);
        Communication communication_2 = new Communication();
        communication_2.setLongCounter("totalBytes",112);

        ConcurrentHashMap<Integer, Communication> taskGroupCommunicationMap = new ConcurrentHashMap<Integer, Communication>();
        taskGroupCommunicationMap.put(taskGroupId_1,communication_1);
        taskGroupCommunicationMap.put(taskGroupId_2,communication_2);

        ReflectUtil.setField(new LocalTGCommunicationManager(),"taskGroupCommunicationMap",taskGroupCommunicationMap);

        ProcessInnerCollector processInnerCollector = new ProcessInnerCollector(0L);
        Communication comm = processInnerCollector.collectFromTaskGroup();
        Assert.assertTrue(comm.getLongCounter("totalBytes") == 1000);
        System.out.println(comm);
    }
}
