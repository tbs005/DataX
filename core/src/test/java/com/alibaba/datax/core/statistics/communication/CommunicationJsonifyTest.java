package com.alibaba.datax.core.statistics.communication;

import com.alibaba.datax.core.job.meta.State;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.junit.Assert;
import org.junit.Test;

public class CommunicationJsonifyTest {
    @Test
    public void testJsonGetSnapshot() {
        Communication communication = new Communication();
        communication.setLongCounter(CommunicationTool.STAGE, 10);
        communication.setLongCounter(CommunicationTool.READ_SUCCEED_RECORDS, 100);
        communication.setLongCounter(CommunicationTool.READ_SUCCEED_BYTES, 102400);
        communication.setLongCounter(CommunicationTool.BYTE_SPEED, 10240);
        communication.setLongCounter(CommunicationTool.RECORD_SPEED, 100);
        communication.setDoubleCounter(CommunicationTool.PERCENTAGE, 0.1);
        communication.setState(State.RUNNING);
        communication.setLongCounter(CommunicationTool.WRITE_RECEIVED_RECORDS, 99);
        communication.setLongCounter(CommunicationTool.WRITE_RECEIVED_BYTES, 102300);

        String jsonString = CommunicationTool.Jsonify.getSnapshot(communication);
        JSONObject metricJson = JSON.parseObject(jsonString);

        Assert.assertEquals(communication.getLongCounter(CommunicationTool.RECORD_SPEED),
                metricJson.getLong("speedRecords"));
        Assert.assertEquals(communication.getDoubleCounter(CommunicationTool.PERCENTAGE),
                metricJson.getDouble("percentage"));
    }
}
