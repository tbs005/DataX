package com.alibaba.datax.plugin.reader.otsstreamreader.internal.functiontest;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.OTSStreamReader;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.common.ConfigurationHelper;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.common.TestHelper;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.config.OTSStreamReaderConfig;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.config.OTSStreamReaderConstants;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.utils.OTSHelper;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.utils.TimeUtils;
import com.alicloud.openservices.tablestore.*;
import com.alicloud.openservices.tablestore.model.*;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ParameterTest {

    private static String dataTable = "DataTable_ParameterTest";
    private static String statusTable = "StatusTable_ParameterTest";

    @Before
    public void before() {
        TestHelper.deleteTable(dataTable);
        TestHelper.deleteTable(statusTable);
    }

    /**
     * 参数完整性测试。完整的参数检查测试在UT中覆盖。
     */
    @Test
    public void testParameterIntegrity() {

        try {
            OTSStreamReader.Job job = new OTSStreamReader.Job();
            job.setPluginJobConf(Configuration.newDefault());
            job.init();
            fail();
        } catch (DataXException ex) {
            assertEquals("missing the key.", ex.getCause().getMessage());
        }

        try {
            Configuration configuration = ConfigurationHelper.loadConf();
            OTSStreamReader.Job job = new OTSStreamReader.Job();
            job.setPluginJobConf(configuration);
            job.init();
            fail();
        } catch (DataXException ex) {
            assertEquals("missing the key.", ex.getCause().getMessage());
        }

        try {
            Configuration configuration = ConfigurationHelper.loadConf();
            configuration.set("dataTable", dataTable);
            configuration.set("statusTable", statusTable);
            OTSStreamReader.Job job = new OTSStreamReader.Job();
            job.setPluginJobConf(configuration);
            job.init();
            fail();
        } catch (DataXException ex) {
            assertEquals("Must set date or time range millis or time range string, please check your config.", ex.getCause().getMessage());
        }
    }

    /**
     * 测试dataTable不存在、未开启Stream。
     */
    @Test
    public void testDataTableCheck() {
        try {
            Configuration configuration = ConfigurationHelper.loadConf(dataTable, statusTable, 1, 2);
            OTSStreamReader.Job job = new OTSStreamReader.Job();
            job.setPluginJobConf(configuration);
            job.init();
            fail();
        } catch (DataXException ex) {
            assertEquals("The data table is not exist.", ex.getCause().getMessage());
        }

        try {
            Configuration configuration = ConfigurationHelper.loadConf(dataTable, statusTable, 1, 2);
            OTSStreamReaderConfig config = OTSStreamReaderConfig.load(configuration);
            SyncClientInterface ots = OTSHelper.getOTSInstance(config);

            TableMeta tableMeta = new TableMeta(dataTable);
            tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema("pk", PrimaryKeyType.STRING));
            TableOptions tableOptions = new TableOptions(-1, 3);
            CreateTableRequest createTableRequest = new CreateTableRequest(tableMeta, tableOptions, new ReservedThroughput(1, 1));
            ots.createTable(createTableRequest);

            OTSStreamReader.Job job = new OTSStreamReader.Job();
            job.setPluginJobConf(configuration);
            job.init();
            fail();
        } catch (DataXException ex) {
            assertEquals("The stream of data table is not enabled.", ex.getCause().getMessage());
        }
    }

    /**
     * 测试startTime，endTime的范围不在允许区间内。
     */
    @Test
    public void testTimeRangeCheck() {
        Configuration configuration = ConfigurationHelper.loadConf(dataTable, statusTable, 1, 2);
        OTSStreamReaderConfig config = OTSStreamReaderConfig.load(configuration);
        SyncClientInterface ots = OTSHelper.getOTSInstance(config);

        TableMeta tableMeta = new TableMeta(dataTable);
        tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema("pk", PrimaryKeyType.STRING));
        TableOptions tableOptions = new TableOptions(-1, 3);
        CreateTableRequest createTableRequest = new CreateTableRequest(tableMeta, tableOptions, new ReservedThroughput(1, 1));
        createTableRequest.setStreamSpecification(new StreamSpecification(true, 1));
        ots.createTable(createTableRequest);

        long now = System.currentTimeMillis();
        long expirationTime = TimeUtils.HOUR_IN_MILLIS;
        long minTime = now - expirationTime + OTSStreamReaderConstants.BEFORE_OFFSET_TIME_MILLIS;
        long maxTime = now - OTSStreamReaderConstants.AFTER_OFFSET_TIME_MILLIS;

        long startTimes[] = {1, 2, 3, System.currentTimeMillis() / 2, minTime - 1000, maxTime + 1000, now, Long.MAX_VALUE - 100};

        for (long startTime : startTimes) {
            long endTime = startTime + 10;
            try {
                configuration = ConfigurationHelper.loadConf(dataTable, statusTable, startTime, endTime);
                OTSStreamReader.Job job = new OTSStreamReader.Job();
                job.setPluginJobConf(configuration);
                job.init();
                fail();
            } catch (DataXException ex) {
                if (startTime < minTime) {
                    assertTrue(ex.getMessage().indexOf("the start timestamp must greater than") != -1);
                } else {
                    assertTrue(ex.getMessage().indexOf("the end timestamp must smaller") != -1);
                }
            }
        }

        long startTime = minTime + 10000;
        long endTimes[] = {maxTime + 10000, maxTime * 2, now, Long.MAX_VALUE - 10};

        for (long endTime : endTimes) {
            try {
                configuration = ConfigurationHelper.loadConf(dataTable, statusTable, startTime, endTime);
                OTSStreamReader.Job job = new OTSStreamReader.Job();
                job.setPluginJobConf(configuration);
                job.init();
                fail();
            } catch (DataXException ex) {
                assertTrue(ex.getMessage().indexOf("the end timestamp must smaller") != -1);
            }
        }
    }

    /**
     * 测试statusTable的tableMeta不符合期望。
     */
    @Test
    public void testStatusTableCheck() {
        long now = System.currentTimeMillis();
        long startTime = now - TimeUtils.MINUTE_IN_MILLIS * 30;
        long endTime = now - TimeUtils.MINUTE_IN_MILLIS * 20;

        Configuration configuration = ConfigurationHelper.loadConf(dataTable, statusTable, startTime, endTime);
        OTSStreamReaderConfig config = OTSStreamReaderConfig.load(configuration);
        SyncClientInterface ots = OTSHelper.getOTSInstance(config);

        TableMeta tableMeta = new TableMeta(statusTable);
        tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema("pk", PrimaryKeyType.STRING));
        TableOptions tableOptions = new TableOptions(-1, 3);
        CreateTableRequest createTableRequest = new CreateTableRequest(tableMeta, tableOptions, new ReservedThroughput(1, 1));
        createTableRequest.setStreamSpecification(new StreamSpecification(true, 1));
        ots.createTable(createTableRequest);

        tableMeta.setTableName(dataTable);
        ots.createTable(createTableRequest);

        try {
            OTSStreamReader.Job job = new OTSStreamReader.Job();
            job.setPluginJobConf(configuration);
            job.init();
            fail();
        } catch (DataXException ex) {
            assertEquals("Unexpected table meta in status table, please check your config.", ex.getCause().getMessage());
        }
    }
}
