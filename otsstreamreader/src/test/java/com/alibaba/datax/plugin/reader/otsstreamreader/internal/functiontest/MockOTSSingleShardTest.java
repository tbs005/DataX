package com.alibaba.datax.plugin.reader.otsstreamreader.internal.functiontest;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.common.*;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.config.OTSStreamReaderConfig;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.utils.OTSHelper;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.utils.TimeUtils;
import com.alicloud.openservices.tablestore.model.StreamRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Time;
import java.util.List;

public class MockOTSSingleShardTest {

    private String shardId = "Shard";
    private String dataTable = "DataTable_MockOTSSingleShardTest";
    private String statusTable = "StatusTable_MockOTSSingleShardTest";

    @Before
    public void deleteStatusTable() {
        TestHelper.deleteTable(statusTable);
    }

    /**
     * 单个Shard, 使用固定的startTime和endTime，在startTime和endTime之间随机产生数据，数据覆盖全部操作类型和值类型。
     * 测试10次，是否导出SequenceInfo各测5次。校验导出数据的正确性。
     * @throws Exception
     */
    @Test
    public void testBasicCase() throws Exception {
        for (int times = 0; times < 10; times++) {
            deleteStatusTable();
            long now = System.currentTimeMillis();
            long startTime = now - 11 * TimeUtils.MINUTE_IN_MILLIS;
            long endTime = now - 10 * TimeUtils.MINUTE_IN_MILLIS;

            Configuration configuration = ConfigurationHelper.loadConf(dataTable, statusTable, startTime, endTime);
            OTSStreamReaderConfig config = OTSStreamReaderConfig.load(configuration);
            config.setIsExportSequenceInfo((times % 2) == 0);
            config.setSlaveLoopInterval(10);

            MockOTS mockOTS = new MockOTS(OTSHelper.getOTSInstance(config));
            mockOTS.enableStream(config.getDataTable(), (int) (2 * TimeUtils.HOUR_IN_MILLIS / 1000));
            ShardInfoForTest shardInfo = new ShardInfoForTest(shardId, now - 20 * TimeUtils.MINUTE_IN_MILLIS, now, 100000);
            TestHelper.prepareData(mockOTS, shardInfo);

            RecordSenderForTest recordSenderForTest = new RecordSenderForTest();
            config.setOtsForTest(mockOTS);
            TestHelper.runReader(mockOTS, config, recordSenderForTest);

            List<StreamRecord> records = TestHelper.filterRecordsByTimeRange(mockOTS, shardId, startTime, endTime);
            AssertHelper.check(recordSenderForTest.getRecords(), records, (times % 2) == 0, shardId);
        }
    }

    /**
     * 单个Shard，使用随机的startTime和endTime，在startTime和endTime之间随机产生数据，测试15次，每次测试产生的数据量不同，
     * 覆盖数据量较少到较多的场景，数据覆盖全部操作类型和值类型。覆盖是否导出SequenceInfo的场景，校验结果正确性。
     * @throws Exception
     */
    @Test
    public void testRandomTimeRangeAndRowNum() throws Exception {
        for (int times = 0; times < 15; times++) {
            deleteStatusTable();
            long now = System.currentTimeMillis();
            long startTime = now - 30 * TimeUtils.MINUTE_IN_MILLIS + Utils.getRandomInt(10 * (int) TimeUtils.MINUTE_IN_MILLIS);
            long endTime = startTime + Utils.getRandomInt(10 * (int) TimeUtils.MINUTE_IN_MILLIS) + 1;

            Configuration configuration = ConfigurationHelper.loadConf(dataTable, statusTable, startTime, endTime);
            OTSStreamReaderConfig config = OTSStreamReaderConfig.load(configuration);
            config.setIsExportSequenceInfo((times % 2) == 0);
            config.setSlaveLoopInterval(10);

            MockOTS mockOTS = new MockOTS(OTSHelper.getOTSInstance(config));
            mockOTS.enableStream(config.getDataTable(), (int) (2 * TimeUtils.HOUR_IN_MILLIS / 1000));
            ShardInfoForTest shardInfo = new ShardInfoForTest(shardId, now - 30 * TimeUtils.MINUTE_IN_MILLIS, now,
                    Utils.getRandomInt((int) Math.pow(2, times)));
            TestHelper.prepareData(mockOTS, shardInfo);

            RecordSenderForTest recordSenderForTest = new RecordSenderForTest();
            config.setOtsForTest(mockOTS);
            TestHelper.runReader(mockOTS, config, recordSenderForTest);

            List<StreamRecord> records = TestHelper.filterRecordsByTimeRange(mockOTS, shardId, startTime, endTime);
            AssertHelper.check(recordSenderForTest.getRecords(), records, (times % 2) == 0, shardId);
        }
    }

    /**
     * 单个Shard，测试相继启动多次数据导出，每次的startTime是上次的endTime，每次导出的时间长度随机，校验结果正确性。
     * @throws Exception
     */
    @Test
    public void testReadSuccessively() throws Exception {
        long now = System.currentTimeMillis();
        long startTime = now - TimeUtils.HOUR_IN_MILLIS;
        long endTime = startTime + Utils.getRandomInt(5 * (int)TimeUtils.MINUTE_IN_MILLIS);
        Configuration configuration = ConfigurationHelper.loadConf(dataTable, statusTable, startTime, endTime);
        OTSStreamReaderConfig config = OTSStreamReaderConfig.load(configuration);

        MockOTS mockOTS = new MockOTS();
        mockOTS.enableStream(config.getDataTable(), (int) (2 * TimeUtils.HOUR_IN_MILLIS / 1000));
        ShardInfoForTest shardInfo = new ShardInfoForTest(shardId, now - 70 * TimeUtils.MINUTE_IN_MILLIS, now, 200000);
        TestHelper.prepareData(mockOTS, shardInfo);

        for (int times = 0; times < 10; times++) {
            mockOTS.setOTS(OTSHelper.getOTSInstance(config));
            config.setIsExportSequenceInfo((times % 2) == 0);
            config.setOtsForTest(mockOTS);
            config.setSlaveLoopInterval(10);

            RecordSenderForTest recordSenderForTest = new RecordSenderForTest();
            TestHelper.runReader(mockOTS, config, recordSenderForTest);
            List<StreamRecord> records = TestHelper.filterRecordsByTimeRange(mockOTS, shardId, startTime, endTime);
            AssertHelper.check(recordSenderForTest.getRecords(), records, (times % 2) == 0, shardId);

            startTime = endTime;
            endTime = startTime + Utils.getRandomInt(5 * (int)TimeUtils.MINUTE_IN_MILLIS);
            configuration = ConfigurationHelper.loadConf(dataTable, statusTable, startTime, endTime);
            config = OTSStreamReaderConfig.load(configuration);
        }
    }

}
