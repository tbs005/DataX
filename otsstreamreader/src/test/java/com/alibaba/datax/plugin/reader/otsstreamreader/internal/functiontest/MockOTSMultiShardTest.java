package com.alibaba.datax.plugin.reader.otsstreamreader.internal.functiontest;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.common.*;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.config.OTSStreamReaderConfig;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.config.OTSStreamReaderConstants;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.model.StreamJob;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.utils.OTSHelper;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.utils.TimeUtils;
import com.alicloud.openservices.tablestore.model.StreamRecord;
import com.alicloud.openservices.tablestore.model.StreamShard;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MockOTSMultiShardTest {

    private static final Logger LOG = LoggerFactory.getLogger(MockOTSMultiShardTest.class);

    private String dataTable = "DataTable_MockOTSMultiShardTest";
    private String statusTable = "StatusTable_MockOTSMultiShardTest";

    @Before
    public void deleteStatusTable() {
        TestHelper.deleteTable(statusTable);
    }

    private List<ShardInfoForTest> getRandomShardInfos(int maxShardCount, long minTime, long maxTime, int maxRowNum) {

        List<ShardInfoForTest> shardInfos = null;
        int shardCount = Utils.getRandomInt(maxShardCount - 3) + 3;
        switch (Utils.getRandomInt(3)) {
            case 0:
                shardInfos = TestHelper.getShardsNoRelationShip(shardCount, minTime, maxTime, maxRowNum);
                break;
            case 1:
                shardInfos = TestHelper.getShardsSimpleRelationShip(shardCount, minTime, maxTime, maxRowNum);
                break;
            case 2:
                shardInfos = TestHelper.getShardsComplicatedRelationShip(shardCount, minTime, maxTime, maxRowNum);
                break;
        }

        for (ShardInfoForTest shardInfo : shardInfos) {
            LOG.info("GetRandomShardInfos: {}, {}, {}, {}, {}, {}, {}.", shardInfo.getShardId(), shardInfo.getParentId(), shardInfo.getParentSiblingId(),
                    shardInfo.getStartTime(), shardInfo.getEndTime(), shardInfo.getRowNum(), shardInfo.getSiblingId());
        }
        return shardInfos;
    }

    /**
     * 多个Shard，使用随机的startTime和endTime，数据覆盖全部操作类型和全部值类型，分别对多种Shard关系进行测试：
     * 1. 无父子关系
     * 2. 只有两层的父子关系，Split或Merge的时间覆盖TimeRange前、中、后。每个Shard是否有数据、数据量大小随机。
     * 3. 有复杂的父子关系，层级很多，Split或Merge的时间覆盖TimeRange前、中、后。每个Shard是否有数据，数据量大小随机。
     * @throws Exception
     */
    @Test
    public void testBasicCase() throws Exception {
        long now = System.currentTimeMillis();

        for (int outerIter = 0; outerIter < 10; outerIter++) {

            int maxShardCount = 10 + outerIter * 10;

            List<ShardInfoForTest> shardInfos = getRandomShardInfos(maxShardCount, now - 20 * TimeUtils.MINUTE_IN_MILLIS, now, 100);

            for (int innerIter = 0; innerIter < 3; innerIter++) {
                LOG.info("Round: " + (outerIter * 3 + innerIter));
                TestHelper.deleteTable(statusTable);

                long startTime = now - 20 * TimeUtils.MINUTE_IN_MILLIS + Utils.getRandomLong(10 * TimeUtils.MINUTE_IN_MILLIS);
                long endTime = startTime + Utils.getRandomLong(5 * TimeUtils.MINUTE_IN_MILLIS);
                boolean isExportSeq = (innerIter % 2) == 0;

                Configuration configuration = ConfigurationHelper.loadConf(dataTable, statusTable, startTime, endTime);
                OTSStreamReaderConfig config = OTSStreamReaderConfig.load(configuration);
                config.setSlaveLoopInterval(10);
                config.setIsExportSequenceInfo(isExportSeq);

                MockOTS mockOTS = new MockOTS(OTSHelper.getOTSInstance(config));
                mockOTS.enableStream(config.getDataTable(), (int) (2 * TimeUtils.HOUR_IN_MILLIS / 1000));
                TestHelper.prepareData(mockOTS, shardInfos);

                RecordSenderForTest recordSenderForTest = new RecordSenderForTest();
                config.setOtsForTest(mockOTS);
                LOG.info("Start run reader.");
                TestHelper.runReader(mockOTS, config, recordSenderForTest);
                LOG.info("Finish run reader, records count: {}", recordSenderForTest.getRecords().size());

                List<String> shardIds = new ArrayList<String>();
                for (ShardInfoForTest shardInfo : shardInfos) {
                    shardIds.add(shardInfo.getShardId());
                }

                List<Map.Entry<StreamRecord, String>> recordsWithShardId =
                        TestHelper.filterRecordsByTimeRange(mockOTS, shardIds, startTime, endTime);

                if (isExportSeq) {
                    AssertHelper.sortAndCheck(recordSenderForTest.getRecords(), recordsWithShardId, isExportSeq);
                } else {
                    AssertHelper.checkRecordsNum(recordSenderForTest.getRecords(), recordsWithShardId);
                }
            }
        }
    }

    /**
     * 多个Shard，测试相继启动多次数据导出，每次的startTime是上次的endTime，每次导出的时间长度随机，分别对多种Shard关系（同上）进行测试。
     */
    @Test
    public void testReadSuccessively() throws Exception {
        long now = System.currentTimeMillis();

        for (int outerIter = 0; outerIter < 10; outerIter++) {
            TestHelper.deleteTable(statusTable);

            int maxShardCount = 10 + outerIter * 20;
            List<ShardInfoForTest> shardInfos = getRandomShardInfos(maxShardCount, now - 70 * TimeUtils.MINUTE_IN_MILLIS, now, 100);

            long startTime = now - TimeUtils.HOUR_IN_MILLIS;
            long endTime = startTime + Utils.getRandomInt(5 * (int)TimeUtils.MINUTE_IN_MILLIS);
            Configuration configuration = ConfigurationHelper.loadConf(dataTable, statusTable, startTime, endTime);
            OTSStreamReaderConfig config = OTSStreamReaderConfig.load(configuration);
            config.setSlaveLoopInterval(10);

            MockOTS mockOTS = new MockOTS(OTSHelper.getOTSInstance(config));
            mockOTS.enableStream(config.getDataTable(), (int) (2 * TimeUtils.HOUR_IN_MILLIS / 1000));
            TestHelper.prepareData(mockOTS, shardInfos);

            for (int innerIter = 0; innerIter < 10; innerIter++) {
                LOG.info("Start reader, round: {}", outerIter * 10 + innerIter);
                mockOTS.setOTS(OTSHelper.getOTSInstance(config));
                config.setIsExportSequenceInfo(true);
                config.setOtsForTest(mockOTS);
                config.setSlaveLoopInterval(10);

                RecordSenderForTest recordSenderForTest = new RecordSenderForTest();
                TestHelper.runReader(mockOTS, config, recordSenderForTest);
                LOG.info("Records count: {}", recordSenderForTest.getRecords().size());

                List<String> shardIds = new ArrayList<String>();
                for (ShardInfoForTest shardInfo : shardInfos) {
                    shardIds.add(shardInfo.getShardId());
                }
                List<Map.Entry<StreamRecord, String>> recordsWithShardId =
                        TestHelper.filterRecordsByTimeRange(mockOTS, shardIds, startTime, endTime);
                AssertHelper.sortAndCheck(recordSenderForTest.getRecords(), recordsWithShardId, true);

                startTime = endTime;
                endTime = startTime + Utils.getRandomInt(5 * (int)TimeUtils.MINUTE_IN_MILLIS);
                configuration = ConfigurationHelper.loadConf(dataTable, statusTable, startTime, endTime);
                config = OTSStreamReaderConfig.load(configuration);
            }
        }
    }

    /**
     * 多个Shard，测试相继启动多次数据导出，每次的startTime是上次的endTime，每次导出的时间长度随机，分别对多种Shard关系（同上）进行测试。
     */
    @Test
    public void testReadSame() throws Exception {
        long now = System.currentTimeMillis();

        for (int outerIter = 0; outerIter < 10; outerIter++) {
            TestHelper.deleteTable(statusTable);

            int maxShardCount = 10 + outerIter * 20;
            List<ShardInfoForTest> shardInfos = getRandomShardInfos(maxShardCount, now - 70 * TimeUtils.MINUTE_IN_MILLIS, now, 100);

            long startTime = now - TimeUtils.HOUR_IN_MILLIS;
            long endTime = startTime + Utils.getRandomInt(5 * (int)TimeUtils.MINUTE_IN_MILLIS);
            Configuration configuration = ConfigurationHelper.loadConf(dataTable, statusTable, startTime, endTime);
            OTSStreamReaderConfig config = OTSStreamReaderConfig.load(configuration);

            MockOTS mockOTS = new MockOTS(OTSHelper.getOTSInstance(config));
            mockOTS.enableStream(config.getDataTable(), (int) (2 * TimeUtils.HOUR_IN_MILLIS / 1000));
            TestHelper.prepareData(mockOTS, shardInfos);

            for (int innerIter = 0; innerIter < 10; innerIter++) {

                mockOTS.setOTS(OTSHelper.getOTSInstance(config));
                config.setSlaveLoopInterval(10);
                config.setIsExportSequenceInfo(true);
                config.setOtsForTest(mockOTS);

                RecordSenderForTest recordSenderForTest = new RecordSenderForTest();
                LOG.info("Start reader, round: {}", outerIter * 10 + innerIter);
                TestHelper.runReader(mockOTS, config, recordSenderForTest);

                List<String> shardIds = new ArrayList<String>();
                for (ShardInfoForTest shardInfo : shardInfos) {
                    shardIds.add(shardInfo.getShardId());
                }
                List<Map.Entry<StreamRecord, String>> recordsWithShardId =
                        TestHelper.filterRecordsByTimeRange(mockOTS, shardIds, startTime, endTime);
                LOG.info("Records count: {}", recordSenderForTest.getRecords().size());
                AssertHelper.sortAndCheck(recordSenderForTest.getRecords(), recordsWithShardId, true);

                startTime = endTime;
                endTime = startTime + Utils.getRandomInt(5 * (int)TimeUtils.MINUTE_IN_MILLIS);
                configuration = ConfigurationHelper.loadConf(dataTable, statusTable, startTime, endTime);
                config = OTSStreamReaderConfig.load(configuration);
            }
        }
    }
}
