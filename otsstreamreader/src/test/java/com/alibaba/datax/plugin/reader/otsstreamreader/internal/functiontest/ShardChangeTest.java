package com.alibaba.datax.plugin.reader.otsstreamreader.internal.functiontest;

import com.alibaba.datax.plugin.reader.otsstreamreader.internal.common.*;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.config.OTSStreamReaderConfig;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.core.CheckpointTimeTracker;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.model.ShardCheckpoint;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.model.StreamJob;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.utils.OTSHelper;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.utils.TimeUtils;
import com.alicloud.openservices.tablestore.model.*;
import com.aliyun.openservices.ots.internal.streamclient.model.CheckpointPosition;
import org.junit.Test;

import java.util.*;

public class ShardChangeTest {

    private static final String dataTable = "DataTable_ShardChangeTest";
    private static final String statusTable = "StatusTable_ShardChangeTest";

    /**
     * reader运行过程中删除一个Shard
     */
    @Test
    public void testDeleteShardWhenRead() throws Exception {
        long now = System.currentTimeMillis();
        long startTime = now - TimeUtils.HOUR_IN_MILLIS * 2;
        long endTime = now - TimeUtils.HOUR_IN_MILLIS * 1;
        OTSStreamReaderConfig config = ConfigurationHelper.loadReaderConfig(dataTable, statusTable, startTime, endTime);
        final MockOTS mockOTS = new MockOTS(OTSHelper.getOTSInstance(config));
        mockOTS.enableStream(dataTable, TimeUtils.DAY_IN_SEC);
        config.setOtsForTest(mockOTS);
        config.setIsExportSequenceInfo(true);

        String streamId = dataTable + "_Stream";
        CheckpointTimeTracker checkpointTimeTracker = new CheckpointTimeTracker(mockOTS, statusTable, streamId);
        StreamJob streamJob = new StreamJob(dataTable, streamId, "version", new HashSet<String>(Arrays.asList("shardToDelete")), startTime - 3600000L, startTime);
        checkpointTimeTracker.writeStreamJob(streamJob);
        checkpointTimeTracker.writeCheckpoint(startTime, new ShardCheckpoint("shardToDelete", streamJob.getVersion(), CheckpointPosition.SHARD_END, 0));

        mockOTS.createShard("shardToDelete", null, null);

        List<StreamRecord> streamRecords = new ArrayList<StreamRecord>();
        PrimaryKey primaryKey = Utils.getPrimaryKey(1);
        List<RecordColumn> columns = Utils.getRecordColumns(1, StreamRecord.RecordType.PUT);

        for (int i = 0; i < 10000; i++) {
            StreamRecord record = new StreamRecord();
            record.setRecordType(StreamRecord.RecordType.PUT);
            record.setPrimaryKey(primaryKey);
            record.setColumns(columns);
            record.setSequenceInfo(new RecordSequenceInfo(0, (startTime + 100) * 1000, i));
            streamRecords.add(record);
        }

        List<String> shardIds = new ArrayList<String>();
        for (int i = 0; i < 10; i++) {
            String shardId = String.format("shard%05d", i);
            mockOTS.createShard(shardId, null, null);
            mockOTS.appendRecords(shardId, streamRecords);
            shardIds.add(shardId);
        }

        System.out.println(System.currentTimeMillis() - now);
        now = System.currentTimeMillis();

        Thread thread = new Thread(new Runnable() {
            public void run() {
                TimeUtils.sleepMillis(1000);
                mockOTS.deleteShard("shardToDelete");
            }
        });
        thread.start();
        RecordSenderForTest recordSenderForTest = new RecordSenderForTest();
        TestHelper.runReader(mockOTS, config, recordSenderForTest);

        System.out.println(System.currentTimeMillis() - now);
        now = System.currentTimeMillis();

        List<Map.Entry<StreamRecord, String>> recordsWithShardId =
                TestHelper.filterRecordsByTimeRange(mockOTS, shardIds, startTime, endTime);
        AssertHelper.sortAndCheck(recordSenderForTest.getRecords(), recordsWithShardId, true);

        System.out.println(System.currentTimeMillis() - now);
    }

    /**
     * reader运行过程中新增Shard
     */
    @Test
    public void testAddShardsWhenRead() throws Exception {
        long now = System.currentTimeMillis();
        long startTime = now - TimeUtils.HOUR_IN_MILLIS * 2;
        long endTime = now - TimeUtils.HOUR_IN_MILLIS * 1;
        OTSStreamReaderConfig config = ConfigurationHelper.loadReaderConfig(dataTable, statusTable, startTime, endTime);
        final MockOTS mockOTS = new MockOTS(OTSHelper.getOTSInstance(config));
        mockOTS.enableStream(dataTable, TimeUtils.DAY_IN_SEC);
        config.setOtsForTest(mockOTS);
        config.setIsExportSequenceInfo(true);

        CheckpointTimeTracker checkpointTimeTracker = new CheckpointTimeTracker(mockOTS, statusTable, dataTable + "_Stream");

        List<StreamRecord> streamRecords = new ArrayList<StreamRecord>();
        PrimaryKey primaryKey = Utils.getPrimaryKey(1);
        List<RecordColumn> columns = Utils.getRecordColumns(1, StreamRecord.RecordType.PUT);

        for (int i = 0; i < 10000; i++) {
            StreamRecord record = new StreamRecord();
            record.setRecordType(StreamRecord.RecordType.PUT);
            record.setPrimaryKey(primaryKey);
            record.setColumns(columns);
            record.setSequenceInfo(new RecordSequenceInfo(0, (startTime + 100) * 1000, i));
            streamRecords.add(record);
        }

        List<String> shardIds = new ArrayList<String>();
        for (int i = 0; i < 10; i++) {
            String shardId = String.format("shard%05d", i);
            mockOTS.createShard(shardId, null, null);
            mockOTS.appendRecords(shardId, streamRecords);
            shardIds.add(shardId);
        }

        System.out.println(System.currentTimeMillis() - now);
        now = System.currentTimeMillis();

        Thread thread = new Thread(new Runnable() {
            public void run() {
                TimeUtils.sleepMillis(1000);
                mockOTS.splitShard("shard00000", "shard10000", "shard10001");
                mockOTS.mergeShard("shard00001", "shard00002", "shard10002");
            }
        });
        thread.start();
        RecordSenderForTest recordSenderForTest = new RecordSenderForTest();
        TestHelper.runReader(mockOTS, config, recordSenderForTest);

        System.out.println(System.currentTimeMillis() - now);
        now = System.currentTimeMillis();

        List<Map.Entry<StreamRecord, String>> recordsWithShardId =
                TestHelper.filterRecordsByTimeRange(mockOTS, shardIds, startTime, endTime);
        AssertHelper.sortAndCheck(recordSenderForTest.getRecords(), recordsWithShardId, true);

        System.out.println(System.currentTimeMillis() - now);
    }
}
