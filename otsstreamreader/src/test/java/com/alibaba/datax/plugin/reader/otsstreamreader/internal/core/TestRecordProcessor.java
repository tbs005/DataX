package com.alibaba.datax.plugin.reader.otsstreamreader.internal.core;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.common.ConfigurationHelper;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.common.MockOTS;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.common.RecordSenderForTest;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.common.TestHelper;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.config.Mode;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.config.OTSStreamReaderConfig;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.model.ShardCheckpoint;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.model.StreamJob;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.utils.OTSHelper;
import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.SyncClientInterface;
import com.alicloud.openservices.tablestore.core.utils.Pair;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.model.internal.CreateTableRequestEx;
import com.aliyun.openservices.ots.internal.streamclient.model.CheckpointPosition;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestRecordProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(TestRecordProcessor.class);

    private static SyncClientInterface client;
    private static OTSStreamReaderConfig config;
    private static OTSStreamReaderChecker otsStreamReaderChecker;

    public void prepare(String dataTable, String statusTable) {
        Configuration configuration = ConfigurationHelper.loadConf(dataTable, statusTable, 1, 2);
        config = OTSStreamReaderConfig.load(configuration);
        client = OTSHelper.getOTSInstance(config);
        otsStreamReaderChecker = new OTSStreamReaderChecker(client, config);

        TestHelper.deleteTable(statusTable);
        TestHelper.deleteTable(dataTable);

        otsStreamReaderChecker.checkAndCreateStatusTableIfNotExist();
    }

    private StreamDetails prepareDataTable(String tableName, List<Long> splitPoints) {
        TableMeta tableMeta = new TableMeta(tableName);
        tableMeta.addPrimaryKeyColumn("pk0", PrimaryKeyType.INTEGER);
        tableMeta.addPrimaryKeyColumn("pk1", PrimaryKeyType.STRING);

        TableOptions tableOptions = new TableOptions();
        tableOptions.setMaxVersions(3);
        tableOptions.setTimeToLive(Integer.MAX_VALUE);

        CreateTableRequestEx request = new CreateTableRequestEx(tableMeta, tableOptions);
        StreamSpecification streamSpec = new StreamSpecification(true, 24);
        request.setStreamSpecification(streamSpec);

        if (splitPoints != null && !splitPoints.isEmpty()) {
            List<PartitionRange> partitions = new ArrayList<PartitionRange>();
            long rangeBegin = 0;
            for (Long splitPoint : splitPoints) {
                partitions.add(new PartitionRange(PrimaryKeyValue.fromLong(rangeBegin), PrimaryKeyValue.fromLong(splitPoint)));
                rangeBegin = splitPoint;
            }
            partitions.add(new PartitionRange(PrimaryKeyValue.fromLong(rangeBegin), PrimaryKeyValue.fromLong(Long.MAX_VALUE)));
            request.setPartitionRangeList(partitions);
        }

        client.createTable(request);

        return OTSHelper.getStreamDetails(client, tableName);
    }

    @Test
    public void testReadRecordFailed() throws Exception {
        String statusTable = "StatusTable_RecordProcessTest";
        String dataTable = "DataTable_RecordProcessTest";
        prepare(dataTable, statusTable);

        config.setColumns(Arrays.asList("pk0", "pk1", "col0", "col1", "col2"));
        StreamDetails streamDetails = prepareDataTable(dataTable, null);
        CheckpointTimeTracker ctt = new CheckpointTimeTracker(client, statusTable, streamDetails.getStreamId());

        Thread.sleep(3000);

        List<StreamShard> shards = OTSHelper.getOrderedShardList(client, streamDetails.getStreamId());
        assertEquals(shards.size(), 1);

        Map<String, StreamShard> shardsMap = new HashMap<String, StreamShard>();
        for (StreamShard shard : shards) {
            shardsMap.put(shard.getShardId(), shard);
        }

        config.setMode(Mode.SINGLE_VERSION_AND_UPDATE_ONLY);

        StreamJob streamJob = new StreamJob(dataTable, streamDetails.getStreamId(), "version", shardsMap.keySet(), 0, 1);
        StreamShard shardToProcess = shards.get(0);

        ShardCheckpoint startPoint = new ShardCheckpoint(shardToProcess.getShardId(), streamJob.getVersion(), CheckpointPosition.TRIM_HORIZON, 0);
        config.setStartTimestampMillis(0);
        config.setEndTimestampMillis(1);

        RecordSenderForTest dataxRs = new RecordSenderForTest();
        boolean shouldSkip = startPoint.getCheckpoint().equals(CheckpointPosition.TRIM_HORIZON);
        MockOTS mockClient = new MockOTS();
        mockClient.throwException(new ClientException("hello world"));
        RecordProcessor recordProcessor = new RecordProcessor(mockClient, config, streamJob, shardToProcess, startPoint, shouldSkip, ctt, dataxRs);
        recordProcessor.run();

        assertEquals(recordProcessor.getState(), RecordProcessor.State.FAILED);
    }

    @Test
    public void testRecordProcessor() throws Exception {
        String statusTable = "StatusTable_RecordProcessTest";
        String dataTable = "DataTable_RecordProcessTest";
        prepare(dataTable, statusTable);

        config.setColumns(Arrays.asList("pk0", "pk1", "col0", "col1", "col2"));

        Thread.sleep(1000);

        StreamDetails streamDetails = prepareDataTable(dataTable, null);
        CheckpointTimeTracker ctt = new CheckpointTimeTracker(client, statusTable, streamDetails.getStreamId());

        List<Pair<Long, Long>> checkpoints = new ArrayList<Pair<Long, Long>>();
        long rowCount = TestHelper.prepareData(client, dataTable, 60000, checkpoints);
        LOG.info("Checkpoints: {}. RowCount: {}.", checkpoints, rowCount);

        Thread.sleep(3000);

        List<StreamShard> shards = OTSHelper.getOrderedShardList(client, streamDetails.getStreamId());
        assertEquals(shards.size(), 1);

        Map<String, StreamShard> shardsMap = new HashMap<String, StreamShard>();
        for (StreamShard shard : shards) {
            shardsMap.put(shard.getShardId(), shard);
        }

        // test read all data in single version
        {
            config.setMode(Mode.SINGLE_VERSION_AND_UPDATE_ONLY);

            long startTime = checkpoints.get(0).getFirst();
            long endTime = checkpoints.get(checkpoints.size() - 1).getFirst();

            StreamJob streamJob = new StreamJob(dataTable, streamDetails.getStreamId(), "version", shardsMap.keySet(), startTime, endTime);
            StreamShard shardToProcess = shards.get(0);

            ShardCheckpoint startPoint = new ShardCheckpoint(shardToProcess.getShardId(), streamJob.getVersion(), CheckpointPosition.TRIM_HORIZON, 0);
            List<Record> records = readAllData(streamJob, shardToProcess, startPoint, ctt, startTime, endTime);
            TestHelper.checkRecordsInSingleVer(records, 0);
            assertEquals(records.size(), rowCount);
            Map<String, ShardCheckpoint> cps = ctt.getAllCheckpoints(endTime);
            assertEquals(cps.size(), 1);
            assertTrue(cps.containsKey(shardToProcess.getShardId()));
            ShardCheckpoint cp = cps.get(shardToProcess.getShardId());
            assertTrue(!cp.getCheckpoint().equals(CheckpointPosition.SHARD_END));
            assertTrue(!cp.getCheckpoint().equals(CheckpointPosition.TRIM_HORIZON));
            assertEquals(cp.getShardId(), shardToProcess.getShardId());
            assertEquals(cp.getVersion(), streamJob.getVersion());

            String iterStr = ctt.getShardLargestCheckpointInTimeRange(shardToProcess.getShardId(), startTime, endTime + 1);
            assertEquals(iterStr, cp.getCheckpoint());
        }

        // test read all data in multi version
        {
            config.setMode(Mode.MULTI_VERSION);
            long startTime = checkpoints.get(0).getFirst();
            long endTime = checkpoints.get(checkpoints.size() - 1).getFirst();

            StreamJob streamJob = new StreamJob(dataTable, streamDetails.getStreamId(), "version", shardsMap.keySet(), startTime, endTime);
            StreamShard shardToProcess = shards.get(0);
            ShardCheckpoint startPoint = new ShardCheckpoint(shardToProcess.getShardId(), streamJob.getVersion(), CheckpointPosition.TRIM_HORIZON, 0);
            List<Record> records = readAllData(streamJob, shardToProcess, startPoint, ctt, startTime, endTime);
            long rowCountGot = TestHelper.checkRecordsInMultiVer(records, 0);
            assertEquals(rowCount, rowCountGot);
            Map<String, ShardCheckpoint> cps = ctt.getAllCheckpoints(endTime);
            assertEquals(cps.size(), 1);
            assertTrue(cps.containsKey(shardToProcess.getShardId()));
            ShardCheckpoint cp = cps.get(shardToProcess.getShardId());
            assertTrue(!cp.getCheckpoint().equals(CheckpointPosition.SHARD_END));
            assertTrue(!cp.getCheckpoint().equals(CheckpointPosition.TRIM_HORIZON));
            assertEquals(cp.getShardId(), shardToProcess.getShardId());
            assertEquals(cp.getVersion(), streamJob.getVersion());

            String iterStr = ctt.getShardLargestCheckpointInTimeRange(shardToProcess.getShardId(), startTime, endTime + 1);
            assertEquals(iterStr, cp.getCheckpoint());
        }

        // test read all data by multi times, each time continued with last checkpoint
        {
            config.setMode(Mode.MULTI_VERSION);
            long startTime = checkpoints.get(0).getFirst();
            long endTime = checkpoints.get(checkpoints.size() - 1).getFirst();

            StreamJob streamJob = new StreamJob(dataTable, streamDetails.getStreamId(), "version", shardsMap.keySet(), startTime, endTime);
            StreamShard shardToProcess = shards.get(0);

            ShardCheckpoint lastCheckpoint = null;
            List<Record> allRecords = new ArrayList<Record>();
            for (int i = 1; i < checkpoints.size(); i++) {
                startTime = checkpoints.get(i - 1).getFirst();
                endTime = checkpoints.get(i).getFirst();
                if (lastCheckpoint == null) {
                    lastCheckpoint = new ShardCheckpoint(shardToProcess.getShardId(), streamJob.getVersion(), CheckpointPosition.TRIM_HORIZON, 0);
                }
                LOG.info("Read data: {}, {}, {}", startTime, endTime, lastCheckpoint);
                List<Record> records = readAllData(streamJob, shardToProcess, lastCheckpoint, ctt, startTime, endTime);
                allRecords.addAll(records);

                Map<String, ShardCheckpoint> cps = ctt.getAllCheckpoints(endTime);
                assertEquals(cps.size(), 1);
                assertTrue(cps.containsKey(shardToProcess.getShardId()));
                ShardCheckpoint cp = cps.get(shardToProcess.getShardId());
                assertTrue(!cp.getCheckpoint().equals(CheckpointPosition.SHARD_END));
                assertTrue(!cp.getCheckpoint().equals(CheckpointPosition.TRIM_HORIZON));
                assertEquals(cp.getShardId(), shardToProcess.getShardId());
                assertEquals(cp.getVersion(), streamJob.getVersion());

                lastCheckpoint = cp;

                LOG.info("Checkpoint: {}.", cp);
            }

            long rowCountGot = TestHelper.checkRecordsInMultiVer(allRecords, 0);
            assertEquals(rowCount, rowCountGot);
        }

        // test read data with time range
        {
            config.setMode(Mode.MULTI_VERSION);
            int index = checkpoints.size() / 2;
            long startTime = checkpoints.get(index).getFirst();
            long endTime = checkpoints.get(index + 1).getFirst();
            long rowCountBefore = checkpoints.get(index).getSecond();
            long rowCountInRange = checkpoints.get(index + 1).getSecond() - rowCountBefore;

            StreamJob streamJob = new StreamJob(dataTable, streamDetails.getStreamId(), "version", shardsMap.keySet(), startTime, endTime);
            StreamShard shardToProcess = shards.get(0);
            ShardCheckpoint startPoint = new ShardCheckpoint(shardToProcess.getShardId(), streamJob.getVersion(), CheckpointPosition.TRIM_HORIZON, 0);
            List<Record> records = readAllData(streamJob, shardToProcess, startPoint, ctt, startTime, endTime);
            long rowCountGot = TestHelper.checkRecordsInMultiVer(records, rowCountBefore);
            assertEquals(rowCountInRange, rowCountGot);
            Map<String, ShardCheckpoint> cps = ctt.getAllCheckpoints(endTime);
            assertEquals(cps.size(), 1);
            assertTrue(cps.containsKey(shardToProcess.getShardId()));
            ShardCheckpoint cp = cps.get(shardToProcess.getShardId());
            assertTrue(!cp.getCheckpoint().equals(CheckpointPosition.SHARD_END));
            assertTrue(!cp.getCheckpoint().equals(CheckpointPosition.TRIM_HORIZON));
            assertEquals(cp.getShardId(), shardToProcess.getShardId());
            assertEquals(cp.getVersion(), streamJob.getVersion());
        }

        // test read data with skip
        {
            config.setMode(Mode.MULTI_VERSION);
            int index = checkpoints.size() / 2;
            long startTime = checkpoints.get(0).getFirst();
            long endTime = checkpoints.get(index + 1).getFirst();
            long rowCountBefore = checkpoints.get(index).getSecond();
            long rowCountInRange = checkpoints.get(index + 1).getSecond() - rowCountBefore;

            StreamJob streamJob = new StreamJob(dataTable, streamDetails.getStreamId(), "version", shardsMap.keySet(), startTime, endTime);
            StreamShard shardToProcess = shards.get(0);
            ShardCheckpoint startPoint = new ShardCheckpoint(shardToProcess.getShardId(), streamJob.getVersion(), CheckpointPosition.TRIM_HORIZON, rowCountBefore);
            List<Record> records = readAllData(streamJob, shardToProcess, startPoint, ctt, startTime, endTime);
            long rowCountGot = TestHelper.checkRecordsInMultiVer(records, rowCountBefore);
            assertEquals(rowCountInRange, rowCountGot);
            Map<String, ShardCheckpoint> cps = ctt.getAllCheckpoints(endTime);
            assertEquals(cps.size(), 1);
            assertTrue(cps.containsKey(shardToProcess.getShardId()));
            ShardCheckpoint cp = cps.get(shardToProcess.getShardId());
            assertTrue(!cp.getCheckpoint().equals(CheckpointPosition.SHARD_END));
            assertTrue(!cp.getCheckpoint().equals(CheckpointPosition.TRIM_HORIZON));
            assertEquals(cp.getShardId(), shardToProcess.getShardId());
            assertEquals(cp.getVersion(), streamJob.getVersion());
        }
    }

    private List<Record> readAllData(StreamJob streamJob, StreamShard shard, ShardCheckpoint startPoint,
                                     CheckpointTimeTracker ctt, long startTime, long endTime) throws Exception {
        streamJob.setStartTimeInMillis(startTime);
        streamJob.setEndTimeInMillis(endTime);
        config.setStartTimestampMillis(startTime);
        config.setEndTimestampMillis(endTime);

        RecordSenderForTest dataxRs = new RecordSenderForTest();
        boolean shouldSkip = startPoint.getCheckpoint().equals(CheckpointPosition.TRIM_HORIZON);
        RecordProcessor recordProcessor = new RecordProcessor(client, config, streamJob, shard, startPoint, shouldSkip, ctt, dataxRs);
        recordProcessor.initialize();

        assertEquals(recordProcessor.getShard().getShardId(), shard.getShardId());
        assertEquals(recordProcessor.getShard().getParentId(), shard.getParentId());
        assertEquals(recordProcessor.getShard().getParentSiblingId(), shard.getParentSiblingId());

        assertEquals(recordProcessor.getState(), RecordProcessor.State.READY);
        long now = System.currentTimeMillis();
        Thread thread = new Thread(recordProcessor);
        thread.start();

        for (int i = 0; i < 20; i++) {
            if (recordProcessor.getState() != RecordProcessor.State.SUCCEED) {
                Thread.sleep(1000);
                assertTrue(recordProcessor.getLastProcessTime() >= now);
                continue;
            } else {
                break;
            }
        }

        assertTrue(recordProcessor.getStartTime() > 0);
        assertTrue(recordProcessor.getLastProcessTime() > 0);
        thread.join();

        assertEquals(recordProcessor.getState(), RecordProcessor.State.SUCCEED);
        return dataxRs.getRecords();
    }
}
