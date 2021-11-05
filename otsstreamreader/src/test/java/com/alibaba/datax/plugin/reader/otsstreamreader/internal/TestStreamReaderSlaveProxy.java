package com.alibaba.datax.plugin.reader.otsstreamreader.internal;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.common.ConfigurationHelper;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.common.RecordSenderForTest;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.common.TestHelper;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.config.Mode;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.config.OTSStreamReaderConfig;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.core.CheckpointTimeTracker;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.core.OTSStreamReaderChecker;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.model.ShardCheckpoint;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.model.StreamJob;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.utils.OTSHelper;
import com.alicloud.openservices.tablestore.SyncClientInterface;
import com.alicloud.openservices.tablestore.core.utils.Pair;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.model.internal.CreateTableRequestEx;
import com.aliyun.openservices.ots.internal.streamclient.model.CheckpointPosition;
import com.google.common.collect.Lists;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.junit.Assert.*;

public class TestStreamReaderSlaveProxy {
    private static final Logger LOG = LoggerFactory.getLogger(TestStreamReaderSlaveProxy.class);
    private SyncClientInterface client;

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
    public void testInitShutDown() throws Exception {
        String dataTable = "DataTable_TestStreamReaderMasterProxy";
        String statusTable = "StatusTable_TestStreamReaderMasterProxy";
        long now = System.currentTimeMillis();
        long startTime = now - 7200000;
        long endTime = now - 3600000;
        Configuration configuration = ConfigurationHelper.loadConf(dataTable, statusTable, startTime, endTime);
        OTSStreamReaderConfig config = OTSStreamReaderConfig.load(configuration);
        client = OTSHelper.getOTSInstance(config);
        TestHelper.deleteTable(statusTable);
        TestHelper.deleteTable(dataTable);

        OTSStreamReaderChecker checker = new OTSStreamReaderChecker(client, config);
        checker.checkAndCreateStatusTableIfNotExist();

        List<Long> splitPoints = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L);
        StreamDetails streamDetails = prepareDataTable(dataTable, splitPoints);
        List<StreamShard> shards = OTSHelper.getOrderedShardList(client, streamDetails.getStreamId());
        Set<String> shardIds = new HashSet<String>();
        for (StreamShard shard : shards) {
            shardIds.add(shard.getShardId());
        }

        // checkpoint is not found, fill with latest checkpoint we can find
        {
            CheckpointTimeTracker ctt = new CheckpointTimeTracker(client, config.getStatusTable(), streamDetails.getStreamId());

            Map<String, String> shardWithLatestIterator = new HashMap<String, String>();
            // write checkpoint of last job
            {
                StreamJob streamJob = new StreamJob(dataTable, streamDetails.getStreamId(), "last_version", new HashSet<String>(shardIds), startTime - 3600000, startTime);
                ctt.writeStreamJob(streamJob);

                // write part of shard checkpoints
                List<String> shardIdList = new ArrayList<String>(streamJob.getShardIds());
                for (int i = 0; i < shardIdList.size() - 1; i++) {
                    ctt.writeCheckpoint(streamJob.getEndTimeInMillis(), new ShardCheckpoint(shardIdList.get(i), streamJob.getVersion(), "shard_iterator", 1));
                }

                // write latest version for part shards
                for (int i = 0; i < shardIdList.size() / 2; i++) {
                    String shardId = shardIdList.get(i);
                    String iterator = "lastIterator";
                    ctt.setShardTimeCheckpoint(shardId, streamJob.getEndTimeInMillis() - 3600000, iterator);
                    shardWithLatestIterator.put(shardId, iterator);
                }
            }

            StreamJob streamJob = new StreamJob(dataTable, streamDetails.getStreamId(), UUID.randomUUID().toString(), new HashSet<String>(shardIds), startTime, endTime);
            List<StreamShard> allShards = OTSHelper.getOrderedShardList(client, streamDetails.getStreamId());
            ctt.writeStreamJob(streamJob);

            OTSStreamReaderSlaveProxy slave = new OTSStreamReaderSlaveProxy();
            Set<String> ownedShards = shardIds;

            slave.init(config, streamJob, allShards, ownedShards);

            assertTrue(!slave.isFindCheckpoints());
            assertEquals(slave.getAllShardsMap().keySet(), streamJob.getShardIds());
            assertEquals(slave.getOwnedShards().keySet(), shardIds);
            assertEquals(slave.getShardToCheckpointMap().size(), shardIds.size());
            for (Map.Entry<String, ShardCheckpoint> entry : slave.getShardToCheckpointMap().entrySet()) {
                String shardId = entry.getKey();
                ShardCheckpoint checkpoint = entry.getValue();
                if (shardWithLatestIterator.containsKey(shardId)) {
                    assertEquals(checkpoint, new ShardCheckpoint(shardId, streamJob.getVersion(), shardWithLatestIterator.get(shardId), 0));
                } else {
                    assertEquals(checkpoint, new ShardCheckpoint(shardId, streamJob.getVersion(), CheckpointPosition.TRIM_HORIZON, 0));
                }
            }
        }

        // checkpoint is found, new shards's checkpoint is set to TRIM_HORIZON
        {
            CheckpointTimeTracker ctt = new CheckpointTimeTracker(client, config.getStatusTable(), streamDetails.getStreamId());

            Map<String, String> shardWithLatestIterator = new HashMap<String, String>();
            // write checkpoint of last job
            {
                StreamJob streamJob = new StreamJob(dataTable, streamDetails.getStreamId(), "last_version", new HashSet<String>(shardIds), startTime - 3600000, startTime);
                ctt.writeStreamJob(streamJob);

                // write shard checkpoints
                List<String> shardIdList = new ArrayList<String>(streamJob.getShardIds());
                for (int i = 0; i < shardIdList.size(); i++) {
                    ctt.writeCheckpoint(streamJob.getEndTimeInMillis(), new ShardCheckpoint(shardIdList.get(i), streamJob.getVersion(), "shard_iterator", 2));
                }
            }

            StreamJob streamJob = new StreamJob(dataTable, streamDetails.getStreamId(), UUID.randomUUID().toString(), new HashSet<String>(shardIds), startTime, endTime);
            List<StreamShard> allShards = OTSHelper.getOrderedShardList(client, streamDetails.getStreamId());
            ctt.writeStreamJob(streamJob);

            OTSStreamReaderSlaveProxy slave = new OTSStreamReaderSlaveProxy();
            Set<String> ownedShards = shardIds;

            slave.init(config, streamJob, allShards, ownedShards);

            assertTrue(slave.isFindCheckpoints());
            assertEquals(slave.getAllShardsMap().keySet(), streamJob.getShardIds());
            Map<String, StreamShard> ownedShardsGot = slave.getOwnedShards();
            assertEquals(ownedShardsGot.keySet(), shardIds);
            Map<String, ShardCheckpoint> allShardCps = slave.getShardToCheckpointMap();
            assertEquals(allShardCps.size(), shardIds.size());
            for (Map.Entry<String, ShardCheckpoint> entry : allShardCps.entrySet()) {
                String shardId = entry.getKey();
                ShardCheckpoint checkpoint = entry.getValue();
                assertEquals(checkpoint, new ShardCheckpoint(shardId, streamJob.getVersion(), "shard_iterator", 2));
            }
        }
    }

    @Test
    public void testRunJob() throws Exception {
        String dataTable = "DataTable_TestStreamReaderMasterProxy";
        String statusTable = "StatusTable_TestStreamReaderMasterProxy";
        long now = System.currentTimeMillis();
        long startTime = now - 7200000;
        long endTime = now - 3600000;
        Configuration configuration = ConfigurationHelper.loadConf(dataTable, statusTable, startTime, endTime);
        OTSStreamReaderConfig config = OTSStreamReaderConfig.load(configuration);
        client = OTSHelper.getOTSInstance(config);
        TestHelper.deleteTable(statusTable);
        TestHelper.deleteTable(dataTable);

        OTSStreamReaderChecker checker = new OTSStreamReaderChecker(client, config);
        checker.checkAndCreateStatusTableIfNotExist();

        config.setColumns(Arrays.asList("pk0", "pk1", "col0", "col1", "col2"));

        Thread.sleep(1000);

        List<Long> splitPoints = Arrays.asList(300L, 600L, 900L, 1200L, 1500L, 1800L, 2100L, 2400L,
                2700L, 3000L, 4000L, 5000L, 8000L, 10000L, 100000L, 200000l);
        StreamDetails streamDetails = prepareDataTable(dataTable, splitPoints);
        CheckpointTimeTracker ctt = new CheckpointTimeTracker(client, statusTable, streamDetails.getStreamId());

        List<Pair<Long, Long>> checkpoints = new ArrayList<Pair<Long, Long>>();
        long rowCount = TestHelper.prepareData(client, dataTable, 120000, checkpoints);
        LOG.info("Checkpoints: {}. RowCount: {}.", checkpoints, rowCount);

        startTime = checkpoints.get(0).getFirst();
        endTime = checkpoints.get(checkpoints.size() - 1).getFirst();
        config.setStartTimestampMillis(startTime);
        config.setEndTimestampMillis(endTime);

        Thread.sleep(3000);

        List<StreamShard> shards = OTSHelper.getOrderedShardList(client, streamDetails.getStreamId());
        assertEquals(shards.size(), splitPoints.size() + 1);

        Map<String, StreamShard> shardsMap = new HashMap<String, StreamShard>();
        for (StreamShard shard : shards) {
            shardsMap.put(shard.getShardId(), shard);
        }

        // read all data
        LOG.info("Start read all data.");
        config.setMode(Mode.SINGLE_VERSION_AND_UPDATE_ONLY);

        StreamJob streamJob = new StreamJob(dataTable, streamDetails.getStreamId(), "version", shardsMap.keySet(), startTime, endTime);
        OTSStreamReaderSlaveProxy slave = new OTSStreamReaderSlaveProxy();
        List<StreamShard> allShards = OTSHelper.getOrderedShardList(client, streamDetails.getStreamId());

        slave.init(config, streamJob, allShards, streamJob.getShardIds());
        assertTrue(!slave.isFindCheckpoints());

        config.setThreadNum(3);

        // check all the data
        RecordSenderForTest dataxRs = new RecordSenderForTest();
        slave.startRead(dataxRs);

        assertEquals(dataxRs.getRecords().size(), rowCount);
        sortRecord(dataxRs.getRecords());
        TestHelper.checkRecordsInSingleVer(dataxRs.getRecords(), 0);

        // check checkpoint
        Map<String, ShardCheckpoint> allCps = ctt.getAllCheckpoints(streamJob.getEndTimeInMillis());
        assertEquals(allCps.size(), streamJob.getShardIds().size());
        for (String shardId : streamJob.getShardIds()) {
            ShardCheckpoint cp = allCps.get(shardId);
            assertNotNull(cp);
            assertTrue(!cp.getCheckpoint().equals(CheckpointPosition.TRIM_HORIZON));
            assertTrue(!cp.getCheckpoint().equals(CheckpointPosition.SHARD_END));
            assertEquals(cp.getSkipCount(), 0);

            // check there is no more data with this iterator
            checkNoMoreData(client, streamJob, cp);
        }
    }

    private void checkNoMoreData(SyncClientInterface client, StreamJob streamJob, ShardCheckpoint cp) {
        GetStreamRecordRequest request = new GetStreamRecordRequest(cp.getCheckpoint());
        GetStreamRecordResponse response = client.getStreamRecord(request);
        assertEquals(response.getRecords().size(), 0);
        assertEquals(response.getNextShardIterator(), cp.getCheckpoint());
    }

    private void sortRecord(List<Record> records) {
        Collections.sort(records, new Comparator<Record>() {
            @Override
            public int compare(Record r1, Record r2) {
                long id1 = r1.getColumn(0).asLong();
                long id2 = r2.getColumn(0).asLong();
                return new Long(id1).compareTo(id2);
            }
        });
    }

    @Test
    public void testCheckCheckpoint() {
        OTSStreamReaderSlaveProxy slave = new OTSStreamReaderSlaveProxy();
        Map<String, ShardCheckpoint> allCps = new HashMap<String, ShardCheckpoint>();
        List<String> shardIds = new ArrayList<String>();
        shardIds.add("shard1");
        shardIds.add("shard2");
        shardIds.add("shard3");
        shardIds.add("shard4");
        shardIds.add("shard5");
        StreamJob streamJob = new StreamJob("dataTable", "streamId", "version", new HashSet<String>(shardIds), 0, 1);

        slave.checkCheckpoint(allCps, streamJob);

        // unknown shard found
        allCps.put("shard6", new ShardCheckpoint("shard6", streamJob.getVersion(), "iterator", 0));
        try {
            slave.checkCheckpoint(allCps, streamJob);
            fail();
        } catch (DataXException e) {
            assertTrue(e.getMessage().indexOf("Some shard from checkpoint is not belong to this job") != -1);
        }

        // different version
        allCps.clear();
        allCps.put("shard5", new ShardCheckpoint("shard5", streamJob.getVersion() + "!", "iterator", 0));
        try {
            slave.checkCheckpoint(allCps, streamJob);
            fail();
        } catch (DataXException e) {
           assertTrue(e.getMessage().indexOf("Version of checkpoint is not equal with version of this job") != -1);
        }
   }
}
