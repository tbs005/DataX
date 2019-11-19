package com.alibaba.datax.plugin.reader.otsstreamreader.internal;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.common.ConfigurationHelper;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.common.TestHelper;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.config.OTSStreamReaderConfig;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.config.OTSStreamReaderConstants;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.core.CheckpointTimeTracker;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.core.OTSStreamReaderChecker;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.model.ShardCheckpoint;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.model.StreamJob;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.utils.GsonParser;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.utils.OTSHelper;
import com.alicloud.openservices.tablestore.SyncClientInterface;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.model.internal.CreateTableRequestEx;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.junit.Assert.*;

public class TestStreamReaderMasterProxy {

    private static final Logger LOG = LoggerFactory.getLogger(TestStreamReaderMasterProxy.class);
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
        OTSStreamReaderMasterProxy master = new OTSStreamReaderMasterProxy();

        List<Long> splitPoints = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L);
        StreamDetails streamDetails = prepareDataTable(dataTable, splitPoints);
        List<StreamShard> shards = OTSHelper.getOrderedShardList(client, streamDetails.getStreamId());
        CheckpointTimeTracker ctt = new CheckpointTimeTracker(client, config.getStatusTable(), streamDetails.getStreamId());
        // insert some checkpoints with endTime
        ctt.writeCheckpoint(endTime, new ShardCheckpoint(shards.get(0).getShardId(), "version", "iterator", 0));
        ctt.writeCheckpoint(endTime, new ShardCheckpoint(shards.get(1).getShardId(), "version", "iterator", 0));

        assertNotNull(ctt.readCheckpoint(shards.get(0).getShardId(), endTime));
        assertNotNull(ctt.readCheckpoint(shards.get(1).getShardId(), endTime));

        master.init(config);

        // checkpoint with endTime should be deleted
        assertNull(ctt.readCheckpoint(shards.get(0).getShardId(), endTime));
        assertNull(ctt.readCheckpoint(shards.get(1).getShardId(), endTime));

        StreamJob streamJob = master.getStreamJob();
        assertNotNull(streamJob);
        assertEquals(streamJob.getStreamId(), streamDetails.getStreamId());
        assertEquals(streamJob.getTableName(), dataTable);
        assertEquals(streamJob.getShardIds().size(), splitPoints.size() + 1);
        assertNotNull(streamJob.getVersion());

        StreamJob streamJobGot = ctt.readStreamJob(endTime);

        assertEquals(streamJob.getShardIds(), streamJobGot.getShardIds());
        assertEquals(streamJob.getTableName(), streamJobGot.getTableName());
        assertEquals(streamJob.getStreamId(), streamJobGot.getStreamId());
        assertEquals(streamJob.getVersion(), streamJobGot.getVersion());
        assertEquals(streamJob.getStartTimeInMillis(), streamJobGot.getStartTimeInMillis());
        assertEquals(streamJob.getEndTimeInMillis(), streamJobGot.getEndTimeInMillis());

        master.close();
    }

    @Test
    public void testSplit() throws Exception {
        OTSStreamReaderMasterProxy master = new OTSStreamReaderMasterProxy();

        {
            Set<String> shardIds = new HashSet<String>();
            for (int i = 0; i < 87; i++) {
                shardIds.add(Integer.toString(i));
            }
            StreamJob streamJob = new StreamJob("TableName", "StreamId", "Version", shardIds, 0, 1);
            master.setStreamJob(streamJob);

            List<Configuration> confs = master.split(10);
            assertEquals(confs.size(), 10);

            Set<String> allShards = new HashSet<String>();
            for (Configuration conf : confs) {
                checkConf(conf, shardIds, 10, allShards);
            }

            assertEquals(allShards.size(), shardIds.size());
        }
        {
            Set<String> shardIds = new HashSet<String>();
            for (int i = 0; i < 13; i++) {
                shardIds.add(Integer.toString(i));
            }
            StreamJob streamJob = new StreamJob("TableName", "StreamId", "Version", shardIds, 0, 1);
            master.setStreamJob(streamJob);

            List<Configuration> confs = master.split(20);
            assertEquals(confs.size(), 13);

            Set<String> allShards = new HashSet<String>();
            for (Configuration conf : confs) {
                checkConf(conf, shardIds, 20, allShards);
            }

            assertEquals(allShards.size(), shardIds.size());
        }
    }

    private void checkConf(Configuration conf, Set<String> shardIds, int split, Set<String> allShards) {
        int minCount = shardIds.size() / split;
        int maxCount = minCount + (shardIds.size() % split == 0 ? 0 : 1);

        List<String> ownedShards = GsonParser.jsonToList((String)conf.get(OTSStreamReaderConstants.OWNED_SHARDS));
        assertTrue(ownedShards.size() >= minCount);
        assertTrue(ownedShards.size() <= maxCount);

        StreamJob streamJob = StreamJob.fromJson((String)conf.get(OTSStreamReaderConstants.STREAM_JOB));
        Set<String> shards = streamJob.getShardIds();
        assertEquals(shards.size(), shardIds.size());

        allShards.addAll(ownedShards);
    }


}
