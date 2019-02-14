package com.alibaba.datax.plugin.reader.otsstreamreader.internal.core;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.OTSStreamReaderException;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.common.ConfigurationHelper;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.common.TestHelper;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.config.OTSStreamReaderConfig;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.config.OTSStreamReaderConstants;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.config.StatusTableConstants;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.model.ShardCheckpoint;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.model.StreamJob;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.utils.OTSHelper;
import com.alicloud.openservices.tablestore.*;
import com.alicloud.openservices.tablestore.model.*;
import com.aliyun.openservices.ots.internal.streamclient.model.CheckpointPosition;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;

public class TestOTSStreamReaderChecker {

    private static SyncClientInterface ots;
    private static String dataTable = "DataTable_TestOTSStreamReaderChecker";
    private static String statusTable = "StatusTable_TestOTSStreamReaderChecker";
    private static OTSStreamReaderConfig config;

    @BeforeClass
    public static void beforeClass() {
        Configuration configuration = ConfigurationHelper.loadConf(dataTable, statusTable, 1, 2);
        config = OTSStreamReaderConfig.load(configuration);
        ots = OTSHelper.getOTSInstance(config);
    }

    @Before
    public void deleteTable() {
        TestHelper.deleteTable(dataTable);
        TestHelper.deleteTable(statusTable);
    }

    @Test
    public void testCheckStreamEnabledAndTimeStampOK() {
        OTSStreamReaderChecker checker = new OTSStreamReaderChecker(ots, config);
        // table is not exist
        {
            try {
                checker.checkStreamEnabledAndTimeRangeOK();
                fail();
            } catch (OTSStreamReaderException ex) {
                assertEquals("The data table is not exist.", ex.getMessage());
            }
        }

        // stream is not enabled
        {
            TableMeta tableMeta = new TableMeta(config.getDataTable());
            tableMeta.addPrimaryKeyColumns(StatusTableConstants.STATUS_TABLE_PK_SCHEMA);
            TableOptions tableOptions = new TableOptions(-1, 1);
            ReservedThroughput reservedThroughput = new ReservedThroughput(1000, 1000);
            CreateTableRequest createTableRequest = new CreateTableRequest(tableMeta, tableOptions, reservedThroughput);
            ots.createTable(createTableRequest);

            try {
                checker.checkStreamEnabledAndTimeRangeOK();
                fail();
            } catch (OTSStreamReaderException ex) {
                assertEquals("The stream of data table is not enabled.", ex.getMessage());
            }
        }

        // time range has expired
        {
            UpdateTableRequest updateTableRequest = new UpdateTableRequest(config.getDataTable());
            updateTableRequest.setStreamSpecification(new StreamSpecification(true, 3));
            ots.updateTable(updateTableRequest);
            config.setStartTimestampMillis(1);
            config.setEndTimestampMillis(2);
            try {
                checker.checkStreamEnabledAndTimeRangeOK();
                fail();
            } catch (OTSStreamReaderException ex) {
                assertTrue(ex.getMessage().startsWith("As expiration time is"));
            }
        }

        // time range must beside offset
        {
            config.setStartTimestampMillis(System.currentTimeMillis());
            config.setEndTimestampMillis(System.currentTimeMillis() + 1);
            try {
                checker.checkStreamEnabledAndTimeRangeOK();
                fail();
            } catch (OTSStreamReaderException ex) {
                assertTrue(ex.getMessage().startsWith("To avoid timing error between different machines"));
            }
        }

        {
            config.setStartTimestampMillis(System.currentTimeMillis()
                    - 3 * 3600 * 1000 + OTSStreamReaderConstants.BEFORE_OFFSET_TIME_MILLIS - 1);
            config.setEndTimestampMillis(System.currentTimeMillis() - 1 * 3600 * 1000);
            try {
                checker.checkStreamEnabledAndTimeRangeOK();
                fail();
            } catch (OTSStreamReaderException ex) {
                assertTrue(ex.getMessage().startsWith("As expiration time is"));
            }
        }

        {
            config.setStartTimestampMillis(System.currentTimeMillis() - 3 * 3600 * 1000 + OTSStreamReaderConstants.BEFORE_OFFSET_TIME_MILLIS + 10000);
            config.setEndTimestampMillis(System.currentTimeMillis() - OTSStreamReaderConstants.AFTER_OFFSET_TIME_MILLIS - 1);
            checker.checkStreamEnabledAndTimeRangeOK();
        }
    }

    @Test
    public void testCheckAndCreateStatusTable() {
        Configuration configuration = ConfigurationHelper.loadConf(dataTable, statusTable, 1, 2);
        OTSStreamReaderConfig c = OTSStreamReaderConfig.load(configuration);
        OTSStreamReaderChecker checker = new OTSStreamReaderChecker(ots, c);

        boolean created = OTSHelper.checkTableExists(ots, c.getStatusTable());
        assertEquals(false, created);

        checker.checkAndCreateStatusTableIfNotExist();

        created = OTSHelper.checkTableExists(ots, c.getStatusTable());
        assertEquals(true, created);

        // table exist but with different table meta
        String tableName = "statusTableWithDifferentSchema";
        TestHelper.deleteTable(tableName);

        List<PrimaryKeySchema> pkSchema = Arrays.asList(
                new PrimaryKeySchema("StreamId", PrimaryKeyType.STRING),
                new PrimaryKeySchema("StatusType", PrimaryKeyType.INTEGER),
                new PrimaryKeySchema("StatusValue", PrimaryKeyType.STRING));
        TableMeta tableMeta = new TableMeta(tableName);
        tableMeta.addPrimaryKeyColumns(pkSchema);
        TableOptions tableOptions = new TableOptions(-1, 1);
        ReservedThroughput reservedThroughput = new ReservedThroughput(1000, 1000);
        CreateTableRequest createTableRequest = new CreateTableRequest(tableMeta, tableOptions, reservedThroughput);
        ots.createTable(createTableRequest);

        c.setStatusTable(tableName);
        try {
            checker.checkAndCreateStatusTableIfNotExist();
        } catch (Exception e) {
            assertEquals("Unexpected table meta in status table, please check your config.", e.getMessage());
        }
    }

    @Test
    public void testCheckAndSetCheckpoints() {
        String streamId = "streamId";
        CheckpointTimeTracker ctt = new CheckpointTimeTracker(ots, statusTable, streamId);
        OTSStreamReaderChecker checker = new OTSStreamReaderChecker(ots, config);
        checker.checkAndCreateStatusTableIfNotExist();

        long startTime = System.currentTimeMillis();
        long endTime = startTime + 1;
        config.setStartTimestampMillis(endTime);
        config.setEndTimestampMillis(endTime + 1);

        List<String> shardIds = new ArrayList<String>();
        Map<String, ShardCheckpoint> allCheckpoints = new HashMap<String, ShardCheckpoint>();
        for (int i = 0; i < 100; i++) {
            String shardId = "shardId_" + i;
            shardIds.add(shardId);

            allCheckpoints.put(shardId, new ShardCheckpoint(shardId, "new_version", CheckpointPosition.TRIM_HORIZON, 0));
        }

        // write last job's checkpoints
        StreamJob streamJob = new StreamJob(dataTable, streamId, "version_0", new HashSet<String>(shardIds), startTime, endTime);
        ctt.writeStreamJob(streamJob);

        List<ShardCheckpoint> checkpoints = new ArrayList<ShardCheckpoint>();
        for (int id = 0; id < shardIds.size(); id++) {
            String shardId = shardIds.get(id);
            ShardCheckpoint checkpoint = new ShardCheckpoint(shardId, streamJob.getVersion(), "shard_iterator_" + id, id);
            checkpoints.add(checkpoint);
            ctt.writeCheckpoint(endTime, checkpoint);
        }

        boolean findCheckpoint = checker.checkAndSetCheckpoints(ctt, new HashMap<String, StreamShard>(), streamJob, allCheckpoints);
        assertTrue(findCheckpoint);

        for (ShardCheckpoint checkpoint : checkpoints) {
            ShardCheckpoint checkpointGot = allCheckpoints.get(checkpoint.getShardId());
            assertNotNull(checkpointGot);
            assertEquals(checkpoint, checkpointGot);
        }
    }

    @Test
    public void testCheckAndSetCheckpoints_NoCheckpoint() {
        String streamId = "streamId";
        CheckpointTimeTracker ctt = new CheckpointTimeTracker(ots, statusTable, streamId);
        OTSStreamReaderChecker checker = new OTSStreamReaderChecker(ots, config);
        checker.checkAndCreateStatusTableIfNotExist();

        long startTime = System.currentTimeMillis();
        long endTime = startTime + 1;
        config.setStartTimestampMillis(endTime);
        config.setEndTimestampMillis(endTime + 1);

        List<String> shardIds = new ArrayList<String>();
        Map<String, ShardCheckpoint> allCheckpoints = new HashMap<String, ShardCheckpoint>();
        for (int i = 0; i < 100; i++) {
            String shardId = "shardId_" + i;
            shardIds.add(shardId);

            allCheckpoints.put(shardId, new ShardCheckpoint(shardId, "new_version", CheckpointPosition.TRIM_HORIZON, 0));
        }

        StreamJob streamJob = new StreamJob(dataTable, streamId, "new_version", new HashSet<String>(shardIds), startTime, endTime);
        boolean findCheckpoint = checker.checkAndSetCheckpoints(ctt, new HashMap<String, StreamShard>(), streamJob, allCheckpoints);
        assertTrue(!findCheckpoint);
    }

    @Test
    public void testCheckAndSetCheckpoints_WithShardEnd() {
        String streamId = "streamId";
        CheckpointTimeTracker ctt = new CheckpointTimeTracker(ots, statusTable, streamId);
        OTSStreamReaderChecker checker = new OTSStreamReaderChecker(ots, config);
        checker.checkAndCreateStatusTableIfNotExist();

        long startTime = System.currentTimeMillis();
        long endTime = startTime + 1;
        config.setStartTimestampMillis(endTime);
        config.setEndTimestampMillis(endTime + 1);

        List<String> shardIds = new ArrayList<String>();
        Map<String, ShardCheckpoint> allCheckpoints = new HashMap<String, ShardCheckpoint>();
        for (int i = 0; i < 100; i++) {
            String shardId = "shardId_" + i;
            shardIds.add(shardId);

            if (i < 50) {
                allCheckpoints.put(shardId, new ShardCheckpoint(shardId, "new_version", CheckpointPosition.TRIM_HORIZON, 0));
            }
        }

        for (int i = 100; i < 150; i++) {
            String shardId = "shardId_" + i;
            allCheckpoints.put(shardId, new ShardCheckpoint(shardId, "new_version", CheckpointPosition.TRIM_HORIZON, 0));
        }

        // write last job's checkpoints
        StreamJob streamJob = new StreamJob(dataTable, streamId, "version_0", new HashSet<String>(shardIds), startTime, endTime);
        ctt.writeStreamJob(streamJob);

        List<ShardCheckpoint> checkpoints = new ArrayList<ShardCheckpoint>();
        for (int id = 0; id < shardIds.size(); id++) {
            String shardId = shardIds.get(id);
            ShardCheckpoint checkpoint = new ShardCheckpoint(shardId, streamJob.getVersion(), id >= 50 ? CheckpointPosition.SHARD_END : "shard_iterator_" + id, id);
            checkpoints.add(checkpoint);
            ctt.writeCheckpoint(endTime, checkpoint);
        }

        boolean findCheckpoint = checker.checkAndSetCheckpoints(ctt, new HashMap<String, StreamShard>(), streamJob, allCheckpoints);
        assertTrue(findCheckpoint);

        for (int i = 0; i < checkpoints.size(); i++) {
            ShardCheckpoint checkpoint = checkpoints.get(i);
            ShardCheckpoint checkpointGot = allCheckpoints.get(checkpoint.getShardId());
            if (i < 50) {
                assertNotNull(checkpointGot);
                assertEquals(checkpoint, checkpointGot);
            } else {
                assertNull(checkpointGot);
            }
        }

        for (int i = 100; i < 150; i++) {
            String shardId = "shardId_" + i;
            assertEquals(allCheckpoints.get(shardId).getCheckpoint(), CheckpointPosition.TRIM_HORIZON);
        }
    }

    @Test
    public void testCheckAndSetCheckpoints_WithShardNotFinished() {
        String streamId = "streamId";
        CheckpointTimeTracker ctt = new CheckpointTimeTracker(ots, statusTable, streamId);
        OTSStreamReaderChecker checker = new OTSStreamReaderChecker(ots, config);
        checker.checkAndCreateStatusTableIfNotExist();

        long startTime = System.currentTimeMillis();
        long endTime = startTime + 1;
        config.setStartTimestampMillis(endTime);
        config.setEndTimestampMillis(endTime + 1);

        List<String> shardIds = new ArrayList<String>();
        Map<String, ShardCheckpoint> allCheckpoints = new HashMap<String, ShardCheckpoint>();
        for (int i = 0; i < 100; i++) {
            String shardId = "shardId_" + i;
            shardIds.add(shardId);

            allCheckpoints.put(shardId, new ShardCheckpoint(shardId, "new_version", CheckpointPosition.TRIM_HORIZON, 0));
        }

        for (int i = 100; i < 150; i++) {
            String shardId = "shardId_" + i;
            shardIds.add(shardId);
        }

        // write last job's checkpoints
        StreamJob streamJob = new StreamJob(dataTable, streamId, "version_0", new HashSet<String>(shardIds), startTime, endTime);
        ctt.writeStreamJob(streamJob);

        List<ShardCheckpoint> checkpoints = new ArrayList<ShardCheckpoint>();
        for (int id = 0; id < shardIds.size(); id++) {
            String shardId = shardIds.get(id);
            ShardCheckpoint checkpoint = new ShardCheckpoint(shardId, streamJob.getVersion(), "shard_iterator_" + id, id);
            checkpoints.add(checkpoint);
            ctt.writeCheckpoint(endTime, checkpoint);
        }

        try {
            checker.checkAndSetCheckpoints(ctt, new HashMap<String, StreamShard>(), streamJob, allCheckpoints);
            fail();
        } catch (Exception e) {
            assertTrue(e.getMessage().startsWith("Shard does not exist now"));
        }
    }

    @Test
    public void testCheckAndSetCheckpoints_ShardLost() {
        String streamId = "streamId";
        CheckpointTimeTracker ctt = new CheckpointTimeTracker(ots, statusTable, streamId);
        OTSStreamReaderChecker checker = new OTSStreamReaderChecker(ots, config);
        checker.checkAndCreateStatusTableIfNotExist();

        long startTime = System.currentTimeMillis();
        long endTime = startTime + 1;
        config.setStartTimestampMillis(endTime);
        config.setEndTimestampMillis(endTime + 1);

        List<String> shardIds = new ArrayList<String>();
        Map<String, ShardCheckpoint> allCheckpoints = new HashMap<String, ShardCheckpoint>();
        Map<String, StreamShard> allShards = new HashMap<String, StreamShard>();
        for (int i = 0; i < 100; i++) {
            String shardId = "shardId_" + i;
            shardIds.add(shardId);

            allCheckpoints.put(shardId, new ShardCheckpoint(shardId, "new_version", CheckpointPosition.TRIM_HORIZON, 0));
            allShards.put(shardId, new StreamShard(shardId));
        }

        // write last job's checkpoints
        StreamJob streamJob = new StreamJob(dataTable, streamId, "version_0", new HashSet<String>(shardIds), startTime, endTime);
        ctt.writeStreamJob(streamJob);

        List<ShardCheckpoint> checkpoints = new ArrayList<ShardCheckpoint>();
        for (int id = 0; id < shardIds.size(); id++) {
            String shardId = shardIds.get(id);
            ShardCheckpoint checkpoint = new ShardCheckpoint(shardId, streamJob.getVersion(), "shard_iterator_" + id, id);
            checkpoints.add(checkpoint);
            ctt.writeCheckpoint(endTime, checkpoint);
        }

        // set shard with an lost shard
        allShards.get("shardId_50").setParentId("shardId_200");

        try {
            checker.checkAndSetCheckpoints(ctt, allShards, streamJob, allCheckpoints);
            fail();
        } catch (Exception e) {
            assertTrue(e.getMessage().startsWith("Can't find checkpoint for shard"));
        }
    }
}
