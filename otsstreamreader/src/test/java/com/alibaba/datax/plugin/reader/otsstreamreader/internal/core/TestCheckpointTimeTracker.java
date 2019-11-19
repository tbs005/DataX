package com.alibaba.datax.plugin.reader.otsstreamreader.internal.core;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.common.ConfigurationHelper;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.common.TestHelper;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.config.OTSStreamReaderConfig;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.config.StatusTableConstants;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.model.ShardCheckpoint;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.model.StreamJob;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.utils.OTSHelper;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.*;
import org.junit.*;

import java.util.*;

import static org.junit.Assert.*;

public class TestCheckpointTimeTracker {

    public final String streamId = "StreamIdForTest";
    private static SyncClientInterface client;
    private static String dataTable = "DataTable_CheckpointTimeTrackerTest";
    private static String statusTable = "StatusTable_CheckpointTimeTrackerTest";
    private static OTSStreamReaderConfig config;
    private static OTSStreamReaderChecker otsStreamReaderChecker;

    @BeforeClass
    public static void beforeClass() {
        Configuration configuration = ConfigurationHelper.loadConf(dataTable, statusTable, 1, 2);
        config = OTSStreamReaderConfig.load(configuration);
        client = OTSHelper.getOTSInstance(config);
        otsStreamReaderChecker = new OTSStreamReaderChecker(client, config);
    }

    @Before
    public void deleteAndRecreateTable() {
        TestHelper.deleteTable(statusTable);
        otsStreamReaderChecker.checkAndCreateStatusTableIfNotExist();
    }

    private void writeShardCount(CheckpointTimeTracker ctt, long timestamp, int shardCount) {
        PrimaryKey primaryKey = ctt.getPrimaryKeyForShardCount(timestamp);

        RowPutChange rowPutChange = new RowPutChange(statusTable, primaryKey);
        rowPutChange.addColumn(StatusTableConstants.SHARDCOUNT_COLUMN_NAME, ColumnValue.fromLong(shardCount));

        PutRowRequest putRowRequest = new PutRowRequest(rowPutChange);
        client.putRow(putRowRequest);
    }

    @Test
    public void testGetShardCountForCheck() {
        CheckpointTimeTracker ctt = new CheckpointTimeTracker(client, statusTable, streamId);

        int shardCount = 666;
        long timestamp = System.currentTimeMillis();
        writeShardCount(ctt, timestamp, shardCount);

        long shardCountGot = ctt.getShardCountForCheck(timestamp);
        assertEquals(shardCount, shardCountGot);

        shardCountGot = ctt.getShardCountForCheck(1);
        assertEquals(shardCountGot, -1);
    }

    @Test
    public void testReadWriteCheckpoint() {
        CheckpointTimeTracker ctt = new CheckpointTimeTracker(client, statusTable, streamId);
        ShardCheckpoint scp = new ShardCheckpoint("shardId1", "version", "checkpoint", 1);

        long now = System.currentTimeMillis();
        ctt.writeCheckpoint(now, scp);

        ShardCheckpoint scpGot = ctt.readCheckpoint(scp.getShardId(), now);
        assertEquals(scp, scpGot);
    }

    @Test
    public void testGetAllCheckpoints() throws Exception {
        CheckpointTimeTracker ctt = new CheckpointTimeTracker(client, statusTable, streamId);

        Thread.sleep(1000);
        long now = System.currentTimeMillis();
        List<ShardCheckpoint> allScps = new ArrayList<ShardCheckpoint>();
        String version = "new_version";
        for (int i = 0; i < 10000; i++) {
            ShardCheckpoint scp = new ShardCheckpoint("shardId" + i, version, "checkpoint_" + i, i);
            ctt.writeCheckpoint(now, scp);
            allScps.add(scp);
        }

        Map<String, ShardCheckpoint> allScpsGot = ctt.getAllCheckpoints(now);
        assertEquals(allScps.size(), allScpsGot.size());
        for (ShardCheckpoint scp : allScps) {
            ShardCheckpoint scpGot = allScpsGot.get(scp.getShardId());
            assertNotNull(scpGot);
            assertEquals(scp, scpGot);
        }

        allScpsGot = ctt.getAllCheckpoints(now + 1);
        assertTrue(allScpsGot.isEmpty());
    }

    @Test
    public void clearAllCheckpoints() throws Exception {
        CheckpointTimeTracker ctt = new CheckpointTimeTracker(client, statusTable, streamId);

        Thread.sleep(1000);
        long now = System.currentTimeMillis();
        List<ShardCheckpoint> allScps = new ArrayList<ShardCheckpoint>();
        String version = "new_version";
        for (int i = 0; i < 100; i++) {
            ShardCheckpoint scp = new ShardCheckpoint("shardId" + i, version, "checkpoint_" + i, i);
            ctt.writeCheckpoint(now, scp);
            allScps.add(scp);
        }

        Map<String, ShardCheckpoint> allScpsGot = ctt.getAllCheckpoints(now);
        assertEquals(allScps.size(), allScpsGot.size());
        for (ShardCheckpoint scp : allScps) {
            ShardCheckpoint scpGot = allScpsGot.get(scp.getShardId());
            assertNotNull(scpGot);
            assertEquals(scp, scpGot);
        }

        ctt.clearAllCheckpoints(now);
        allScpsGot = ctt.getAllCheckpoints(now);
        assertTrue(allScpsGot.isEmpty());
    }

    private void writeOldCheckpoint(long timestamp, String shardId, String checkpointValue) {
        String statusValue = String.format("%16d", timestamp) + "\t" + shardId;

        List<PrimaryKeyColumn> pkCols = new ArrayList<PrimaryKeyColumn>();
        pkCols.add(new PrimaryKeyColumn("StreamId", PrimaryKeyValue.fromString(streamId)));
        pkCols.add(new PrimaryKeyColumn("StatusType", PrimaryKeyValue.fromString("CheckpointForDataxReader")));
        pkCols.add(new PrimaryKeyColumn("StatusValue", PrimaryKeyValue.fromString(statusValue)));

        PrimaryKey primaryKey = new PrimaryKey(pkCols);

        RowPutChange rowPutChange = new RowPutChange(statusTable, primaryKey);
        rowPutChange.addColumn("Checkpoint", ColumnValue.fromString(checkpointValue));

        PutRowRequest putRowRequest = new PutRowRequest(rowPutChange);
        client.putRow(putRowRequest);
    }

    @Test
    public void testCompatibleWithOldCheckpoint() {
        CheckpointTimeTracker ctt = new CheckpointTimeTracker(client, statusTable, streamId);
        ShardCheckpoint scp = new ShardCheckpoint("shardId_compatible", "", "checkpoint_compatible", 0);
        long now = System.currentTimeMillis();
        writeOldCheckpoint(now, scp.getShardId(), scp.getCheckpoint());

        ShardCheckpoint scpGot = ctt.readCheckpoint(scp.getShardId(), now);
        assertEquals(scp, scpGot);

        scpGot = ctt.readCheckpoint(scp.getShardId(), now + 1);
        assertNull(scpGot);
    }

    @Test
    public void testGetAllOldCheckpoints() throws Exception {
        CheckpointTimeTracker ctt = new CheckpointTimeTracker(client, statusTable, streamId);

        Thread.sleep(1000);
        long now = System.currentTimeMillis();
        List<ShardCheckpoint> allScps = new ArrayList<ShardCheckpoint>();
        for (int i = 0; i < 10000; i++) {
            ShardCheckpoint scp = new ShardCheckpoint("shardId" + i, "", "checkpoint_" + i, 0);
            writeOldCheckpoint(now, scp.getShardId(), scp.getCheckpoint());
            allScps.add(scp);
        }

        Map<String, ShardCheckpoint> allScpsGot = ctt.getAllCheckpoints(now);
        assertEquals(allScps.size(), allScpsGot.size());
        for (ShardCheckpoint scp : allScps) {
            ShardCheckpoint scpGot = allScpsGot.get(scp.getShardId());
            assertNotNull(scpGot);
            assertEquals(scp, scpGot);
        }

        allScpsGot = ctt.getAllCheckpoints(now + 1);
        assertTrue(allScpsGot.isEmpty());
    }

    @Test
    public void testReadWriteShardTimeCheckpoint() {
        CheckpointTimeTracker ctt = new CheckpointTimeTracker(client, statusTable, streamId);
        long now = System.currentTimeMillis();

        int count = 20;
        for (int i = 0; i < count; i++) {
            ctt.setShardTimeCheckpoint("stShardId_" + i, now, "checkpoint_0");
            ctt.setShardTimeCheckpoint("stShardId_" + i, now - 1, "checkpoint_1");
            ctt.setShardTimeCheckpoint("stShardId_" + i, now - 2, "checkpoint_2");
            ctt.setShardTimeCheckpoint("stShardId_" + i, now - 3, "checkpoint_3");
            ctt.setShardTimeCheckpoint("stShardId_" + i, now - 4, "checkpoint_4");
            ctt.setShardTimeCheckpoint("stShardId_" + i, now - 5, "checkpoint_5");
        }

        for (int i = 0; i < count; i++) {
            String checkpoint = ctt.getShardLargestCheckpointInTimeRange("stShardId_" + i, now - 1, now + 100);
            assertNotNull(checkpoint);
            assertEquals(checkpoint, "checkpoint_0");
        }

        String checkpoint = ctt.getShardLargestCheckpointInTimeRange("shardId_0", now + 1, now + 100);
        assertNull(checkpoint);

        checkpoint = ctt.getShardLargestCheckpointInTimeRange("shardId_999", now - 1, now + 100);
        assertNull(checkpoint);
    }

    @Test
    public void testReadWriteStreamJob() throws Exception {
        CheckpointTimeTracker ctt = new CheckpointTimeTracker(client, statusTable, streamId);
        long endTime = System.currentTimeMillis();
        long startTime = endTime - 3600;

        Set<String> shardIds = new HashSet<String>();
        for (int i = 0; i < 100000; i++) {
            shardIds.add("00ec4f3d-20f3-40d5-bb47-c6608720d4d8_1476847567848063_" + i);
        }

        StreamJob streamJob = new StreamJob(dataTable, streamId, "version_0", shardIds, startTime, endTime);
        ctt.writeStreamJob(streamJob);

        StreamJob streamJobGot = ctt.readStreamJob(endTime);

        assertEquals(streamJob.getShardIds(), streamJobGot.getShardIds());
        assertEquals(streamJob.getStartTimeInMillis(), streamJobGot.getStartTimeInMillis());
        assertEquals(streamJob.getEndTimeInMillis(), streamJobGot.getEndTimeInMillis());
        assertEquals(streamJob.getStreamId(), streamJobGot.getStreamId());
        assertEquals(streamJob.getTableName(), streamJobGot.getTableName());
        assertEquals(streamJob.getVersion(), streamJobGot.getVersion());
    }

    @Test
    public void testGetAndCheckAllCheckpoints() throws Exception {
        CheckpointTimeTracker ctt = new CheckpointTimeTracker(client, statusTable, streamId);

        long now = System.currentTimeMillis();

        // stream job match with checkpoints
        {
            long startTime = now;
            long endTime = now + 1;

            List<String> shardIds = new ArrayList<String>();
            for (int i = 0; i < 100; i++) {
                shardIds.add("shardId_" + i);
            }

            StreamJob streamJob = new StreamJob(dataTable, streamId, "version_0", new HashSet<String>(shardIds), startTime, endTime);
            ctt.writeStreamJob(streamJob);

            List<ShardCheckpoint> checkpoints = new ArrayList<ShardCheckpoint>();
            for (int id = 0; id < shardIds.size(); id++) {
                String shardId = shardIds.get(id);
                ShardCheckpoint checkpoint = new ShardCheckpoint(shardId, streamJob.getVersion(), "shard_iterator_" + id, id);
                checkpoints.add(checkpoint);
                ctt.writeCheckpoint(endTime, checkpoint);
            }

            Map<String,ShardCheckpoint> checkpointsGot = new HashMap<String, ShardCheckpoint>();
            boolean findCheckpoint = ctt.getAndCheckAllCheckpoints(endTime, streamId, checkpointsGot);
            assertTrue(findCheckpoint);

            assertEquals(checkpointsGot.size(), checkpoints.size());
            for (ShardCheckpoint checkpoint : checkpoints) {
                ShardCheckpoint checkpointGot = checkpointsGot.get(checkpoint.getShardId());
                assertNotNull(checkpointGot);

                assertEquals(checkpoint, checkpointGot);
            }
        }

        // checkpoints is less than that in job
        {
            long startTime = now + 1;
            long endTime = now + 2;

            List<String> shardIds = new ArrayList<String>();
            for (int i = 0; i < 100; i++) {
                shardIds.add("shardId_" + i);
            }

            StreamJob streamJob = new StreamJob(dataTable, streamId, "version_0", new HashSet<String>(shardIds), startTime, endTime);
            ctt.writeStreamJob(streamJob);

            List<ShardCheckpoint> checkpoints = new ArrayList<ShardCheckpoint>();
            for (int id = 0; id < shardIds.size() - 2; id++) {
                String shardId = shardIds.get(id);
                ShardCheckpoint checkpoint = new ShardCheckpoint(shardId, streamJob.getVersion(), "shard_iterator_" + id, id);
                checkpoints.add(checkpoint);
                ctt.writeCheckpoint(endTime, checkpoint);
            }

            Map<String,ShardCheckpoint> checkpointsGot = new HashMap<String, ShardCheckpoint>();
            boolean findCheckpoint = ctt.getAndCheckAllCheckpoints(endTime, streamId, checkpointsGot);
            assertTrue(!findCheckpoint);
            assertTrue(checkpointsGot.isEmpty());
        }

        // checkpoint is more than that in job
        {
            long startTime = now + 2;
            long endTime = now + 3;

            List<String> shardIds = new ArrayList<String>();
            for (int i = 0; i < 100; i++) {
                shardIds.add("shardId_" + i);
            }

            StreamJob streamJob = new StreamJob(dataTable, streamId, "version_0", new HashSet<String>(shardIds), startTime, endTime);
            ctt.writeStreamJob(streamJob);

            List<ShardCheckpoint> checkpoints = new ArrayList<ShardCheckpoint>();
            for (int id = 0; id < shardIds.size() - 3; id++) {
                String shardId = shardIds.get(id);
                ShardCheckpoint checkpoint = new ShardCheckpoint(shardId, streamJob.getVersion(), "shard_iterator_" + id, id);
                checkpoints.add(checkpoint);
                ctt.writeCheckpoint(endTime, checkpoint);
            }

            ctt.writeCheckpoint(endTime, new ShardCheckpoint("shardId_a", streamJob.getVersion(), "shard_iterator", 0));
            ctt.writeCheckpoint(endTime, new ShardCheckpoint("shardId_b", streamJob.getVersion(), "shard_iterator", 0));
            ctt.writeCheckpoint(endTime, new ShardCheckpoint("shardId_c", streamJob.getVersion(), "shard_iterator", 0));

            Map<String,ShardCheckpoint> checkpointsGot = new HashMap<String, ShardCheckpoint>();
            boolean findCheckpoint = ctt.getAndCheckAllCheckpoints(endTime, streamId, checkpointsGot);
            assertTrue(!findCheckpoint);
            assertTrue(checkpointsGot.isEmpty());
        }

        // all checkpoints with the same version, but with different version with job
        {
            long startTime = now + 3;
            long endTime = now + 4;

            List<String> shardIds = new ArrayList<String>();
            for (int i = 0; i < 100; i++) {
                shardIds.add("shardId_" + i);
            }

            StreamJob streamJob = new StreamJob(dataTable, streamId, "version_0", new HashSet<String>(shardIds), startTime, endTime);
            ctt.writeStreamJob(streamJob);

            List<ShardCheckpoint> checkpoints = new ArrayList<ShardCheckpoint>();
            for (int id = 0; id < shardIds.size(); id++) {
                String shardId = shardIds.get(id);
                ShardCheckpoint checkpoint = new ShardCheckpoint(shardId, streamJob.getVersion() + "!", "shard_iterator_" + id, id);
                checkpoints.add(checkpoint);
                ctt.writeCheckpoint(endTime, checkpoint);
            }

            Map<String,ShardCheckpoint> checkpointsGot = new HashMap<String, ShardCheckpoint>();
            boolean findCheckpoint = ctt.getAndCheckAllCheckpoints(endTime, streamId, checkpointsGot);
            assertTrue(!findCheckpoint);
            assertTrue(checkpointsGot.isEmpty());
        }

        // check point is mix with different versions
        {
            long startTime = now + 4;
            long endTime = now + 5;

            List<String> shardIds = new ArrayList<String>();
            for (int i = 0; i < 100; i++) {
                shardIds.add("shardId_" + i);
            }

            StreamJob streamJob = new StreamJob(dataTable, streamId, "version_0", new HashSet<String>(shardIds), startTime, endTime);
            ctt.writeStreamJob(streamJob);

            List<ShardCheckpoint> checkpoints = new ArrayList<ShardCheckpoint>();
            for (int id = 0; id < shardIds.size(); id++) {
                String shardId = shardIds.get(id);
                ShardCheckpoint checkpoint = new ShardCheckpoint(
                        shardId,
                        id > 2 && id < 10 ? streamJob.getVersion() + "!" : streamJob.getVersion(),
                        "shard_iterator_" + id,
                        id);

                checkpoints.add(checkpoint);
                ctt.writeCheckpoint(endTime, checkpoint);
            }

            Map<String,ShardCheckpoint> checkpointsGot = new HashMap<String, ShardCheckpoint>();
            boolean findCheckpoint = ctt.getAndCheckAllCheckpoints(endTime, streamId, checkpointsGot);
            assertTrue(!findCheckpoint);
            assertTrue(checkpointsGot.isEmpty());
        }

        // stream id is different
        {
            long startTime = now;
            long endTime = now + 1;

            List<String> shardIds = new ArrayList<String>();
            for (int i = 0; i < 100; i++) {
                shardIds.add("shardId_" + i);
            }

            StreamJob streamJob = new StreamJob(dataTable, streamId, "version_0", new HashSet<String>(shardIds), startTime, endTime);
            ctt.writeStreamJob(streamJob);

            List<ShardCheckpoint> checkpoints = new ArrayList<ShardCheckpoint>();
            for (int id = 0; id < shardIds.size(); id++) {
                String shardId = shardIds.get(id);
                ShardCheckpoint checkpoint = new ShardCheckpoint(shardId, streamJob.getVersion(), "shard_iterator_" + id, id);
                checkpoints.add(checkpoint);
                ctt.writeCheckpoint(endTime, checkpoint);
            }

            Map<String,ShardCheckpoint> checkpointsGot = new HashMap<String, ShardCheckpoint>();
            boolean findCheckpoint = ctt.getAndCheckAllCheckpoints(endTime, streamId + "!", checkpointsGot);
            assertTrue(!findCheckpoint);
            assertTrue(checkpointsGot.isEmpty());
        }
    }

    @Test
    public void testGetAndCheckAllOldCheckpoints() {
        CheckpointTimeTracker ctt = new CheckpointTimeTracker(client, statusTable, streamId);

        long now = System.currentTimeMillis();
        int shardCount = 100;
        List<ShardCheckpoint> checkpoints = new ArrayList<ShardCheckpoint>();
        for (int i = 0; i < shardCount; i++) {
            ShardCheckpoint checkpoint = new ShardCheckpoint("shardId_" + i, "", "shard_iterator_" + i, 0);
            writeOldCheckpoint(now, checkpoint.getShardId(), checkpoint.getCheckpoint());
            checkpoints.add(checkpoint);
        }

        // shard count not exist
        Map<String,ShardCheckpoint> checkpointsGot = new HashMap<String, ShardCheckpoint>();
        boolean findCheckpoint = ctt.getAndCheckAllCheckpoints(now, streamId, checkpointsGot);
        assertTrue(!findCheckpoint);
        assertTrue(checkpointsGot.isEmpty());

        // count not match
        writeShardCount(ctt, now, shardCount - 1);
        findCheckpoint = ctt.getAndCheckAllCheckpoints(now, streamId, checkpointsGot);
        assertTrue(!findCheckpoint);

        // with correct shard count
        writeShardCount(ctt, now, shardCount);

        findCheckpoint = ctt.getAndCheckAllCheckpoints(now, streamId, checkpointsGot);
        assertTrue(findCheckpoint);
        assertEquals(checkpointsGot.size(), shardCount);
        for (ShardCheckpoint checkpoint : checkpoints) {
            ShardCheckpoint checkpointGot = checkpointsGot.get(checkpoint.getShardId());
            assertNotNull(checkpointGot);

            assertEquals(checkpoint, checkpointGot);
        }

    }
}
