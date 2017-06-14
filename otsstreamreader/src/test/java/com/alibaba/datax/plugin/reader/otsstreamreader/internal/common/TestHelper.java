package com.alibaba.datax.plugin.reader.otsstreamreader.internal.common;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.OTSStreamReaderMasterProxy;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.OTSStreamReaderSlaveProxy;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.config.OTSStreamReaderConfig;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.config.OTSStreamReaderConstants;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.model.OTSErrorCode;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.model.StreamJob;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.utils.GsonParser;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.utils.OTSHelper;
import com.alicloud.openservices.tablestore.*;
import com.alicloud.openservices.tablestore.core.utils.Pair;
import com.alicloud.openservices.tablestore.model.*;
import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestHelper {

    public static void deleteTable(String tableName) {
        Configuration configuration = ConfigurationHelper.loadConf("aa", "bb", 1, 2);
        OTSStreamReaderConfig config = OTSStreamReaderConfig.load(configuration);
        SyncClientInterface ots = OTSHelper.getOTSInstance(config);

        try {
            ots.deleteTable(new DeleteTableRequest(tableName));
        } catch (TableStoreException ex) {
            if (!ex.getErrorCode().equals(OTSErrorCode.OBJECT_NOT_EXIST)) {
                throw ex;
            }
        } finally {
            ots.shutdown();
        }
    }

    /**
     * 每个shard的startTime和endTime在minTime和maxTime范围内，是否有数据和数据量随机。
     */
    public static List<ShardInfoForTest> getShardsNoRelationShip(int shardCount, long minTime, long maxTime, int maxRowNum) {
        List<ShardInfoForTest> shardInfos = new ArrayList<ShardInfoForTest>();
        for (int i = 0; i < shardCount; i++) {
            String shardId = String.format("shard%05d", i);
            long startTime = Utils.getRandomLong(maxTime - minTime) + minTime;
            long endTime = Utils.getRandomLong(maxTime - startTime) + startTime;
            int rowNum = Utils.getRandomBoolean() ? 0 : Utils.getRandomInt(maxRowNum);
            ShardInfoForTest shardInfoForTest = new ShardInfoForTest(shardId, null, null, startTime, endTime, rowNum);
            shardInfos.add(shardInfoForTest);
        }
        return shardInfos;
    }

    /**
     * shard之间只有两层的父子关系，split或者merge的时间点均匀分散在minTime和maxTime之间。
     */
    public static List<ShardInfoForTest> getShardsSimpleRelationShip(int shardCount, long minTime, long maxTime, int maxRowNum) {
        int splitOrMergeTimes = shardCount / 3;
        List<ShardInfoForTest> shardInfos = getShardsNoRelationShip(shardCount % 3, minTime, maxTime, maxRowNum);
        long step = (maxTime - minTime) / splitOrMergeTimes;
        long middleTime = minTime;
        for (int i = shardCount % 3; i < shardCount; i += 3) {
            middleTime += step;
            boolean isSplit = Utils.getRandomBoolean();
            if (isSplit) {
                String parentShard = String.format("shard%05d", i);
                String childShard1 = String.format("shard%05d", i + 1);
                String childShard2 = String.format("shard%05d", i + 2);
                long parentStartTime = Utils.getRandomLong(middleTime - minTime) + minTime;
                long childEndTime1 = Utils.getRandomLong(maxTime - middleTime) + middleTime;
                long childEndTime2 = Utils.getRandomLong(maxTime - middleTime) + middleTime;
                int parentRowNum = Utils.getRandomBoolean() ? 0 : Utils.getRandomInt(maxRowNum);
                int childRowNum1 = Utils.getRandomBoolean() ? 0 : Utils.getRandomInt(maxRowNum);
                int childRowNum2 = Utils.getRandomBoolean() ? 0 : Utils.getRandomInt(maxRowNum);
                shardInfos.add(new ShardInfoForTest(parentShard, parentStartTime, middleTime, parentRowNum));
                shardInfos.add(new ShardInfoForTest(childShard1, parentShard, null, childShard2, middleTime, childEndTime1, childRowNum1));
                shardInfos.add(new ShardInfoForTest(childShard2, parentShard, null, middleTime, childEndTime2, childRowNum2));
            } else {
                String parentShard1 = String.format("shard%05d", i);
                String parentShard2 = String.format("shard%05d", i + 1);
                String childShard = String.format("shard%05d", i + 2);
                long parentStartTime1 = Utils.getRandomLong(middleTime - minTime) + minTime;
                long parentStartTime2 = Utils.getRandomLong(middleTime - minTime) + minTime;
                long childEndTime = Utils.getRandomLong(maxTime - middleTime) + middleTime;
                int parentRowNum1 = Utils.getRandomBoolean() ? 0 : Utils.getRandomInt(maxRowNum);
                int parentRowNum2 = Utils.getRandomBoolean() ? 0 : Utils.getRandomInt(maxRowNum);
                int childRowNum = Utils.getRandomBoolean() ? 0 : Utils.getRandomInt(maxRowNum);
                shardInfos.add(new ShardInfoForTest(parentShard1, parentStartTime1, middleTime, parentRowNum1));
                shardInfos.add(new ShardInfoForTest(parentShard2, parentStartTime2, middleTime, parentRowNum2));
                shardInfos.add(new ShardInfoForTest(childShard, parentShard1, parentShard2, middleTime, childEndTime, childRowNum));
            }
        }
        return shardInfos;
    }

    /**
     * shard之间具有较复杂的关系，图的结构是随机的。
     */
    public static List<ShardInfoForTest> getShardsComplicatedRelationShip(int shardCount, long minTime, long maxTime, int maxRowNum) {
        List<ShardInfoForTest> shardInfos = new ArrayList<ShardInfoForTest>();
        Set<Integer> openShardIdxs = new HashSet<Integer>();

        for (int i = 0; i < shardCount; i++) {
            int type = openShardIdxs.size() < 2 ? 0 : Utils.getRandomInt(3);
            if (i == (shardCount - 1) && type == 1) {
                type = 0;   // 最后一个shard不通过split得到，因为split会产生两个shard。
            }
            switch (type) {
                case 0: { // no parent
                    String shardId = String.format("shard%05d", i);
                    long startTime = Utils.getRandomLong(maxTime - minTime) + minTime;
                    long endTime = Utils.getRandomLong(maxTime - startTime) + startTime;
                    int rowNum = Utils.getRandomBoolean() ? 0 : Utils.getRandomInt(maxRowNum);
                    shardInfos.add(new ShardInfoForTest(shardId, startTime, endTime, rowNum));
                    openShardIdxs.add(i);
                    break;
                }
                case 1: { // split
                    List<Integer> list = Lists.newArrayList(openShardIdxs);
                    int parentIdx = list.get(Utils.getRandomInt(list.size()));
                    openShardIdxs.remove(parentIdx);
                    String parentId = shardInfos.get(parentIdx).getShardId();
                    long parentEndTime = shardInfos.get(parentIdx).getEndTime();
                    for (int j = i; j < i + 2; j++) {
                        String shardId = String.format("shard%05d", j);
                        long endTime = Utils.getRandomLong(maxTime - parentEndTime) + parentEndTime;
                        int rowNum = Utils.getRandomBoolean() ? 0 : Utils.getRandomInt(maxRowNum);
                        ShardInfoForTest shardInfo = new ShardInfoForTest(shardId, parentId, null, parentEndTime, endTime, rowNum);
                        if (j == i) {
                            shardInfo.setSiblingId(String.format("shard%05d", i + 1));
                        }
                        shardInfos.add(shardInfo);
                    }
                    openShardIdxs.add(i);
                    openShardIdxs.add(i + 1);
                    i++;
                    break;
                }
                case 2: { // merge
                    List<Integer> list = Lists.newArrayList(openShardIdxs);
                    int tmp1 = Utils.getRandomInt(list.size());
                    int tmp2 = Utils.getRandomInt(list.size() - 1);
                    tmp2 += (tmp2 >= tmp1) ? 1 : 0;
                    int parentIdx1 = list.get(tmp1);
                    int parentIdx2 = list.get(tmp2);
                    openShardIdxs.remove(parentIdx1);
                    openShardIdxs.remove(parentIdx2);
                    String parentId1 = shardInfos.get(parentIdx1).getShardId();
                    String parentId2 = shardInfos.get(parentIdx2).getShardId();
                    long maxParentEndTime = Math.max(shardInfos.get(parentIdx1).getEndTime(), shardInfos.get(parentIdx2).getEndTime());
                    long endTime = Utils.getRandomLong(maxTime - maxParentEndTime) + maxParentEndTime;
                    int rowNum = Utils.getRandomBoolean() ? 0 : Utils.getRandomInt(maxRowNum);
                    shardInfos.add(new ShardInfoForTest(String.format("shard%05d", i), parentId1, parentId2, maxParentEndTime, endTime, rowNum));
                    openShardIdxs.add(i);
                    break;
                }
            }
        }
        return shardInfos;
    }

    /**
     * 在startTime ～ endTime范围内写入大约rowNum行，实际写入行数可能比rowNum略少一点。
     */
    public static void prepareData(MockOTS mockOTS, ShardInfoForTest shardInfo) {
        String shardId = shardInfo.getShardId();
        if (shardInfo.getParentId() == null) {
            mockOTS.createShard(shardId, null, null);
        } else if (shardInfo.getParentSiblingId() == null) {
            if (shardInfo.getSiblingId() != null) {
                mockOTS.splitShard(shardInfo.getParentId(), shardId, shardInfo.getSiblingId());
            }
        } else {
            mockOTS.mergeShard(shardInfo.getParentId(), shardInfo.getParentSiblingId(), shardId);
        }

        long startTime = shardInfo.getStartTime();
        long endTime = shardInfo.getEndTime();
        int rowNum = shardInfo.getRowNum();
        int rowCount = 0;
        for (long i = startTime; i < endTime; i += Utils.getRandomInt(10)) {
            int expectCount = (int) ((i - startTime) * rowNum / (double) (endTime - startTime));
            if (expectCount - rowCount > 5) {
                List<StreamRecord> streamRecords = new ArrayList<StreamRecord>();
                for (int j = 0; j < expectCount - rowCount - Utils.getRandomInt(5); j++) {
                    StreamRecord record = new StreamRecord();
                    record.setRecordType(Utils.getRandomRecordType());
                    record.setPrimaryKey(Utils.getPrimaryKey(2));
                    record.setColumns(Utils.getRecordColumns(5, record.getRecordType()));
                    record.setSequenceInfo(new RecordSequenceInfo(0, i * 1000, j));
                    streamRecords.add(record);
                }
                mockOTS.appendRecords(shardId, streamRecords);
                rowCount = expectCount;
            }
        }
    }

    public static void prepareData(MockOTS mockOTS, List<ShardInfoForTest> shardInfos) {
        for (ShardInfoForTest shardInfo : shardInfos) {
            prepareData(mockOTS, shardInfo);
        }
    }

    public static List<StreamRecord> filterRecordsByTimeRange(MockOTS mockOTS, String shardId, long startTime, long endTime) {
        List<StreamRecord> records = new ArrayList<StreamRecord>();
        boolean shouldSkip = true;
        for (StreamRecord streamRecord : mockOTS.getShardRecords(shardId)) {
            if (streamRecord.getSequenceInfo().getTimestamp() / 1000 >= endTime) {
                break;
            }
            if (shouldSkip && streamRecord.getSequenceInfo().getTimestamp() / 1000 < startTime) {
                continue;
            }
            shouldSkip = false;
            records.add(streamRecord);
        }
        return records;
    }

    public static List<Map.Entry<StreamRecord, String>> filterRecordsByTimeRange(MockOTS mockOTS, List<String> shardIds, long startTime, long endTime) {
        List<Map.Entry<StreamRecord, String>> recordsWithShardId = new ArrayList<Map.Entry<StreamRecord, String>>();
        for (final String shardId : shardIds) {
            List<StreamRecord> streamRecords =
                    TestHelper.filterRecordsByTimeRange(mockOTS, shardId, startTime, endTime);
            for (final StreamRecord streamRecord : streamRecords) {
                recordsWithShardId.add(new Map.Entry<StreamRecord, String>() {
                    public StreamRecord getKey() {
                        return streamRecord;
                    }

                    public String getValue() {
                        return shardId;
                    }

                    public String setValue(String s) {
                        return null;
                    }
                });
            }
        }
        return recordsWithShardId;
    }

    public static void runReader(SyncClientInterface ots, OTSStreamReaderConfig config, RecordSender recordSender) throws Exception {
        OTSStreamReaderMasterProxy master = new OTSStreamReaderMasterProxy();
        master.init(config);
        List<Configuration> configurations = master.split(1);

        StreamJob streamJob = master.getStreamJob();

        OTSStreamReaderSlaveProxy slave = new OTSStreamReaderSlaveProxy();
        OTSStreamReaderConfig slaveConfig = GsonParser.jsonToConfig(
                (String) configurations.get(0).get(OTSStreamReaderConstants.CONF));

        List<StreamShard> allShards = OTSHelper.getOrderedShardList(ots, streamJob.getStreamId());
        slaveConfig.setOtsForTest(config.getOtsForTest());
        slave.init(slaveConfig, streamJob, allShards, streamJob.getShardIds());
        slave.startRead(recordSender);
        slave.close();
    }

    public static long prepareData(SyncClientInterface client, String dataTable, int runTimeInMillis, final List<Pair<Long, Long>> checkpoints) throws Exception {
        final AtomicLong counter = new AtomicLong(0);
        class DataWriter implements Runnable {
            SyncClientInterface client;
            String tableName;
            AtomicBoolean stop;

            public DataWriter(SyncClientInterface client, String tableName) {
                this.client = client;
                this.tableName = tableName;
                this.stop = new AtomicBoolean(true);
            }

            @Override
            public void run() {
                this.stop.set(false);

                while (!stop.get()) {
                    try {
                        // make checkpoint every 300 rows
                        if (counter.get() % 300 == 0) {
                            Thread.sleep(200);
                            long now = System.currentTimeMillis();
                            Thread.sleep(200);
                            checkpoints.add(new Pair<Long, Long>(now, counter.get()));
                        }

                        RowPutChange rowChange = new RowPutChange(tableName);
                        long index = counter.getAndIncrement();
                        PrimaryKey primaryKey = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                                .addPrimaryKeyColumn("pk0", PrimaryKeyValue.fromLong(index))
                                .addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("" + index))
                                .build();

                        rowChange.setPrimaryKey(primaryKey);
                        rowChange.addColumn("col0", ColumnValue.fromLong(index));
                        if (index % 3 == 0) {
                            rowChange.addColumn("col1", ColumnValue.fromString("" + index));
                        }
                        rowChange.addColumn("col2", ColumnValue.fromBoolean(index % 2 == 0));
                        rowChange.addColumn("col3", ColumnValue.fromLong(index));

                        PutRowRequest request = new PutRowRequest();
                        request.setRowChange(rowChange);
                        client.putRow(request);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }

                try {
                    Thread.sleep(1000);
                } catch (Exception e) {

                }
                checkpoints.add(new Pair<Long, Long>(System.currentTimeMillis(), counter.get()));
            }

            public void stop() {
                this.stop.set(true);
            }
        }

        DataWriter dataWriter = new DataWriter(client, dataTable);
        Thread thread = new Thread(dataWriter);
        thread.start();

        Thread.sleep(runTimeInMillis);

        dataWriter.stop();
        thread.join();

        return counter.get();
    }


    public static long checkRecordsInMultiVer(List<Record> records, long index) {
        int rowsCount = 0;
        for (Iterator<Record> iter = records.iterator(); iter.hasNext(); ) {
            Record record;
            // delete row
            assertTrue(iter.hasNext());
            record = iter.next();
            assertEquals(record.getColumnNumber(), 6);
            assertEquals(record.getColumn(0).asLong().longValue(), index);
            assertEquals(record.getColumn(1).asString(), Long.toString(index));
            assertNull(record.getColumn(2).asString());
            assertNull(record.getColumn(3).asString());
            assertNull(record.getColumn(4).asString());
            assertEquals(record.getColumn(5).asString(), "DR");

            assertTrue(iter.hasNext());
            record = iter.next();
            assertEquals(record.getColumnNumber(), 6);
            assertEquals(record.getColumn(0).asLong().longValue(), index);
            assertEquals(record.getColumn(1).asString(), Long.toString(index));
            assertEquals(record.getColumn(2).asString(), "col0");
            assertTrue(record.getColumn(3).asLong() > 0);
            assertEquals(record.getColumn(4).asLong().longValue(), index);
            assertEquals(record.getColumn(5).asString(), "U");

            if (index % 3 == 0) {
                assertTrue(iter.hasNext());
                record = iter.next();
                assertEquals(record.getColumnNumber(), 6);
                assertEquals(record.getColumn(0).asLong().longValue(), index);
                assertEquals(record.getColumn(1).asString(), Long.toString(index));
                assertEquals(record.getColumn(2).asString(), "col1");
                assertTrue(record.getColumn(3).asLong() > 0);
                assertEquals(record.getColumn(4).asString(), Long.toString(index));
                assertEquals(record.getColumn(5).asString(), "U");
            }

            assertTrue(iter.hasNext());
            record = iter.next();
            assertEquals(record.getColumnNumber(), 6);
            assertEquals(record.getColumn(0).asLong().longValue(), index);
            assertEquals(record.getColumn(1).asString(), Long.toString(index));
            assertEquals(record.getColumn(2).asString(), "col2");
            assertTrue(record.getColumn(3).asLong() > 0);
            assertEquals(record.getColumn(4).asBoolean(), index % 2 == 0);
            assertEquals(record.getColumn(5).asString(), "U");

            assertTrue(iter.hasNext());
            record = iter.next();
            assertEquals(record.getColumnNumber(), 6);
            assertEquals(record.getColumn(0).asLong().longValue(), index);
            assertEquals(record.getColumn(1).asString(), Long.toString(index));
            assertEquals(record.getColumn(2).asString(), "col3");
            assertTrue(record.getColumn(3).asLong() > 0);
            assertEquals(record.getColumn(4).asLong().longValue(), index);
            assertEquals(record.getColumn(5).asString(), "U");

            index++;
            rowsCount++;
        }
        return rowsCount;
    }

    public static void checkRecordsInSingleVer(List<Record> records, long index) {
        for (Record record : records) {
            assertEquals(record.getColumnNumber(), 5);
            long id = record.getColumn(0).asLong();
            assertEquals(id, index);
            index++;

            assertEquals(record.getColumn(1).asString(), Long.toString(id));
            assertEquals(record.getColumn(2).asLong().longValue(), id);
            if (id % 3 == 0) {
                assertEquals(record.getColumn(3).asString(), Long.toString(id));
            } else {
                assertNull(record.getColumn(3).asString());
            }

            assertEquals(record.getColumn(4).asBoolean(), id % 2 == 0);
        }
    }

}
