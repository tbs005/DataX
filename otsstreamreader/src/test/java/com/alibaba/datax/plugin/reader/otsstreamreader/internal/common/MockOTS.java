package com.alibaba.datax.plugin.reader.otsstreamreader.internal.common;

import com.alibaba.datax.plugin.reader.otsstreamreader.internal.model.OTSErrorCode;
import com.alicloud.openservices.tablestore.*;
import com.alicloud.openservices.tablestore.model.*;
import com.aliyun.openservices.ots.internal.model.OTSModelGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class MockOTS implements SyncClientInterface {

    private static final Logger LOG = LoggerFactory.getLogger(SyncClientInterface.class);

    private OTSModelGenerator otsModelGenerator = new OTSModelGenerator();
    private long creationTime;

    private NavigableMap<String, StreamShard> shardMap = new ConcurrentSkipListMap<String, StreamShard>();
    private Map<String, List<StreamRecord>> shardRecords = new ConcurrentHashMap<String, List<StreamRecord>>();
    private Map<String, Boolean> isShardClose = new ConcurrentHashMap<String, Boolean>();
    private Map<String, Integer> startIteratorIdx = new ConcurrentHashMap<String, Integer>();

    private String tableName;
    private boolean enableStream;
    private int expirationTime;
    private SyncClientInterface ots;
    private Exception exception;

    public MockOTS() {

    }

    public void throwException(Exception e) {
        this.exception = e;
    }

    private void throwException(){
        if (exception != null) {
            throw new ClientException(exception);
        }
    }

    public MockOTS(SyncClientInterface ots) {
        this.ots = ots;
    }

    public void setOTS(SyncClientInterface ots) {
        this.ots = ots;
    }

    public void enableStream(String tableName, int expirationTime) {
        disableStream();
        this.tableName = tableName;
        this.enableStream = true;
        this.expirationTime = expirationTime;
        this.creationTime = System.currentTimeMillis();
    }

    public void disableStream() {
        this.enableStream = false;
        shardMap.clear();
        shardRecords.clear();
        isShardClose.clear();
        startIteratorIdx.clear();
    }

    private void checkEnableStream() {
        if (!enableStream) {
            throw new TableStoreException("", null, OTSErrorCode.OBJECT_NOT_EXIST, "", 404);
        }
    }

    @Override
    public ListStreamResponse listStream(ListStreamRequest listStreamRequest) throws TableStoreException, ClientException {
        throwException();
        checkEnableStream();

        return otsModelGenerator.genListStreamResponse(listStreamRequest.getTableName(),
                listStreamRequest.getTableName() + "_Stream", creationTime);
    }

    @Override
    public DescribeStreamResponse describeStream(DescribeStreamRequest describeStreamRequest) throws TableStoreException, ClientException {
        throwException();
        checkEnableStream();

        List<StreamShard> shards = new ArrayList<StreamShard>();
        for (String shardId : shardMap.keySet()) {
            shards.add(shardMap.get(shardId));
        }
        return otsModelGenerator.genDescribeStreamResponse(describeStreamRequest.getStreamId().split("_")[0],
                describeStreamRequest.getStreamId(), creationTime, expirationTime, shards);
    }

    @Override
    public GetShardIteratorResponse getShardIterator(GetShardIteratorRequest getShardIteratorRequest) throws TableStoreException, ClientException {
        throwException();
        checkEnableStream();

        String shardId = getShardIteratorRequest.getShardId();
        if (shardMap.get(shardId) == null) {
            throw new RuntimeException("Illegal state.");
        }
        long idx = startIteratorIdx.get(shardId);
        return otsModelGenerator.genGetShardIteratorResponse(shardId + "\t" + idx);
    }

    @Override
    public GetStreamRecordResponse getStreamRecord(GetStreamRecordRequest getStreamRecordRequest) throws TableStoreException, ClientException {
        throwException();
        checkEnableStream();

        List<StreamRecord> streamRecords = new ArrayList<StreamRecord>();
        int limit = getStreamRecordRequest.getLimit();
        if (limit < 0) {
            limit = 10000000;
        }
        String iterator = getStreamRecordRequest.getShardIterator();

        if (iterator.split("\t").length != 2) {
            LOG.error("Requested stream data is already trimmed, Iterator: {}.", iterator);
            throw new TableStoreException("Requested stream data is already trimmed or does not exist.",
                    null, "OTSTrimmedDataAccess", "", 404);
        }

        String shardId = iterator.split("\t")[0];
        int idx = Integer.parseInt(iterator.split("\t")[1]);

        List<StreamRecord> list = shardRecords.get(shardId);
        synchronized (list) {
            long minIdx = startIteratorIdx.get(shardId);
            int endIdx = Math.min(idx + limit, list.size());

            if (idx < minIdx || idx > list.size()) {
                throw new TableStoreException("Requested stream data is already trimmed or does not exist.",
                        null, "OTSTrimmedDataAccess", "", 404);
            }

            for (int i = idx; i < endIdx; i++) {
                streamRecords.add(list.get(i));
            }
            String nextIterator = shardId + "\t" + endIdx;
            if (isShardClose.get(shardId) && endIdx == list.size()) {
                nextIterator = null;
            }
            return otsModelGenerator.genGetStreamRecordResponse(streamRecords, nextIterator);
        }
    }

    public void createShard(String shardId, String parentShardId, String parentSiblingShardId) {
        checkEnableStream();

        synchronized (shardMap) {
            if (shardMap.get(shardId) != null) {
                throw new RuntimeException("Illegal state.");
            }
            StreamShard streamShard = new StreamShard(shardId);
            streamShard.setParentId(parentShardId);
            streamShard.setParentSiblingId(parentSiblingShardId);
            shardMap.put(shardId, streamShard);
            shardRecords.put(shardId, new ArrayList<StreamRecord>());
            isShardClose.put(shardId, false);
            startIteratorIdx.put(shardId, 0);
        }
    }

    public void appendRecords(String shardId, List<StreamRecord> records) {
        checkEnableStream();

        if (shardMap.get(shardId) == null || isShardClose.get(shardId)) {
            throw new RuntimeException("Illegal state.");
        }
        List<StreamRecord> list = shardRecords.get(shardId);
        synchronized (list) {
            list.addAll(records);
        }
    }

    private void closeShard(String shardId) {
        checkEnableStream();

        synchronized (isShardClose) {
            if (shardMap.get(shardId) == null || isShardClose.get(shardId)) {
                throw new RuntimeException("Illegal state.");
            }
            isShardClose.put(shardId, true);
        }
    }

    public void splitShard(String shardId, String resShardId1, String resShardId2) {
        checkEnableStream();

        closeShard(shardId);
        createShard(resShardId1, shardId, null);
        createShard(resShardId2, shardId, null);
    }

    public void mergeShard(String shardId1, String shardId2, String resShardId) {
        checkEnableStream();

        closeShard(shardId1);
        closeShard(shardId2);
        createShard(resShardId, shardId1, shardId2);
    }

    public void deleteShard(String shardId) {
        shardMap.remove(shardId);
        shardRecords.remove(shardId);
        isShardClose.remove(shardId);
        startIteratorIdx.remove(shardId);
    }

    public synchronized void clearExpirationRecords() {
        for (String shardId : shardRecords.keySet()) {
            List<StreamRecord> streamRecords = shardRecords.get(shardId);
            int startIdx = startIteratorIdx.get(shardId);
            synchronized (streamRecords) {
                for (int i = startIdx; i < streamRecords.size(); i++) {
                    long ts = streamRecords.get(i).getSequenceInfo().getTimestamp();
                    if (ts < (System.currentTimeMillis() - expirationTime)) {
                        startIdx++;
                        streamRecords.set(i, null);
                    }
                }
                startIteratorIdx.put(shardId, startIdx);
            }
        }
    }

    public List<StreamRecord> getShardRecords(String shardId) {
        checkEnableStream();

        return shardRecords.get(shardId);
    }

    public void shutdown() {
        if (ots != null) {
            ots.shutdown();
        }
    }

    @Override
    public CreateTableResponse createTable(CreateTableRequest createTableRequest) throws TableStoreException, ClientException {
        throwException();
        if (ots != null) {
            return ots.createTable(createTableRequest);
        }
        throw new UnsupportedOperationException("");
    }

    @Override
    public UpdateTableResponse updateTable(UpdateTableRequest updateTableRequest) throws TableStoreException, ClientException {
        throwException();
        if (ots != null) {
            return ots.updateTable(updateTableRequest);
        }
        throw new UnsupportedOperationException("");
    }

    @Override
    public DescribeTableResponse describeTable(DescribeTableRequest describeTableRequest) throws TableStoreException, ClientException {
        throwException();
        if (describeTableRequest.getTableName().equals(tableName)) {
            StreamDetails streamDetails = new StreamDetails(enableStream, tableName + "_Stream", expirationTime, 0);
            return otsModelGenerator.genDescribeTableResponse(streamDetails);
        }
        if (ots != null) {
            return ots.describeTable(describeTableRequest);
        }
        throw new UnsupportedOperationException("");
    }

    @Override
    public ListTableResponse listTable() throws TableStoreException, ClientException {
        throwException();
        if (ots != null) {
            return ots.listTable();
        }
        throw new UnsupportedOperationException("");
    }

    @Override
    public DeleteTableResponse deleteTable(DeleteTableRequest deleteTableRequest) throws TableStoreException, ClientException {
        throwException();
        if (ots != null) {
            return ots.deleteTable(deleteTableRequest);
        }
        throw new UnsupportedOperationException("");
    }

    @Override
    public GetRowResponse getRow(GetRowRequest getRowRequest) throws TableStoreException, ClientException {
        throwException();
        if (ots != null) {
            return ots.getRow(getRowRequest);
        }
        throw new UnsupportedOperationException("");
    }

    @Override
    public PutRowResponse putRow(PutRowRequest putRowRequest) throws TableStoreException, ClientException {
        throwException();
        if (ots != null) {
            return ots.putRow(putRowRequest);
        }
        throw new UnsupportedOperationException("");
    }

    @Override
    public UpdateRowResponse updateRow(UpdateRowRequest updateRowRequest) throws TableStoreException, ClientException {
        throwException();
        if (ots != null) {
            return ots.updateRow(updateRowRequest);
        }
        throw new UnsupportedOperationException("");
    }

    @Override
    public DeleteRowResponse deleteRow(DeleteRowRequest deleteRowRequest) throws TableStoreException, ClientException {
        throwException();
        if (ots != null) {
            return ots.deleteRow(deleteRowRequest);
        }
        throw new UnsupportedOperationException("");
    }

    @Override
    public BatchGetRowResponse batchGetRow(BatchGetRowRequest batchGetRowRequest) throws TableStoreException, ClientException {
        throwException();
        if (ots != null) {
            return ots.batchGetRow(batchGetRowRequest);
        }
        throw new UnsupportedOperationException("");
    }

    @Override
    public BatchWriteRowResponse batchWriteRow(BatchWriteRowRequest batchWriteRowRequest) throws TableStoreException, ClientException {
        throwException();
        if (ots != null) {
            return ots.batchWriteRow(batchWriteRowRequest);
        }
        throw new UnsupportedOperationException("");
    }

    @Override
    public GetRangeResponse getRange(GetRangeRequest getRangeRequest) throws TableStoreException, ClientException {
        throwException();
        if (ots != null) {
            return ots.getRange(getRangeRequest);
        }
        throw new UnsupportedOperationException("");
    }

    @Override
    public Iterator<Row> createRangeIterator(RangeIteratorParameter rangeIteratorParameter) throws TableStoreException, ClientException {
        throwException();
        if (ots != null) {
            return ots.createRangeIterator(rangeIteratorParameter);
        }
        throw new UnsupportedOperationException("");
    }

    @Override
    public WideColumnIterator createWideColumnIterator(GetRowRequest getRowRequest) throws TableStoreException, ClientException {
        return null;
    }
}
