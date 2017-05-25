package com.aliyun.openservices.ots.internal.model;

import com.alicloud.openservices.tablestore.model.*;

import java.util.ArrayList;
import java.util.List;

public class OTSModelGenerator {

    public ListStreamResponse genListStreamResponse(String tableName, String streamId, long creationTime) {
        ListStreamResponse result = new ListStreamResponse();
        List<Stream> streamList = new ArrayList<Stream>();
        Stream stream = new Stream();
        stream.setTableName(tableName);
        stream.setStreamId(streamId);
        stream.setCreationTime(creationTime);
        streamList.add(stream);
        result.setStreams(streamList);
        return result;
    }

    public DescribeStreamResponse genDescribeStreamResponse(String tableName,
                                                        String streamId,
                                                        long creationTime,
                                                        int expirationTime,
                                                        List<StreamShard> shards) {
        DescribeStreamResponse result = new DescribeStreamResponse();
        result.setTableName(tableName);
        result.setStreamId(streamId);
        result.setCreationTime(creationTime);
        result.setExpirationTime(expirationTime);
        result.setStatus(StreamStatus.ACTIVE);
        result.setShards(shards);
        return result;
    }

    public GetShardIteratorResponse genGetShardIteratorResponse(String shardIterator) {
        GetShardIteratorResponse result = new GetShardIteratorResponse();
        result.setShardIterator(shardIterator);
        return result;
    }

    public GetStreamRecordResponse genGetStreamRecordResponse(List<StreamRecord> streamRecords, String nextIterator) {
        GetStreamRecordResponse result = new GetStreamRecordResponse();
        result.setRecords(streamRecords);
        result.setNextShardIterator(nextIterator);
        return result;
    }

    public DescribeTableResponse genDescribeTableResponse(StreamDetails streamDetails) {
        DescribeTableResponse describeTableResponse = new DescribeTableResponse(new Response());
        describeTableResponse.setStreamDetails(streamDetails);
        return describeTableResponse;
    }
}
