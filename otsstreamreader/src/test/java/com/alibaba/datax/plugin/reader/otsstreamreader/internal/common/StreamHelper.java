package com.alibaba.datax.plugin.reader.otsstreamreader.internal.common;

import com.alicloud.openservices.tablestore.*;
import com.alicloud.openservices.tablestore.model.*;

import java.util.List;

public class StreamHelper {

    public static String getStreamId(SyncClientInterface ots, String tableName) {
        ListStreamResponse listStreamResponse = ots.listStream(new ListStreamRequest(tableName));
        return listStreamResponse.getStreams().get(0).getStreamId();
    }

    public static List<StreamShard> getShardList(SyncClientInterface ots, String tableName) {
        DescribeStreamResponse describeStreamResponse = ots.describeStream(new DescribeStreamRequest(getStreamId(ots, tableName)));
        return describeStreamResponse.getShards();
    }

    public static String getShardIterator(SyncClientInterface ots, String tableName) {
        GetShardIteratorResponse getShardIteratorResponse = ots.getShardIterator(
                new GetShardIteratorRequest(getStreamId(ots, tableName), getShardList(ots, tableName).get(0).getShardId()));
        return getShardIteratorResponse.getShardIterator();
    }

}
