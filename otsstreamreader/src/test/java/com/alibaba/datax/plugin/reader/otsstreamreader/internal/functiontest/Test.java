package com.alibaba.datax.plugin.reader.otsstreamreader.internal.functiontest;

import com.alibaba.datax.plugin.reader.otsstreamreader.internal.common.ConfigurationHelper;
import com.alicloud.openservices.tablestore.*;
import com.alicloud.openservices.tablestore.model.*;

import java.util.Arrays;
import java.util.List;

public class Test {

    private static SyncClientInterface ots = ConfigurationHelper.getOTSFromConfig();
    private static String tableName = "dataTable";

    private void deleteTable() {
        ots.deleteTable(new DeleteTableRequest(tableName));
    }

    private void createTable() {
        TableMeta tableMeta = new TableMeta(tableName);
        tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema("pk", PrimaryKeyType.STRING));
        TableOptions tableOptions = new TableOptions(-1, 3);
        CreateTableRequest createTableRequest = new CreateTableRequest(tableMeta, tableOptions, new ReservedThroughput(1, 1));
        createTableRequest.setStreamSpecification(new StreamSpecification(true, 24 * 3));
        ots.createTable(createTableRequest);
    }

    private void prepareData() {
        for (int i = 0; i < 100; i++) {
            PrimaryKey primaryKey = new PrimaryKey(Arrays.asList(new PrimaryKeyColumn("pk", PrimaryKeyValue.fromString("" + i))));
            RowPutChange rowPutChange = new RowPutChange(tableName, primaryKey);
            rowPutChange.addColumn("col" + i, ColumnValue.fromLong(i));
            ots.putRow(new PutRowRequest(rowPutChange));
        }
    }

    private String getStreamId() {
        ListStreamResponse listStreamResponse = ots.listStream(new ListStreamRequest(tableName));
        return listStreamResponse.getStreams().get(0).getStreamId();
    }

    private List<StreamShard> getShardList() {
        DescribeStreamResponse describeStreamResponse = ots.describeStream(new DescribeStreamRequest(getStreamId()));
        return describeStreamResponse.getShards();
    }

    private String getShardIterator() {
        GetShardIteratorResponse getShardIteratorResponse = ots.getShardIterator(
                new GetShardIteratorRequest(getStreamId(), getShardList().get(0).getShardId()));
        return getShardIteratorResponse.getShardIterator();
    }

    private void getStreamRecords() {
        GetStreamRecordResponse getStreamRecordResponse = ots.getStreamRecord(
                new GetStreamRecordRequest(getShardIterator()));
        System.out.println(getStreamRecordResponse.getRecords().size());
        System.out.println(getStreamRecordResponse.getRecords());
    }

    private void listTable() {
        ListTableResponse result = ots.listTable();
        System.out.println(result.getTableNames());
    }

    private void test() {
      //  deleteTable();
      //  createTable();
        prepareData();
        getStreamRecords();

      /*
        DescribeTableResponse describeTableResponse = ots.describeTable(new DescribeTableRequest(tableName));
        System.out.println(describeTableResponse.getStreamDetails().getExpirationTime());
        System.out.println(describeTableResponse.getStreamDetails().getStreamId());
        */
     //   prepareData();
     //   getStreamRecords();
    }

    public static void main(String[] args) {
        Test test = new Test();
        test.test();
        ots.shutdown();
    }

}
