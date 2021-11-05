package com.alibaba.datax.plugin.reader.otsstreamreader.internal.functiontest;

import com.alibaba.datax.plugin.reader.otsstreamreader.internal.common.*;
import com.alicloud.openservices.tablestore.*;
import com.alicloud.openservices.tablestore.model.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Semaphore;

public class SingleShardTest {

    private static String tableName = "SingleShardTest_Records";
    private static SyncClientInterface ots = ConfigurationHelper.getOTSFromConfig();
    private static AsyncClientInterface otsAsync = ConfigurationHelper.getOTSAsyncFromConfig();


    public static void main(String[] args) throws Exception {
        SingleShardTest test = new SingleShardTest();
       // test.doRandomOps(1000000);
        ots.shutdown();
        otsAsync.shutdown();
    }

    private void getStreamRecords() {
        GetStreamRecordResponse getStreamRecordResponse = ots.getStreamRecord(new GetStreamRecordRequest(StreamHelper.getShardIterator(ots, tableName)));
        System.out.println(getStreamRecordResponse.getRecords().get(1));
    }

    private void createTable() {
        TableMeta tableMeta = new TableMeta(tableName);
        tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema("Pk1", PrimaryKeyType.STRING));
        tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema("Pk2", PrimaryKeyType.INTEGER));
        tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema("Pk3", PrimaryKeyType.BINARY));
        tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema("Pk4", PrimaryKeyType.STRING));

        TableOptions tableOptions = new TableOptions(-1, 3);
        StreamSpecification streamSpecification = new StreamSpecification(true, 72);
        CreateTableRequest createTableRequest = new CreateTableRequest(tableMeta, tableOptions, new ReservedThroughput(1, 1));
        createTableRequest.setStreamSpecification(streamSpecification);
        ots.createTable(createTableRequest);
    }

    /**
     * 随机操作
     */
    private void doRandomOps(int opNum) throws InterruptedException {
        final Semaphore semaphore = new Semaphore(300);
        for (int i = 0; i < opNum; i++) {
            if ((i % 10000) == 0) {
                System.out.println(i);
            }
            semaphore.acquire();
            int opType = Utils.getRandomInt(4);
            switch (opType) {
                case 0: {
                    PutRowRequest putRowRequest = genPutRowRequest(tableName, 100, Utils.getRandomBoolean());
                    otsAsync.putRow(putRowRequest, new TableStoreCallback<PutRowRequest, PutRowResponse>() {
                        @Override
                        public void onCompleted(PutRowRequest request, PutRowResponse response) {
                            semaphore.release();
                        }

                        @Override
                        public void onFailed(PutRowRequest request, Exception ex) {
                            ex.printStackTrace();
                        }
                    });
                    break;
                }
                case 1: {
                    UpdateRowRequest updateTableRequest = genUpdateRowRequest(tableName, 100, Utils.getRandomBoolean());
                    otsAsync.updateRow(updateTableRequest, new TableStoreCallback<UpdateRowRequest, UpdateRowResponse>() {
                        @Override
                        public void onCompleted(UpdateRowRequest request, UpdateRowResponse response) {
                            semaphore.release();
                        }

                        @Override
                        public void onFailed(UpdateRowRequest request, Exception ex) {
                            ex.printStackTrace();
                        }
                    });
                    break;
                }
                case 2: {
                    DeleteRowRequest deleteRowRequest = genDeleteRowRequest(tableName);
                    otsAsync.deleteRow(deleteRowRequest, new TableStoreCallback<DeleteRowRequest, DeleteRowResponse>() {
                        @Override
                        public void onCompleted(DeleteRowRequest request, DeleteRowResponse response) {
                            semaphore.release();
                        }

                        @Override
                        public void onFailed(DeleteRowRequest request, Exception ex) {
                            ex.printStackTrace();
                        }
                    });
                    break;
                }
                case 3: {
                    BatchWriteRowRequest batchWriteRowRequest = genBatchWriteRowRequest(tableName, 1, 1000, Utils.getRandomBoolean());
                    otsAsync.batchWriteRow(batchWriteRowRequest, new TableStoreCallback<BatchWriteRowRequest, BatchWriteRowResponse>() {
                        @Override
                        public void onCompleted(BatchWriteRowRequest request, BatchWriteRowResponse response) {
                            semaphore.release();
                        }

                        @Override
                        public void onFailed(BatchWriteRowRequest request, Exception ex) {
                            ex.printStackTrace();
                        }
                    });
                }
            }
        }
    }

    /**
     * 随机产生PK
     */
    private PrimaryKey getPrimaryKey() {
        PrimaryKeyColumn primaryKeyColumns[] = new PrimaryKeyColumn[] {null, null, null, null};
        primaryKeyColumns[0] = new PrimaryKeyColumn("Pk1", PrimaryKeyValue.fromString("" + Utils.getRandomInt(10)));
        primaryKeyColumns[1] = new PrimaryKeyColumn("Pk2", PrimaryKeyValue.fromLong(Utils.getRandomInt(10)));
        if (Utils.getRandomBoolean()) {
            primaryKeyColumns[2] = new PrimaryKeyColumn("Pk3", PrimaryKeyValue.fromBinary(Utils.getRandomStr(10).getBytes()));
            primaryKeyColumns[3] = new PrimaryKeyColumn("Pk4", PrimaryKeyValue.fromString(Utils.getRandomStr(10)));
        } else {
            //固定这两列PK，使生成的PK有较大概率重复。
            primaryKeyColumns[2] = new PrimaryKeyColumn("Pk3", PrimaryKeyValue.fromBinary(new byte[0]));
            primaryKeyColumns[3] = new PrimaryKeyColumn("Pk4", PrimaryKeyValue.fromString(""));
        }
        return new PrimaryKey(primaryKeyColumns);
    }

    private List<Column> genRandomColumns(int colValueLength, boolean withTs) {
        List<Column> cols = new ArrayList<Column>();
        int stat = 1 + Utils.getRandomInt(31);
        long ts = System.currentTimeMillis();
        for (int i = 0; i < 5; ++i) {
            int t = stat & (1 << i);
            t = 1;
            if (t > 0) {
                if (withTs) {
                    switch (i) {
                        case 0:
                            cols.add(new Column("COL_STRING",
                                    ColumnValue.fromString(Utils.getRandomStr(colValueLength)), ts));
                            break;
                        case 1:
                            cols.add(new Column("COL_LONG",
                                    ColumnValue.fromLong(Utils.getRandomInt(Integer.MAX_VALUE)), ts));
                            break;
                        case 2:
                            cols.add(new Column("COL_DOUBLE",
                                    ColumnValue.fromDouble(Utils.getRandomDouble()), ts));
                            break;
                        case 3:
                            cols.add(new Column("COL_BOOLEAN",
                                    ColumnValue.fromBoolean(Utils.getRandomBoolean()), ts));
                            break;
                        case 4:
                            cols.add(new Column("COL_BINARY",
                                    ColumnValue.fromBinary(Utils.getRandomStr(colValueLength).getBytes()), ts));
                    }
                } else {
                    switch (i) {
                        case 0:
                            cols.add(new Column("COL_STRING",
                                    ColumnValue.fromString(Utils.getRandomStr(colValueLength))));
                            break;
                        case 1:
                            cols.add(new Column("COL_LONG",
                                    ColumnValue.fromLong(Utils.getRandomInt(Integer.MAX_VALUE))));
                            break;
                        case 2:
                            cols.add(new Column("COL_DOUBLE",
                                    ColumnValue.fromDouble(Utils.getRandomDouble())));
                            break;
                        case 3:
                            cols.add(new Column("COL_BOOLEAN",
                                    ColumnValue.fromBoolean(Utils.getRandomBoolean())));
                            break;
                        case 4:
                            cols.add(new Column("COL_BINARY",
                                    ColumnValue.fromBinary(Utils.getRandomStr(colValueLength).getBytes())));
                    }
                }
            }
        }
        return cols;
    }

    private PutRowRequest genPutRowRequest(String tableName, int colValueLength, boolean withTs) {
        PrimaryKey primaryKey = getPrimaryKey();
        RowPutChange change = new RowPutChange(tableName, primaryKey);
        change.addColumns(genRandomColumns(colValueLength, withTs));
        PutRowRequest request = new PutRowRequest();
        request.setRowChange(change);
        return request;
    }

    private UpdateRowRequest genUpdateRowRequest(String tableName, int colValueLength, boolean withTs) {
        PrimaryKey primaryKey = getPrimaryKey();
        RowUpdateChange change = new RowUpdateChange(tableName, primaryKey);
        Random random = new Random();
        List<Column> columns = genRandomColumns(colValueLength, withTs);
        for (int i = 0; i < columns.size(); ++i) {
            int op = random.nextInt(3);
            switch (op) {
                case 0:
                    change.put(columns.get(i));
                    break;
                case 1:
                    change.deleteColumn(columns.get(i).getName(), System.currentTimeMillis());
                    break;
                case 2:
                    change.deleteColumns(columns.get(i).getName());
            }
        }
        UpdateRowRequest request = new UpdateRowRequest();
        request.setRowChange(change);
        return request;
    }

    private DeleteRowRequest genDeleteRowRequest(String tableName) {
        PrimaryKey primaryKey = getPrimaryKey();
        DeleteRowRequest request = new DeleteRowRequest();
        RowDeleteChange change = new RowDeleteChange(tableName, primaryKey);
        request.setRowChange(change);
        return request;
    }

    private BatchWriteRowRequest genBatchWriteRowRequest(String tableName, int rowNum, int valueLength, boolean withTs) {
        BatchWriteRowRequest request = new BatchWriteRowRequest();
        int opType = Utils.getRandomInt(3);
        switch (opType) {
            case 0: {
                for (int i = 0; i < rowNum; i++) {
                    PutRowRequest putRowRequest = genPutRowRequest(tableName, valueLength, withTs);
                    request.addRowChange(putRowRequest.getRowChange());
                }
                break;
            }
            case 1: {
                for (int i = 0; i < rowNum; i++) {
                    UpdateRowRequest updateRowRequest = genUpdateRowRequest(tableName, valueLength, withTs);
                    request.addRowChange(updateRowRequest.getRowChange());
                }
                break;
            }
            case 2: {
                for (int i = 0; i < rowNum; i++) {
                    DeleteRowRequest deleteRowRequest = genDeleteRowRequest(tableName);
                    request.addRowChange(deleteRowRequest.getRowChange());
                }
            }
        }
        return request;
    }
}
