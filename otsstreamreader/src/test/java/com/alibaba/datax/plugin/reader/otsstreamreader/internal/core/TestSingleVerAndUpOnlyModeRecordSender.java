package com.alibaba.datax.plugin.reader.otsstreamreader.internal.core;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.common.RecordSenderForTest;
import com.alicloud.openservices.tablestore.model.*;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestSingleVerAndUpOnlyModeRecordSender {
    @Test
    public void testSingleVerAndUpOnlyModeRecordSender() {
        RecordSenderForTest dataxRs = new RecordSenderForTest();
        List<String> columnNames = Arrays.asList("pk0", "pk1", "ColumnPut1", "ColumnPut2", "ColumnPut3", "ColumnPut4");
        SingleVerAndUpOnlyModeRecordSender recordSender = new SingleVerAndUpOnlyModeRecordSender(dataxRs, "shard_id", true, columnNames);

        {
            StreamRecord record = new StreamRecord();
            record.setRecordType(StreamRecord.RecordType.PUT);
            {
                PrimaryKey primaryKey = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                        .addPrimaryKeyColumn("pk0", PrimaryKeyValue.fromLong(0))
                        .addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("Hello"))
                        .build();
                record.setPrimaryKey(primaryKey);

                List<RecordColumn> columns = new ArrayList<RecordColumn>();
                columns.add(new RecordColumn(new Column("ColumnPut1", ColumnValue.fromLong(100), 1477015075000L), RecordColumn.ColumnType.PUT));
                columns.add(new RecordColumn(new Column("ColumnPut2", ColumnValue.fromString("100"), 1477015075000L), RecordColumn.ColumnType.PUT));
                columns.add(new RecordColumn(new Column("ColumnPut5", ColumnValue.fromString("100"), 1477015075000L), RecordColumn.ColumnType.PUT));
                record.setColumns(columns);
                RecordSequenceInfo sequenceInfo = new RecordSequenceInfo(0, 100001, 0);
                record.setSequenceInfo(sequenceInfo);

                recordSender.sendToDatax(record);
            }
            {
                PrimaryKey primaryKey = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                        .addPrimaryKeyColumn("pk0", PrimaryKeyValue.fromLong(1))
                        .addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("Hello"))
                        .build();
                record.setPrimaryKey(primaryKey);

                List<RecordColumn> columns = new ArrayList<RecordColumn>();
                columns.add(new RecordColumn(new Column("ColumnPut3", ColumnValue.fromBoolean(true), 1477015075000L), RecordColumn.ColumnType.PUT));
                columns.add(new RecordColumn(new Column("ColumnPut4", ColumnValue.fromBinary(new byte[]{1, 0, 0}), 1477015075000L), RecordColumn.ColumnType.PUT));
                columns.add(new RecordColumn(new Column("ColumnPut6", ColumnValue.fromBinary(new byte[]{1, 0, 0}), 1477015075000L), RecordColumn.ColumnType.PUT));
                record.setColumns(columns);
                RecordSequenceInfo sequenceInfo = new RecordSequenceInfo(0, 100002, 1);
                record.setSequenceInfo(sequenceInfo);

                recordSender.sendToDatax(record);
            }

            List<Record> records = dataxRs.getRecords();
            assertEquals(records.size(), 2);

            Record record1 = records.get(0);
            assertEquals(record1.getColumnNumber(), 7);
            assertEquals(record1.getColumn(0).asLong().longValue(), 0);
            assertEquals(record1.getColumn(1).asString(), "Hello");
            assertEquals(record1.getColumn(2).asLong().longValue(), 100);
            assertEquals(record1.getColumn(3).asString(), "100");
            assertNull(record1.getColumn(4).asString());
            assertNull(record1.getColumn(5).asString());
            assertEquals(record1.getColumn(6).asString(), "0000000000_00000000000000100001_0000000000_shard_id");

            Record record2 = records.get(1);
            assertEquals(record2.getColumnNumber(), 7);
            assertEquals(record2.getColumn(0).asLong().longValue(), 1);
            assertEquals(record2.getColumn(1).asString(), "Hello");
            assertNull(record2.getColumn(2).asString());
            assertNull(record2.getColumn(3).asString());
            assertEquals(record2.getColumn(4).asBoolean(), true);
            assertArrayEquals(record2.getColumn(5).asBytes(), new byte[]{1, 0, 0});
            assertEquals(record2.getColumn(6).asString(), "0000000000_00000000000000100002_0000000001_shard_id");

            dataxRs.flush();
        }

        {
            StreamRecord record = new StreamRecord();
            record.setRecordType(StreamRecord.RecordType.UPDATE);
            {
                PrimaryKey primaryKey = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                        .addPrimaryKeyColumn("pk0", PrimaryKeyValue.fromLong(0))
                        .addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("Hello"))
                        .build();
                record.setPrimaryKey(primaryKey);

                List<RecordColumn> columns = new ArrayList<RecordColumn>();
                columns.add(new RecordColumn(new Column("ColumnPut1", ColumnValue.fromLong(100), 1477015075000L), RecordColumn.ColumnType.PUT));
                columns.add(new RecordColumn(new Column("ColumnPut2", ColumnValue.fromString("100"), 1477015075000L), RecordColumn.ColumnType.PUT));

                // delete will be ignored
                columns.add(new RecordColumn(new Column("ColumnPut3", ColumnValue.fromString("100"), 1477015075000L), RecordColumn.ColumnType.DELETE_ONE_VERSION));
                record.setColumns(columns);
                RecordSequenceInfo sequenceInfo = new RecordSequenceInfo(0, 100001, 0);
                record.setSequenceInfo(sequenceInfo);

                recordSender.sendToDatax(record);
            }
            {
                PrimaryKey primaryKey = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                        .addPrimaryKeyColumn("pk0", PrimaryKeyValue.fromLong(1))
                        .addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("Hello"))
                        .build();
                record.setPrimaryKey(primaryKey);

                List<RecordColumn> columns = new ArrayList<RecordColumn>();
                columns.add(new RecordColumn(new Column("ColumnPut4", ColumnValue.fromBoolean(true), 1477015075000L), RecordColumn.ColumnType.PUT));
                columns.add(new RecordColumn(new Column("ColumnPut5", ColumnValue.fromBinary(new byte[]{1, 0, 0}), 1477015075000L), RecordColumn.ColumnType.PUT));
                columns.add(new RecordColumn(new Column("ColumnPut3", ColumnValue.fromString("100"), 1477015075000L), RecordColumn.ColumnType.DELETE_ALL_VERSION));
                record.setColumns(columns);
                RecordSequenceInfo sequenceInfo = new RecordSequenceInfo(0, 100002, 1);
                record.setSequenceInfo(sequenceInfo);

                recordSender.sendToDatax(record);
            }

            List<Record> records = dataxRs.getRecords();

            assertEquals(records.size(), 2);

            Record record1 = records.get(0);
            assertEquals(record1.getColumnNumber(), 7);
            assertEquals(record1.getColumn(0).asLong().longValue(), 0);
            assertEquals(record1.getColumn(1).asString(), "Hello");
            assertEquals(record1.getColumn(2).asLong().longValue(), 100);
            assertEquals(record1.getColumn(3).asString(), "100");
            assertNull(record1.getColumn(4).asString());
            assertNull(record1.getColumn(5).asString());
            assertEquals(record1.getColumn(6).asString(), "0000000000_00000000000000100001_0000000000_shard_id");

            Record record2 = records.get(1);
            assertEquals(record2.getColumnNumber(), 7);
            assertEquals(record2.getColumn(0).asLong().longValue(), 1);
            assertEquals(record2.getColumn(1).asString(), "Hello");
            assertNull(record2.getColumn(2).asString());
            assertNull(record2.getColumn(3).asString());
            assertNull(record2.getColumn(4).asString());
            assertEquals(record2.getColumn(5).asBoolean(), true);
            assertEquals(record2.getColumn(6).asString(), "0000000000_00000000000000100002_0000000001_shard_id");

            dataxRs.flush();
        }

        {
            // no columns is found
            StreamRecord record = new StreamRecord();
            record.setRecordType(StreamRecord.RecordType.UPDATE);
            {
                PrimaryKey primaryKey = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                        .addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromLong(0))
                        .addPrimaryKeyColumn("pk3", PrimaryKeyValue.fromString("Hello"))
                        .build();
                record.setPrimaryKey(primaryKey);

                List<RecordColumn> columns = new ArrayList<RecordColumn>();
                columns.add(new RecordColumn(new Column("ColumnPut5", ColumnValue.fromLong(100), 1477015075000L), RecordColumn.ColumnType.PUT));
                columns.add(new RecordColumn(new Column("ColumnPut6", ColumnValue.fromString("100"), 1477015075000L), RecordColumn.ColumnType.PUT));
                // delete will be ignored
                columns.add(new RecordColumn(new Column("ColumnPut3", ColumnValue.fromString("100"), 1477015075000L), RecordColumn.ColumnType.DELETE_ONE_VERSION));
                record.setColumns(columns);
                RecordSequenceInfo sequenceInfo = new RecordSequenceInfo(0, 100001, 0);
                record.setSequenceInfo(sequenceInfo);

                recordSender.sendToDatax(record);
            }
            List<Record> records = dataxRs.getRecords();
            assertEquals(records.size(), 0);
            dataxRs.flush();
        }

        {
            StreamRecord record = new StreamRecord();
            record.setRecordType(StreamRecord.RecordType.DELETE);
            {
                PrimaryKey primaryKey = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                        .addPrimaryKeyColumn("pk0", PrimaryKeyValue.fromLong(0))
                        .addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("Hello"))
                        .build();
                record.setPrimaryKey(primaryKey);

                RecordSequenceInfo sequenceInfo = new RecordSequenceInfo(0, 100001, 0);
                record.setSequenceInfo(sequenceInfo);

                recordSender.sendToDatax(record);
            }

            List<Record> records = dataxRs.getRecords();

            assertEquals(records.size(), 0);
            dataxRs.flush();
        }
    }
}
