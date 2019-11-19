package com.alibaba.datax.plugin.reader.otsstreamreader.internal.core;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.common.RecordSenderForTest;
import com.alicloud.openservices.tablestore.model.*;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestMultiVerModeRecordSender {
    @Test
    public void testMultiVerRecordSender() {
        RecordSenderForTest dataxRs = new RecordSenderForTest();
        MultiVerModeRecordSender recordSender = new MultiVerModeRecordSender(dataxRs, "shard_id", true);

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
                record.setColumns(columns);
                RecordSequenceInfo sequenceInfo = new RecordSequenceInfo(0, 100002, 1);
                record.setSequenceInfo(sequenceInfo);

                recordSender.sendToDatax(record);
            }

            List<Record> records = dataxRs.getRecords();
            assertEquals(records.size(), 6);

            Record record1 = records.get(0);
            assertEquals(record1.getColumnNumber(), 7);
            assertEquals(record1.getColumn(0).asLong().longValue(), 0);
            assertEquals(record1.getColumn(1).asString(), "Hello");
            assertNull(record1.getColumn(2).asString());
            assertNull(record1.getColumn(3).asString());
            assertNull(record1.getColumn(4).asString());
            assertEquals(record1.getColumn(5).asString(), "DR");
            assertEquals(record1.getColumn(6).asString(), "0000000000_00000000000000100001_0000000000_shard_id:0000000000");

            Record record2 = records.get(1);
            assertEquals(record2.getColumnNumber(), 7);
            assertEquals(record2.getColumn(0).asLong().longValue(), 0);
            assertEquals(record2.getColumn(1).asString(), "Hello");
            assertEquals(record2.getColumn(2).asString(), "ColumnPut1");
            assertEquals(record2.getColumn(3).asLong().longValue(), 1477015075000L);
            assertEquals(record2.getColumn(4).asLong().longValue(), 100);
            assertEquals(record2.getColumn(5).asString(), "U");
            assertEquals(record2.getColumn(6).asString(), "0000000000_00000000000000100001_0000000000_shard_id:0000000001");

            Record record3 = records.get(2);
            assertEquals(record3.getColumnNumber(), 7);
            assertEquals(record3.getColumn(0).asLong().longValue(), 0);
            assertEquals(record3.getColumn(1).asString(), "Hello");
            assertEquals(record3.getColumn(2).asString(), "ColumnPut2");
            assertEquals(record3.getColumn(3).asLong().longValue(), 1477015075000L);
            assertEquals(record3.getColumn(4).asString(), "100");
            assertEquals(record3.getColumn(5).asString(), "U");
            assertEquals(record3.getColumn(6).asString(), "0000000000_00000000000000100001_0000000000_shard_id:0000000002");

            Record record4 = records.get(3);
            assertEquals(record4.getColumnNumber(), 7);
            assertEquals(record4.getColumn(0).asLong().longValue(), 1);
            assertEquals(record4.getColumn(1).asString(), "Hello");
            assertNull(record4.getColumn(2).asString());
            assertNull(record4.getColumn(3).asString());
            assertNull(record4.getColumn(4).asString());
            assertEquals(record4.getColumn(5).asString(), "DR");
            assertEquals(record4.getColumn(6).asString(), "0000000000_00000000000000100002_0000000001_shard_id:0000000000");

            Record record5 = records.get(4);
            assertEquals(record5.getColumnNumber(), 7);
            assertEquals(record5.getColumn(0).asLong().longValue(), 1);
            assertEquals(record5.getColumn(1).asString(), "Hello");
            assertEquals(record5.getColumn(2).asString(), "ColumnPut3");
            assertEquals(record5.getColumn(3).asLong().longValue(), 1477015075000L);
            assertEquals(record5.getColumn(4).asBoolean(), true);
            assertEquals(record5.getColumn(5).asString(), "U");
            assertEquals(record5.getColumn(6).asString(), "0000000000_00000000000000100002_0000000001_shard_id:0000000001");

            Record record6 = records.get(5);
            assertEquals(record6.getColumnNumber(), 7);
            assertEquals(record6.getColumn(0).asLong().longValue(), 1);
            assertEquals(record6.getColumn(1).asString(), "Hello");
            assertEquals(record6.getColumn(2).asString(), "ColumnPut4");
            assertEquals(record6.getColumn(3).asLong().longValue(), 1477015075000L);
            assertArrayEquals(record6.getColumn(4).asBytes(), new byte[]{1, 0, 0});
            assertEquals(record6.getColumn(5).asString(), "U");
            assertEquals(record6.getColumn(6).asString(), "0000000000_00000000000000100002_0000000001_shard_id:0000000002");

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
                columns.add(new RecordColumn(new Column("ColumnPut6", ColumnValue.fromString("100"), 1477015075000L), RecordColumn.ColumnType.DELETE_ALL_VERSION));
                record.setColumns(columns);
                RecordSequenceInfo sequenceInfo = new RecordSequenceInfo(0, 100002, 1);
                record.setSequenceInfo(sequenceInfo);

                recordSender.sendToDatax(record);
            }

            List<Record> records = dataxRs.getRecords();

            assertEquals(records.size(), 6);
            Record record1 = records.get(0);
            assertEquals(record1.getColumnNumber(), 7);
            assertEquals(record1.getColumn(0).asLong().longValue(), 0);
            assertEquals(record1.getColumn(1).asString(), "Hello");
            assertEquals(record1.getColumn(2).asString(), "ColumnPut1");
            assertEquals(record1.getColumn(3).asLong().longValue(), 1477015075000L);
            assertEquals(record1.getColumn(4).asLong().longValue(), 100);
            assertEquals(record1.getColumn(5).asString(), "U");
            assertEquals(record1.getColumn(6).asString(), "0000000000_00000000000000100001_0000000000_shard_id:0000000000");

            Record record2 = records.get(1);
            assertEquals(record2.getColumnNumber(), 7);
            assertEquals(record2.getColumn(0).asLong().longValue(), 0);
            assertEquals(record2.getColumn(1).asString(), "Hello");
            assertEquals(record2.getColumn(2).asString(), "ColumnPut2");
            assertEquals(record2.getColumn(3).asLong().longValue(), 1477015075000L);
            assertEquals(record2.getColumn(4).asLong().longValue(), 100);
            assertEquals(record2.getColumn(5).asString(), "U");
            assertEquals(record2.getColumn(6).asString(), "0000000000_00000000000000100001_0000000000_shard_id:0000000001");

            Record record3 = records.get(2);
            assertEquals(record3.getColumnNumber(), 7);
            assertEquals(record3.getColumn(0).asLong().longValue(), 0);
            assertEquals(record3.getColumn(1).asString(), "Hello");
            assertEquals(record3.getColumn(2).asString(), "ColumnPut3");
            assertEquals(record3.getColumn(3).asLong().longValue(), 1477015075000L);
            assertNull(record3.getColumn(4).asString());
            assertEquals(record3.getColumn(5).asString(), "DO");
            assertEquals(record3.getColumn(6).asString(), "0000000000_00000000000000100001_0000000000_shard_id:0000000002");

            Record record4 = records.get(3);
            assertEquals(record4.getColumnNumber(), 7);
            assertEquals(record4.getColumn(0).asLong().longValue(), 1);
            assertEquals(record4.getColumn(1).asString(), "Hello");
            assertEquals(record4.getColumn(2).asString(), "ColumnPut4");
            assertEquals(record4.getColumn(3).asLong().longValue(), 1477015075000L);
            assertEquals(record4.getColumn(4).asBoolean(), true);
            assertEquals(record4.getColumn(5).asString(), "U");
            assertEquals(record4.getColumn(6).asString(), "0000000000_00000000000000100002_0000000001_shard_id:0000000000");

            Record record5 = records.get(4);
            assertEquals(record5.getColumnNumber(), 7);
            assertEquals(record5.getColumn(0).asLong().longValue(), 1);
            assertEquals(record5.getColumn(1).asString(), "Hello");
            assertEquals(record5.getColumn(2).asString(), "ColumnPut5");
            assertEquals(record5.getColumn(3).asLong().longValue(), 1477015075000L);
            assertArrayEquals(record5.getColumn(4).asBytes(), new byte[]{1, 0, 0});
            assertEquals(record5.getColumn(5).asString(), "U");
            assertEquals(record5.getColumn(6).asString(), "0000000000_00000000000000100002_0000000001_shard_id:0000000001");

            Record record6 = records.get(5);
            assertEquals(record6.getColumnNumber(), 7);
            assertEquals(record6.getColumn(0).asLong().longValue(), 1);
            assertEquals(record6.getColumn(1).asString(), "Hello");
            assertEquals(record6.getColumn(2).asString(), "ColumnPut6");
            assertNull(record6.getColumn(3).asLong());
            assertNull(record6.getColumn(4).asString());
            assertEquals(record6.getColumn(5).asString(), "DA");
            assertEquals(record6.getColumn(6).asString(), "0000000000_00000000000000100002_0000000001_shard_id:0000000002");

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

            assertEquals(records.size(), 1);
            Record record1 = records.get(0);
            assertEquals(record1.getColumnNumber(), 7);
            assertEquals(record1.getColumn(0).asLong().longValue(), 0);
            assertEquals(record1.getColumn(1).asString(), "Hello");
            assertNull(record1.getColumn(2).asString());
            assertNull(record1.getColumn(3).asLong());
            assertNull(record1.getColumn(4).asLong());
            assertEquals(record1.getColumn(5).asString(), "DR");
            assertEquals(record1.getColumn(6).asString(), "0000000000_00000000000000100001_0000000000_shard_id:0000000000");

            dataxRs.flush();
        }
    }
}
