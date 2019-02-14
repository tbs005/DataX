package com.alibaba.datax.plugin.reader.otsstreamreader.internal.common;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.element.Column;
import com.alicloud.openservices.tablestore.model.*;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AssertHelper {

    public static void checkDeleteRow(Record record, PrimaryKey primaryKey, boolean isExportSeq, RecordSequenceInfo sequenceInfo, String shardId) {
        int colNum = primaryKey.getPrimaryKeyColumns().length + 4 + (isExportSeq ? 1 : 0);
        assertEquals(colNum, record.getColumnNumber());
        for (int i = 0; i < colNum; i++) {
            Column column = record.getColumn(i);
            if (i < primaryKey.getPrimaryKeyColumns().length) {
                switch (primaryKey.getPrimaryKeyColumn(i).getValue().getType()) {
                    case INTEGER:
                        assertEquals(primaryKey.getPrimaryKeyColumn(i).getValue().asLong(), (long)column.asLong());
                        break;
                    case STRING:
                        assertEquals(primaryKey.getPrimaryKeyColumn(i).getValue().asString(), column.asString());
                        break;
                    case BINARY:
                        assertTrue(Arrays.equals(primaryKey.getPrimaryKeyColumn(i).getValue().asBinary(), column.asBytes()));
                        break;
                    default:
                        throw new RuntimeException();
                }
            } else {
                int tmp = i - primaryKey.getPrimaryKeyColumns().length;
                switch (tmp) {
                    case 0:
                        assertEquals(null, column.asString());
                        break;
                    case 1:
                        assertEquals(null, column.asLong());
                        break;
                    case 2:
                        assertEquals(null, column.asString());
                        break;
                    case 3:
                        assertEquals("DR", column.asString());
                        break;
                    case 4:
                        String sequenceId = String.format("%010d_%020d_%010d_%s:%010d", sequenceInfo.getEpoch(),
                                sequenceInfo.getTimestamp(), sequenceInfo.getRowIndex(), shardId, 0);
                        assertEquals(sequenceId, column.asString());
                        break;
                    default:
                        throw new RuntimeException();
                }
            }
        }
    }

    public static void checkPut(Record record, PrimaryKey primaryKey, RecordColumn recordColumn, boolean isExportSeq,
                                RecordSequenceInfo sequenceInfo, int idx, String shardId) {
        int colNum = primaryKey.getPrimaryKeyColumns().length + 4 + (isExportSeq ? 1 : 0);
        assertEquals(colNum, record.getColumnNumber());
        for (int i = 0; i < colNum; i++) {
            Column column = record.getColumn(i);
            if (i < primaryKey.getPrimaryKeyColumns().length) {
                switch (primaryKey.getPrimaryKeyColumn(i).getValue().getType()) {
                    case INTEGER:
                        assertEquals(primaryKey.getPrimaryKeyColumn(i).getValue().asLong(), (long)column.asLong());
                        break;
                    case STRING:
                        assertEquals(primaryKey.getPrimaryKeyColumn(i).getValue().asString(), column.asString());
                        break;
                    case BINARY:
                        assertTrue(Arrays.equals(primaryKey.getPrimaryKeyColumn(i).getValue().asBinary(), column.asBytes()));
                        break;
                    default:
                        throw new RuntimeException();
                }
            } else {
                int tmp = i - primaryKey.getPrimaryKeyColumns().length;
                switch (tmp) {
                    case 0:
                        assertEquals(recordColumn.getColumn().getName(), column.asString());
                        break;
                    case 1:
                        assertEquals(recordColumn.getColumn().getTimestamp(), (long)column.asLong());
                        break;
                    case 2:
                        switch (recordColumn.getColumn().getValue().getType()) {
                            case BOOLEAN:
                                assertEquals(recordColumn.getColumn().getValue().asBoolean(), column.asBoolean());
                                break;
                            case DOUBLE:
                                assertEquals(recordColumn.getColumn().getValue().asDouble(), (double)column.asDouble(), 1e-9);
                                break;
                            case INTEGER:
                                assertEquals(recordColumn.getColumn().getValue().asLong(), (long)column.asLong());
                                break;
                            case STRING:
                                assertEquals(recordColumn.getColumn().getValue().asString(), column.asString());
                                break;
                            case BINARY:
                                assertTrue(Arrays.equals(recordColumn.getColumn().getValue().asBinary(), column.asBytes()));
                                break;
                            default:
                                throw new RuntimeException();
                        }
                        break;
                    case 3:
                        assertEquals("U", column.asString());
                        break;
                    case 4:
                        String sequenceId = String.format("%010d_%020d_%010d_%s:%010d", sequenceInfo.getEpoch(),
                                sequenceInfo.getTimestamp(), sequenceInfo.getRowIndex(), shardId, idx);
                        assertEquals(sequenceId, column.asString());
                        break;
                    default:
                        throw new RuntimeException();
                }
            }
        }
    }

    public static void checkDeleteOneVersion(Record record, PrimaryKey primaryKey, RecordColumn recordColumn,
                                             boolean isExportSeq, RecordSequenceInfo sequenceInfo, int idx, String shardId) {
        int colNum = primaryKey.getPrimaryKeyColumns().length + 4 + (isExportSeq ? 1 : 0);
        assertEquals(colNum, record.getColumnNumber());
        for (int i = 0; i < colNum; i++) {
            Column column = record.getColumn(i);
            if (i < primaryKey.getPrimaryKeyColumns().length) {
                switch (primaryKey.getPrimaryKeyColumn(i).getValue().getType()) {
                    case INTEGER:
                        assertEquals(primaryKey.getPrimaryKeyColumn(i).getValue().asLong(), (long)column.asLong());
                        break;
                    case STRING:
                        assertEquals(primaryKey.getPrimaryKeyColumn(i).getValue().asString(), column.asString());
                        break;
                    case BINARY:
                        assertTrue(Arrays.equals(primaryKey.getPrimaryKeyColumn(i).getValue().asBinary(), column.asBytes()));
                        break;
                    default:
                        throw new RuntimeException();
                }
            } else {
                int tmp = i - primaryKey.getPrimaryKeyColumns().length;
                switch (tmp) {
                    case 0:
                        assertEquals(recordColumn.getColumn().getName(), column.asString());
                        break;
                    case 1:
                        assertEquals(recordColumn.getColumn().getTimestamp(), (long)column.asLong());
                        break;
                    case 2:
                        assertEquals(null, column.asString());
                        break;
                    case 3:
                        assertEquals("DO", column.asString());
                        break;
                    case 4:
                        String sequenceId = String.format("%010d_%020d_%010d_%s:%010d", sequenceInfo.getEpoch(),
                                sequenceInfo.getTimestamp(), sequenceInfo.getRowIndex(), shardId, idx);
                        assertEquals(sequenceId, column.asString());
                        break;
                    default:
                        throw new RuntimeException();
                }
            }
        }
    }

    public static void checkDeleteAllVersion(Record record, PrimaryKey primaryKey, RecordColumn recordColumn,
                                             boolean isExportSeq, RecordSequenceInfo sequenceInfo, int idx, String shardId) {
        int colNum = primaryKey.getPrimaryKeyColumns().length + 4 + (isExportSeq ? 1 : 0);
        assertEquals(colNum, record.getColumnNumber());
        for (int i = 0; i < colNum; i++) {
            Column column = record.getColumn(i);
            if (i < primaryKey.getPrimaryKeyColumns().length) {
                switch (primaryKey.getPrimaryKeyColumn(i).getValue().getType()) {
                    case INTEGER:
                        assertEquals(primaryKey.getPrimaryKeyColumn(i).getValue().asLong(), (long)column.asLong());
                        break;
                    case STRING:
                        assertEquals(primaryKey.getPrimaryKeyColumn(i).getValue().asString(), column.asString());
                        break;
                    case BINARY:
                        assertTrue(Arrays.equals(primaryKey.getPrimaryKeyColumn(i).getValue().asBinary(), column.asBytes()));
                        break;
                    default:
                        throw new RuntimeException();
                }
            } else {
                int tmp = i - primaryKey.getPrimaryKeyColumns().length;
                switch (tmp) {
                    case 0:
                        assertEquals(recordColumn.getColumn().getName(), column.asString());
                        break;
                    case 1:
                        assertEquals(null, column.asLong());
                        break;
                    case 2:
                        assertEquals(null, column.asString());
                        break;
                    case 3:
                        assertEquals("DA", column.asString());
                        break;
                    case 4:
                        String sequenceId = String.format("%010d_%020d_%010d_%s:%010d", sequenceInfo.getEpoch(),
                                sequenceInfo.getTimestamp(), sequenceInfo.getRowIndex(), shardId, idx);
                        assertEquals(sequenceId, column.asString());
                        break;
                    default:
                        throw new RuntimeException();
                }
            }
        }
    }

    public static void check(List<Record> records, StreamRecord streamRecord, boolean isExportSeq, String shardId) {
        PrimaryKey primaryKey = streamRecord.getPrimaryKey();
        List<RecordColumn> columns = streamRecord.getColumns();
        StreamRecord.RecordType recordType = streamRecord.getRecordType();
        int idx = 0;
        if (recordType == StreamRecord.RecordType.PUT || recordType == StreamRecord.RecordType.DELETE) {
            checkDeleteRow(records.get(0), primaryKey, isExportSeq, streamRecord.getSequenceInfo(), shardId);
            idx = 1;
        }
        assertEquals(columns.size() + idx, records.size());
        for (RecordColumn recordColumn : columns) {
            switch (recordColumn.getColumnType()) {
                case PUT:
                    checkPut(records.get(idx), primaryKey, recordColumn, isExportSeq, streamRecord.getSequenceInfo(), idx, shardId);
                    break;
                case DELETE_ONE_VERSION:
                    checkDeleteOneVersion(records.get(idx), primaryKey, recordColumn, isExportSeq, streamRecord.getSequenceInfo(), idx, shardId);
                    break;
                case DELETE_ALL_VERSION:
                    checkDeleteAllVersion(records.get(idx), primaryKey, recordColumn, isExportSeq, streamRecord.getSequenceInfo(), idx, shardId);
                    break;
                default:
                    throw new RuntimeException();
            }
            idx++;
        }
    }

    public static void check(List<Record> records, List<StreamRecord> streamRecords, boolean isExportSeq, String shardId) {
        int idx = 0;
        for (StreamRecord streamRecord : streamRecords) {
            int count = streamRecord.getRecordType() != StreamRecord.RecordType.UPDATE ? 1 : 0;
            count += streamRecord.getColumns().size();
            check(records.subList(idx, idx + count), streamRecord, isExportSeq, shardId);
            idx += count;
        }
        assertEquals(idx, records.size());
    }

    private static void sortRecords(List<Record> records) {
        Collections.sort(records, new Comparator<Record>() {
            public int compare(Record record, Record other) {
                String sequenceId = record.getColumn(record.getColumnNumber() - 1).asString();
                String otherSequenceId = other.getColumn(other.getColumnNumber() - 1).asString();
                return sequenceId.compareTo(otherSequenceId);
            }
        });
    }

    private static void sortStreamRecords(final List<Map.Entry<StreamRecord, String>> streamRecordsWithShardId) {
        Collections.sort(streamRecordsWithShardId, new Comparator<Map.Entry<StreamRecord, String>>() {
            public int compare(Map.Entry<StreamRecord, String> streamRecordWithShardId, Map.Entry<StreamRecord, String> other) {
                RecordSequenceInfo sequenceInfo = streamRecordWithShardId.getKey().getSequenceInfo();
                String sequenceId = String.format("%010d_%020d_%010d_%s", sequenceInfo.getEpoch(), sequenceInfo.getTimestamp(),
                        sequenceInfo.getRowIndex(), streamRecordWithShardId.getValue());
                RecordSequenceInfo otherSequenceInfo = other.getKey().getSequenceInfo();
                String otherSequenceId = String.format("%010d_%020d_%010d_%s", otherSequenceInfo.getEpoch(), otherSequenceInfo.getTimestamp(),
                        otherSequenceInfo.getRowIndex(), other.getValue());
                return sequenceId.compareTo(otherSequenceId);
            }
        });
    }

    public static void sortAndCheck(List<Record> records, List<Map.Entry<StreamRecord, String>> streamRecordsWithShardId, boolean isExportSeq) {
        if (!isExportSeq) {
            throw new RuntimeException("Can't sort records when sequenceInfo is not exported.");
        }

        sortRecords(records);
        sortStreamRecords(streamRecordsWithShardId);

        try {
            int idx = 0;
            for (Map.Entry<StreamRecord, String> streamRecordWithShardId : streamRecordsWithShardId) {
                StreamRecord streamRecord = streamRecordWithShardId.getKey();
                String shardId = streamRecordWithShardId.getValue();
                int count = streamRecord.getRecordType() != StreamRecord.RecordType.UPDATE ? 1 : 0;
                count += streamRecord.getColumns().size();
                check(records.subList(idx, idx + count), streamRecord, true, shardId);
                idx += count;
            }
            assertEquals(idx, records.size());
        } catch (RuntimeException ex) {
            throw ex;  // for debug
        }
    }

    public static void checkRecordsNum(List<Record> records, List<Map.Entry<StreamRecord, String>> streamRecordsWithShardId) {
        int count = 0;
        for (Map.Entry<StreamRecord, String> streamRecordWithShardId : streamRecordsWithShardId) {
            StreamRecord streamRecord = streamRecordWithShardId.getKey();
            count += streamRecord.getRecordType() != StreamRecord.RecordType.UPDATE ? 1 : 0;
            count += streamRecord.getColumns().size();
        }
        assertEquals(count, records.size());
    }
}
