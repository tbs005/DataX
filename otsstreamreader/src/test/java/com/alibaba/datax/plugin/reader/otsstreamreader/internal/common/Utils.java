package com.alibaba.datax.plugin.reader.otsstreamreader.internal.common;

import com.alicloud.openservices.tablestore.model.*;
import org.apache.commons.lang3.RandomStringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class Utils {

    private static final Random random = new Random();

    public static int getRandomInt(int len) {
        return random.nextInt(len);
    }

    public static long getRandomLong(long len) {
        if (len == 0) return 0;
        return Math.abs(random.nextLong()) % len;
    }

    public static boolean getRandomBoolean() {
        return random.nextBoolean();
    }

    public static double getRandomDouble() {
        return getRandomInt(Integer.MAX_VALUE) / (double)(getRandomInt(Integer.MAX_VALUE) + 1);
    }

    public static String getRandomStr(int len) {
        return RandomStringUtils.random(len);
    }

    public static StreamRecord.RecordType getRandomRecordType() {
        switch (getRandomInt(3)) {
            case 0:
                return StreamRecord.RecordType.PUT;
            case 1:
                return StreamRecord.RecordType.UPDATE;
            case 2:
                return StreamRecord.RecordType.DELETE;
            default:
                throw new RuntimeException();
        }
    }

    public static PrimaryKey getPrimaryKey(int pkNum) {
        int maxPkLen = 10;
        List<PrimaryKeyColumn> pkCols = new ArrayList<PrimaryKeyColumn>();
        for (int i = 0; i < pkNum; i++) {
            switch (getRandomInt(3)) {
                case 0:
                    pkCols.add(new PrimaryKeyColumn("pk" + i, PrimaryKeyValue.fromLong(getRandomInt(Integer.MAX_VALUE))));
                    break;
                case 1:
                    pkCols.add(new PrimaryKeyColumn("pk" + i, PrimaryKeyValue.fromString(getRandomStr(getRandomInt(maxPkLen)))));
                    break;
                case 2:
                    pkCols.add(new PrimaryKeyColumn("pk" + i, PrimaryKeyValue.fromBinary(getRandomStr(getRandomInt(maxPkLen)).getBytes())));
                    break;
                default:
                    throw new RuntimeException();
            }
        }
        return new PrimaryKey(pkCols);
    }

    public static ColumnValue getColumnValue() {
        int maxValueLen = 10;
        switch (getRandomInt(5)) {
            case 0:
                return ColumnValue.fromBoolean(getRandomInt(2) == 1);
            case 1:
                return ColumnValue.fromDouble(getRandomInt(Integer.MAX_VALUE) / (double)(getRandomInt(Integer.MAX_VALUE) + 1));
            case 2:
                return ColumnValue.fromLong(getRandomInt(Integer.MAX_VALUE));
            case 3:
                return ColumnValue.fromString(getRandomStr(maxValueLen));
            case 4:
                return ColumnValue.fromBinary(getRandomStr(maxValueLen).getBytes());
            default:
                throw new RuntimeException();
        }
    }

    public static RecordColumn.ColumnType getColumnType() {
        switch (getRandomInt(3)) {
            case 0:
                return RecordColumn.ColumnType.PUT;
            case 1:
                return RecordColumn.ColumnType.DELETE_ONE_VERSION;
            case 2:
                return RecordColumn.ColumnType.DELETE_ALL_VERSION;
            default:
                throw new RuntimeException();
        }
    }

    public static List<RecordColumn> getRecordColumns(int colNum, StreamRecord.RecordType recordType) {
        if (recordType == StreamRecord.RecordType.DELETE) {
            return new ArrayList<RecordColumn>();
        }
        int maxNameLen = 10;
        int maxVersions = 3;
        List<RecordColumn> recordColumns = new ArrayList<RecordColumn>();
        for (int i = 0; i < colNum; i++) {
            String colName = getRandomStr(maxNameLen);
            for (int j = 0; j < getRandomInt(maxVersions); j++) {
                com.alicloud.openservices.tablestore.model.Column column = new com.alicloud.openservices.tablestore.model.Column(colName, getColumnValue(), getRandomInt(1000000000));
                RecordColumn.ColumnType columnType = RecordColumn.ColumnType.PUT;
                if (recordType == StreamRecord.RecordType.UPDATE) {
                    columnType = getColumnType();
                }
                RecordColumn recordColumn = new RecordColumn(column, columnType);
                recordColumns.add(recordColumn);
            }
        }
        return recordColumns;
    }

}
