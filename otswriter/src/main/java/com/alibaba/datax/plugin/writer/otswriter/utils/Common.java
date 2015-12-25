package com.alibaba.datax.plugin.writer.otswriter.utils;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSAttrColumn;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSErrorMessage;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSLine;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSOpType;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSPKColumn;
import com.alibaba.datax.plugin.writer.otswriter.model.Pair;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSBatchWriterRowTask.LineAndError;
import com.aliyun.openservices.ots.ClientException;
import com.aliyun.openservices.ots.OTSException;
import com.aliyun.openservices.ots.model.ColumnValue;
import com.aliyun.openservices.ots.model.PrimaryKeyValue;
import com.aliyun.openservices.ots.model.RowChange;
import com.aliyun.openservices.ots.model.RowPrimaryKey;
import com.aliyun.openservices.ots.model.RowPutChange;
import com.aliyun.openservices.ots.model.RowUpdateChange;

public class Common {

    public static String getDetailMessage(Exception exception) {
        if (exception instanceof OTSException) {
            OTSException e = (OTSException) exception;
            return "OTSException[ErrorCode:" + e.getErrorCode() + ", ErrorMessage:" + e.getMessage() + ", RequestId:" + e.getRequestId() + "]";
        } else if (exception instanceof ClientException) {
            ClientException e = (ClientException) exception;
            return "ClientException[ErrorCode:" + e.getErrorCode() + ", ErrorMessage:" + e.getMessage() + "]";
        } else if (exception instanceof IllegalArgumentException) {
            IllegalArgumentException e = (IllegalArgumentException) exception;
            return "IllegalArgumentException[ErrorMessage:" + e.getMessage() + "]";
        } else {
            return "Exception[ErrorMessage:" + exception.getMessage() + "]";
        }
    }

    public static RowPrimaryKey getPKFromRecord(List<OTSPKColumn> pkColumns, Record r) {
        RowPrimaryKey primaryKey = new RowPrimaryKey();
        int pkCount = pkColumns.size();
        for (int i = 0; i < pkCount; i++) {
            Column col = r.getColumn(i);
            OTSPKColumn expect = pkColumns.get(i);

            if (col.getRawData() == null) {
                throw new IllegalArgumentException(String.format(OTSErrorMessage.PK_COLUMN_VALUE_IS_NULL_ERROR, expect.getName()));
            }

            PrimaryKeyValue pk = ColumnConversion.columnToPrimaryKeyValue(col, expect);
            primaryKey.addPrimaryKeyColumn(expect.getName(), pk);
        }
        return primaryKey;
    }

    public static List<Pair<String, ColumnValue>> getAttrFromRecord(int pkCount, List<OTSAttrColumn> attrColumns, Record r) {
        List<Pair<String, ColumnValue>> attr = new ArrayList<Pair<String, ColumnValue>>(r.getColumnNumber());
        for (int i = 0; i < attrColumns.size(); i++) {
            Column col = r.getColumn(i + pkCount);
            OTSAttrColumn expect = attrColumns.get(i);

            if (col.getRawData() == null) {
                attr.add(new Pair<String, ColumnValue>(expect.getName(), null));
                continue;
            }

            ColumnValue cv = ColumnConversion.columnToColumnValue(col, expect);
            attr.add(new Pair<String, ColumnValue>(expect.getName(), cv));
        }
        return attr;
    }

    public static RowChange columnValuesToRowChange(String tableName, OTSOpType type, RowPrimaryKey pk, List<Pair<String, ColumnValue>> values) {
        switch (type) {
            case PUT_ROW:
                RowPutChange rowPutChange = new RowPutChange(tableName);
                rowPutChange.setPrimaryKey(pk);

                for (Pair<String, ColumnValue> en : values) {
                    if (en.getValue() != null) {
                        rowPutChange.addAttributeColumn(en.getKey(), en.getValue());
                    } 
                }

                return rowPutChange;
            case UPDATE_ROW:
                RowUpdateChange rowUpdateChange = new RowUpdateChange(tableName);
                rowUpdateChange.setPrimaryKey(pk);

                for (Pair<String, ColumnValue> en : values) {
                    if (en.getValue() != null) {
                        rowUpdateChange.addAttributeColumn(en.getKey(), en.getValue());
                    } else {
                        rowUpdateChange.deleteAttributeColumn(en.getKey());
                    }
                }
                return rowUpdateChange;
            default:
                throw new IllegalArgumentException(String.format(OTSErrorMessage.UNSUPPORT_PARSE, type, "RowChange"));
        }
    }

    public static long getDelaySendMillinSeconds(int hadRetryTimes, int initSleepInMilliSecond) {

        if (hadRetryTimes <= 0) {
            return 0;
        }

        int sleepTime = initSleepInMilliSecond;
        for (int i = 1; i < hadRetryTimes; i++) {
            sleepTime += sleepTime;
            if (sleepTime > 30000) {
                sleepTime = 30000;
                break;
            } 
        }
        return sleepTime;
    }

    public static void collectDirtyRecord(TaskPluginCollector collector, List<LineAndError> errors) {
        for (LineAndError re : errors) {
            collector.collectDirtyRecord(re.getLine().getRecord(), re.getError().getMessage());
        }
    }

    public static void collectDirtyRecord(TaskPluginCollector collector, List<OTSLine> lines, String errorMsg) {
        for (OTSLine l : lines) {
            collector.collectDirtyRecord(l.getRecord(), errorMsg);
        }
    }
}
