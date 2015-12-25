package com.alibaba.datax.plugin.writer.otswriter.utils;

import java.util.List;
import java.util.Map.Entry;

import com.alibaba.datax.plugin.writer.otswriter.model.OTSErrorMessage;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSOpType;
import com.alibaba.datax.plugin.writer.otswriter.model.Pair;
import com.aliyun.openservices.ots.model.ColumnValue;
import com.aliyun.openservices.ots.model.PrimaryKeyValue;
import com.aliyun.openservices.ots.model.RowPrimaryKey;

public class SizeCalculateHelper {

    public static int getPrimaryKeyValueSize(PrimaryKeyValue value) {
        
        switch (value.getType()) {
        case INTEGER:
            return 8;
        case STRING:
            return value.asString().length();
        default:
            throw new IllegalArgumentException(String.format(OTSErrorMessage.UNSUPPORT_PARSE, value.getType(), "PrimaryKeyValue"));
        }
    }
    
    public static int getColumnValueSize(ColumnValue value) {
        switch (value.getType()) {
        case BINARY:
            return value.asBinary().length;
        case BOOLEAN:
            return 1;
        case DOUBLE:
            return 8;
        case INTEGER:
            return 8;
        case STRING:
            return value.asString().length();
        default:
            throw new IllegalArgumentException(String.format(OTSErrorMessage.UNSUPPORT_PARSE, value.getType(), "PrimaryKeyValue"));
        }
    }
    
    public static int getRowPrimaryKeySize(RowPrimaryKey pk) {
        int size = 0;
        for (Entry<String, PrimaryKeyValue> en : pk.getPrimaryKey().entrySet()) {
            size += en.getKey().length();
            size += getPrimaryKeyValueSize(en.getValue());
        }
        return size;
    }
    
    public static int getAttributeColumnSize(List<Pair<String, ColumnValue>> attr, OTSOpType op) {
        int size = 0;
        for (Pair<String, ColumnValue> en : attr) {
            if (en.getValue() == null) {
                if (op == OTSOpType.UPDATE_ROW) {
                    size += en.getKey().length();
                }
            } else {
                size += en.getKey().length();
                size += getColumnValueSize(en.getValue());
            }
        }
        return size;
    }
}
