package com.alibaba.datax.plugin.writer.otswriter.model;

import java.util.List;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.plugin.writer.otswriter.utils.Common;
import com.alibaba.datax.plugin.writer.otswriter.utils.SizeCalculateHelper;
import com.aliyun.openservices.ots.model.ColumnValue;
import com.aliyun.openservices.ots.model.RowChange;
import com.aliyun.openservices.ots.model.RowPrimaryKey;

public class OTSLine {
    private int dataSize = 0;
    private Record record = null;
    private RowPrimaryKey pk = null;
    private RowChange attr = null;
    
    public OTSLine(String tableName, OTSOpType type, Record record, List<OTSPKColumn> pkColumns, List<OTSAttrColumn> attrColumns) {
        List<Pair<String, ColumnValue>> values = Common.getAttrFromRecord(pkColumns.size(), attrColumns, record);

        this.record = record;
        this.pk = Common.getPKFromRecord(pkColumns, record);
        this.attr = Common.columnValuesToRowChange(tableName, type, pk, values);
        this.dataSize = SizeCalculateHelper.getRowPrimaryKeySize(this.pk) + SizeCalculateHelper.getAttributeColumnSize(values, type);
    }

    public Record getRecord() {
        return record;
    }

    public RowPrimaryKey getPk() {
        return pk;
    }

    public int getDataSize() {
        return dataSize;
    }

    public RowChange getAttr() {
        return attr;
    }
}
