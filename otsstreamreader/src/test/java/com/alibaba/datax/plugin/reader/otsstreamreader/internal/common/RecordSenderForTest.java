package com.alibaba.datax.plugin.reader.otsstreamreader.internal.common;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.RecordSender;

import java.util.ArrayList;
import java.util.List;

public class RecordSenderForTest implements RecordSender{

    private List<Record> records = new ArrayList<Record>();

    public Record createRecord() {
        return new Record() {
            private List<Column> columns = new ArrayList<Column>();

            public void addColumn(Column column) {
                columns.add(column);
            }

            public void setColumn(int i, Column column) {
                columns.add(i, column);
            }

            public Column getColumn(int i) {
                return columns.get(i);
            }

            public int getColumnNumber() {
                return columns.size();
            }

            public int getByteSize() {
                return 0;
            }

            public int getMemorySize() {
                return 0;
            }
        };
    }

    public void sendToWriter(Record record) {
        synchronized (records) {
            records.add(record);
        }
    }

    public List<Record> getRecords() {
        return records;
    }

    public void flush() {
        records.clear();
    }

    public void terminate() {

    }

    public void shutdown() {

    }

}
