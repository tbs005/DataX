package com.alibaba.datax.core.scaffold;

import java.io.UnsupportedEncodingException;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.core.transport.record.DefaultRecord;

public class RecordProducer {
	public static Record produceRecord() {

		try {
			Record record = new DefaultRecord();
			record.addColumn(ColumnProducer.produceLongColumn(1));
			record.addColumn(ColumnProducer.produceStringColumn("bazhen"));
			record.addColumn(ColumnProducer.produceBoolColumn(true));
			record.addColumn(ColumnProducer.produceDateColumn(System
					.currentTimeMillis()));
			record.addColumn(ColumnProducer.produceBytesColumn("bazhen"
					.getBytes("utf-8")));
			return record;
		} catch (UnsupportedEncodingException e) {
			throw new IllegalArgumentException(e);
		}
	}
}
