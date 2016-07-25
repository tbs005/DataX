package com.alibaba.datax.core.scaffold;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.core.transport.record.DefaultRecord;

import java.io.UnsupportedEncodingException;

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


	public static Record produceRecordHasNull() {

		try {
			Record record = new DefaultRecord();
			record.addColumn(ColumnProducer.produceLongColumn(1));                  //0
      			record.addColumn(ColumnProducer.produceStringColumn("bazhen"));     //1
			record.addColumn(ColumnProducer.produceBoolColumn(true));               //2
			record.addColumn(ColumnProducer.produceDateColumn(System
					.currentTimeMillis()));                                         //3
			record.addColumn(ColumnProducer.produceBytesColumn("bazhen"
					.getBytes("utf-8")));                                           //4
			record.addColumn(new LongColumn());                                     //5
			record.addColumn(new StringColumn());                                   //6
			record.addColumn(new BoolColumn());                                     //7
			record.addColumn(new DateColumn());                                    //8
			record.addColumn(new BytesColumn());                                    //9
			return record;
		} catch (UnsupportedEncodingException e) {
			throw new IllegalArgumentException(e);
		}
	}
}
