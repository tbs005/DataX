package com.alibaba.datax.core.transport.record;

import org.junit.Assert;
import org.junit.Test;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.scaffold.RecordProducer;

public class RecordTest {
	@Test
	public void test() {
		Record record = RecordProducer.produceRecord();
		System.out.println(record.toString());

		Configuration configuration = Configuration.from(record.toString());
		Assert.assertTrue(configuration.getInt("size") == 5);
		Assert.assertTrue(configuration.getList("data").size() == 5);
	}
}
