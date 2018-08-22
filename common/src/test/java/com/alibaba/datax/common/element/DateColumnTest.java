package com.alibaba.datax.common.element;

import com.alibaba.datax.common.base.BaseTest;
import com.alibaba.datax.common.exception.DataXException;
import org.junit.Assert;
import org.junit.Test;

import java.util.Date;

public class DateColumnTest extends BaseTest  {
	@Test
	public void test() {
		long time = System.currentTimeMillis();
		DateColumn date = new DateColumn(time);
		Assert.assertTrue(date.getType().equals(Column.Type.DATE));
		Assert.assertTrue(date.asDate().getTime() == time);
		Assert.assertTrue(date.asLong().equals(time));
		System.out.println(date.asString());
		Assert.assertTrue(date.asString().startsWith("201"));

		try {
			date.asBytes();
		} catch (Exception e) {
			Assert.assertTrue(e instanceof DataXException);
		}

		try {
			date.asDouble();
		} catch (Exception e) {
			Assert.assertTrue(e instanceof DataXException);
		}
	}

	@Test
	public void test_null() {
		DateColumn date = new DateColumn();
		DateColumn nul1 = new DateColumn((Long)null);
		DateColumn nul2 = new DateColumn((Date)null);
		DateColumn nul3 = new DateColumn((java.sql.Date)null);
		DateColumn nul4 = new DateColumn((java.sql.Time)null);
		DateColumn nul5 = new DateColumn((java.sql.Timestamp)null);
		Assert.assertTrue(date.getType().equals(Column.Type.DATE));

		Assert.assertTrue(date.asDate() == null);
		Assert.assertTrue(date.asLong() == null);
		Assert.assertTrue(date.asString() == null);

		Assert.assertTrue(nul1.asDate() == null);
		Assert.assertTrue(nul1.asLong() == null);
		Assert.assertTrue(nul1.asString() == null);

		Assert.assertTrue(nul2.asDate() == null);
		Assert.assertTrue(nul2.asLong() == null);
		Assert.assertTrue(nul2.asString() == null);

		Assert.assertTrue(nul3.asDate() == null);
		Assert.assertTrue(nul3.asLong() == null);
		Assert.assertTrue(nul3.asString() == null);

		Assert.assertTrue(nul4.asDate() == null);
		Assert.assertTrue(nul4.asLong() == null);
		Assert.assertTrue(nul4.asString() == null);

		Assert.assertTrue(nul5.asDate() == null);
		Assert.assertTrue(nul5.asLong() == null);
		Assert.assertTrue(nul5.asString() == null);

		try {
			date.asBytes();
		} catch (Exception e) {
			Assert.assertTrue(e instanceof DataXException);
		}

		try {
			date.asDouble();
		} catch (Exception e) {
			Assert.assertTrue(e instanceof DataXException);
		}

		try {
			date.asBoolean();
		} catch (Exception e) {
			Assert.assertTrue(e instanceof DataXException);
		}

	}

	@Test
	public void testDataColumn() throws Exception {
		DateColumn date = new DateColumn(1449925250000L);
		Assert.assertEquals(date.asString(),"2015-12-12 21:00:50");
		Assert.assertEquals(date.asDate(),new Date(1449925250000L));

		java.sql.Date dat2 = new java.sql.Date(1449925251001L);
		date = new DateColumn(dat2);
		Assert.assertEquals(date.asString(),"2015-12-12");
		Assert.assertEquals(date.asDate(),new Date(1449925251001L));

		java.sql.Time dat3 = new java.sql.Time(1449925252002L);
		date = new DateColumn(dat3);
		Assert.assertEquals(date.asString(),"21:00:52");
		Assert.assertEquals(date.asDate(),new Date(1449925252002L));

		java.sql.Timestamp ts = new java.sql.Timestamp(1449925253003L);
		date = new DateColumn(ts);
		Assert.assertEquals(date.asString(),"2015-12-12 21:00:53");
		Assert.assertEquals(date.asDate(),new Date(1449925253003L));
	}
}
