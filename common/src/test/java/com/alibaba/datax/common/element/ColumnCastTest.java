package com.alibaba.datax.common.element;

import com.alibaba.datax.common.util.Configuration;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.sql.Date;
import java.sql.Time;
import java.text.ParseException;

public class ColumnCastTest {
	private Configuration produce() throws IOException {
		String path = ColumnCastTest.class.getClassLoader().getResource(".")
				.getFile();
		String content = FileUtils.readFileToString(new File(StringUtils.join(
				new String[] { path, "all.json" }, File.separator)));
		return Configuration.from(content);
	}

	@Test
	public void test_string() throws IOException, ParseException {
		Configuration configuration = this.produce();
		StringCast.init(configuration);

		System.out.println(StringCast.asDate(new StringColumn("2014-09-18")));
		Assert.assertTrue(StringCast.asDate(new StringColumn("2014-09-18"))
				.getTime() == 1410969600000L);

		Assert.assertTrue(StringCast.asDate(new StringColumn("20140918"))
				.getTime() == 1410969600000L);

		Assert.assertTrue(StringCast.asDate(new StringColumn("08:00:00"))
				.getTime() == 0L);

		Assert.assertTrue(StringCast.asDate(
				new StringColumn("2014-09-18 16:00:00")).getTime() == 1411027200000L);
		configuration
				.set("common.column.datetimeFormat", "yyyy/MM/dd HH:mm:ss");
		StringCast.init(configuration);
		Assert.assertTrue(StringCast.asDate(
				new StringColumn("2014/09/18 16:00:00")).getTime() == 1411027200000L);

		configuration.set("common.column.timeZone", "GMT");
		StringCast.init(configuration);

		java.util.Date date = StringCast.asDate(new StringColumn(
				"2014/09/18 16:00:00"));
		System.out.println(DateFormatUtils.format(date, "yyyy/MM/dd HH:mm:ss"));
		Assert.assertTrue("2014/09/19 00:00:00".equals(DateFormatUtils.format(
				date, "yyyy/MM/dd HH:mm:ss")));

	}

	@Test
	public void test_date() throws IOException {
		Assert.assertTrue(DateCast.asString(
				new DateColumn(System.currentTimeMillis())).startsWith("201"));

		Configuration configuration = this.produce();
		configuration
				.set("common.column.datetimeFormat", "MM/dd/yyyy HH:mm:ss");
		DateCast.init(configuration);
		System.out.println(DateCast.asString(new DateColumn(System
				.currentTimeMillis())));
		Assert.assertTrue(!DateCast.asString(
				new DateColumn(System.currentTimeMillis())).startsWith("2014"));

		DateColumn dateColumn = new DateColumn(new Time(0L));
		System.out.println(dateColumn.asString());
		Assert.assertTrue(dateColumn.asString().equals("08:00:00"));

		configuration.set("common.column.timeZone", "GMT");
		DateCast.init(configuration);
		System.err.println(DateCast.asString(dateColumn));
		Assert.assertTrue(dateColumn.asString().equals("00:00:00"));

		configuration.set("common.column.timeZone", "GMT+8");
		DateCast.init(configuration);
		System.out.println(dateColumn.asString());
		Assert.assertTrue(dateColumn.asString().equals("08:00:00"));

		dateColumn = new DateColumn(new Date(0L));
		System.out.println(dateColumn.asString());
		Assert.assertTrue(dateColumn.asString().equals("1970-01-01"));

		dateColumn = new DateColumn(new java.util.Date(0L));
		System.out.println(dateColumn.asString());
		Assert.assertTrue(dateColumn.asString().equals("01/01/1970 08:00:00"));
	}
}