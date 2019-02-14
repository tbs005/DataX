package com.alibaba.datax.common.element;

import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;

public class LongColumnTest {

	@Test
	public void test_null() {
		LongColumn column = new LongColumn();
		System.out.println(column.asString());
		Assert.assertTrue(column.asString() == null);
		System.out.println(column.toString());
		Assert.assertTrue(column.toString().equals(
				"{\"byteSize\":0,\"type\":\"LONG\"}"));
		Assert.assertTrue(column.asBoolean() == null);
		Assert.assertTrue(column.asDouble() == null);
		Assert.assertTrue(column.asString() == null);
		Assert.assertTrue(column.asDate() == null);

		try {
			Assert.assertTrue(column.asBytes() == null);
			Assert.assertTrue(false);
		} catch (Exception e) {
			Assert.assertTrue(true);
		}
	}

	@Test
	public void test_normal() {
		LongColumn column = new LongColumn(1);
		System.out.println(column.asString());
		Assert.assertTrue(column.asString().equals("1"));
		System.out.println(column.toString());
		Assert.assertEquals(column.toString(),
				"{\"byteSize\":8,\"rawData\":1,\"type\":\"LONG\"}");
		Assert.assertTrue(column.asBoolean().equals(true));

		System.out.println(column.asDouble());
		Assert.assertTrue(column.asDouble().equals(1.0d));
		Assert.assertTrue(column.asDate().equals(new Date(1L)));

		try {
			Assert.assertTrue(column.asBytes() == null);
			Assert.assertTrue(false);
		} catch (Exception e) {
			Assert.assertTrue(true);
		}
	}

	@Test
	public void test_max() {
		LongColumn column = new LongColumn(Long.MAX_VALUE);
		System.out.println(column.asString());
		Assert.assertTrue(column.asString().equals(
				String.valueOf(Long.MAX_VALUE)));
		System.out.println(column.toString());
		Assert.assertTrue(column
				.toString()
				.equals(String
						.format("{\"byteSize\":8,\"rawData\":9223372036854775807,\"type\":\"LONG\"}",
								Long.MAX_VALUE)));
		Assert.assertTrue(column.asBoolean().equals(true));

		System.out.println(column.asDouble());
		Assert.assertTrue(column.asDouble().equals((double) Long.MAX_VALUE));
		Assert.assertTrue(column.asDate().equals(new Date(Long.MAX_VALUE)));

		try {
			Assert.assertTrue(column.asBytes() == null);
			Assert.assertTrue(false);
		} catch (Exception e) {
			Assert.assertTrue(true);
		}
	}

	@Test
	public void test_min() {
		LongColumn column = new LongColumn(Long.MIN_VALUE);
		System.out.println(column.asString());
		Assert.assertTrue(column.asString().equals(
				String.valueOf(Long.MIN_VALUE)));
		System.out.println(column.toString());
		Assert.assertTrue(column
				.toString()
				.equals(String
						.format("{\"byteSize\":8,\"rawData\":-9223372036854775808,\"type\":\"LONG\"}",
								Long.MIN_VALUE)));
		Assert.assertTrue(column.asBoolean().equals(true));

		System.out.println(column.asDouble());
		Assert.assertTrue(column.asDouble().equals((double) Long.MIN_VALUE));
		Assert.assertTrue(column.asDate().equals(new Date(Long.MIN_VALUE)));

		try {
			Assert.assertTrue(column.asBytes() == null);
			Assert.assertTrue(false);
		} catch (Exception e) {
			Assert.assertTrue(true);
		}
	}

	@Test
	public void test_string() {
		LongColumn column = new LongColumn(String.valueOf(Long.MIN_VALUE));
		System.out.println(column.asString());
		Assert.assertTrue(column.asString().equals(
				String.valueOf(Long.MIN_VALUE)));
		System.out.println(column.toString());
		Assert.assertTrue(column
				.toString()
				.equals("{\"byteSize\":20,\"rawData\":-9223372036854775808,\"type\":\"LONG\"}"));
		Assert.assertTrue(column.asBoolean().equals(true));

		System.out.println(column.asDouble());
		Assert.assertTrue(column.asDouble().equals((double) Long.MIN_VALUE));
		Assert.assertTrue(column.asDate().equals(new Date(Long.MIN_VALUE)));

		try {
			Assert.assertTrue(column.asBytes() == null);
			Assert.assertTrue(false);
		} catch (Exception e) {
			Assert.assertTrue(true);
		}
	}

	@Test
	public void test_science() {
		LongColumn column = new LongColumn(String.valueOf("4.7E+38"));
		System.out.println(column.asString());
		Assert.assertTrue(column.asString().equals(
				"470000000000000000000000000000000000000"));
		System.out.println(column.toString());
		Assert.assertTrue(column.asBoolean().equals(true));

		System.out.println(">>" + column.asBigDecimal());
		System.out.println(">>" + new BigDecimal("4.7E+38").toPlainString());
		Assert.assertTrue(column.asBigDecimal().toPlainString()
				.equals(new BigDecimal("4.7E+38").toPlainString()));

		try {
			Assert.assertTrue(column.asBytes() == null);
			Assert.assertTrue(false);
		} catch (Exception e) {
			Assert.assertTrue(true);
		}
	}

	@Test
	public void test_bigInteger() {
		LongColumn column = new LongColumn(BigInteger.valueOf(Long.MIN_VALUE));
		System.out.println(column.asString());
		Assert.assertTrue(column.asString().equals(
				String.valueOf(Long.MIN_VALUE)));
		System.out.println(column.toString());
		Assert.assertEquals(column.toString()
				,String.format("{\"byteSize\":8,\"rawData\":-9223372036854775808,\"type\":\"LONG\"}",
								Long.MIN_VALUE));
		Assert.assertTrue(column.asBoolean().equals(true));

		System.out.println(column.asDouble());
		Assert.assertTrue(column.asDouble().equals((double) Long.MIN_VALUE));
		Assert.assertTrue(column.asDate().equals(new Date(Long.MIN_VALUE)));

		try {
			Assert.assertTrue(column.asBytes() == null);
			Assert.assertTrue(false);
		} catch (Exception e) {
			Assert.assertTrue(true);
		}
	}

	@Test
	public void test_overflow() {
		LongColumn column = new LongColumn(String.valueOf(Long.MAX_VALUE)
				+ "000");

		Assert.assertTrue(column.asBoolean().equals(true));
		Assert.assertTrue(column.asBigDecimal().equals(
				new BigDecimal(String.valueOf(Long.MAX_VALUE) + "000")));
		Assert.assertTrue(column.asString().equals(
				String.valueOf(Long.MAX_VALUE) + "000"));
		Assert.assertTrue(column.asBigInteger().equals(
				new BigInteger(String.valueOf(Long.MAX_VALUE) + "000")));

		try {
			column.asLong();
			Assert.assertTrue(false);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.assertTrue(true);
		}

		column = new LongColumn(String.valueOf(Long.MIN_VALUE) + "000");

		Assert.assertTrue(column.asBoolean().equals(true));
		Assert.assertTrue(column.asBigDecimal().equals(
				new BigDecimal(String.valueOf(Long.MIN_VALUE) + "000")));
		Assert.assertTrue(column.asString().equals(
				String.valueOf(Long.MIN_VALUE) + "000"));
		Assert.assertTrue(column.asBigInteger().equals(
				new BigInteger(String.valueOf(Long.MIN_VALUE) + "000")));

		try {
			column.asLong();
			Assert.assertTrue(false);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.assertTrue(true);
		}

	}
}
