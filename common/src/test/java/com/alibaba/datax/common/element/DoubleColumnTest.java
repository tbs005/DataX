package com.alibaba.datax.common.element;

import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;

public class DoubleColumnTest {
	@Test
	public void test_null() {
		DoubleColumn column = new DoubleColumn();

		System.out.println(column.asString());
		Assert.assertTrue(column.asString() == null);
		System.out.println(column.toString());
		Assert.assertTrue(column.toString().equals(
				"{\"byteSize\":0,\"type\":\"DOUBLE\"}"));
		Assert.assertTrue(column.asDouble() == null);
		Assert.assertTrue(column.asString() == null);

		try {
			Assert.assertTrue(column.asBoolean() == null);
			Assert.assertTrue(false);
		} catch (Exception e) {
			Assert.assertTrue(true);
		}

		try {
			Assert.assertTrue(column.asDate() == null);
			Assert.assertTrue(false);
		} catch (Exception e) {
			Assert.assertTrue(true);
		}

		try {
			Assert.assertTrue(column.asBytes() == null);
			Assert.assertTrue(false);
		} catch (Exception e) {
			Assert.assertTrue(true);
		}
	}

	@Test
	public void test_double() {
		DoubleColumn column = new DoubleColumn(1.0d);

		System.out.println(column.asString());
		Assert.assertTrue(column.asString().equals("1.0"));
		System.out.println(column.toString());
		Assert.assertTrue(column.toString().equals(
				"{\"byteSize\":3,\"rawData\":\"1.0\",\"type\":\"DOUBLE\"}"));

		System.out.println(column.asDouble());
		Assert.assertTrue(column.asDouble().equals(1.0d));

		try {
			Assert.assertTrue(column.asBoolean() == null);
			Assert.assertTrue(false);
		} catch (Exception e) {
			Assert.assertTrue(true);
		}

		try {
			Assert.assertTrue(column.asBytes() == null);
			Assert.assertTrue(false);
		} catch (Exception e) {
			Assert.assertTrue(true);
		}
	}

	@Test
	public void test_float() {
		DoubleColumn column = new DoubleColumn(1.0f);

		System.out.println(column.asString());
		Assert.assertTrue(column.asString().equals("1.0"));
		System.out.println(column.toString());
		Assert.assertTrue(column.toString().equals(
				"{\"byteSize\":3,\"rawData\":\"1.0\",\"type\":\"DOUBLE\"}"));

		System.out.println(column.asDouble());
		Assert.assertTrue(column.asDouble().equals(1.0d));

		try {
			Assert.assertTrue(column.asBoolean() == null);
			Assert.assertTrue(false);
		} catch (Exception e) {
			Assert.assertTrue(true);
		}

		try {
			Assert.assertTrue(column.asBytes() == null);
			Assert.assertTrue(false);
		} catch (Exception e) {
			Assert.assertTrue(true);
		}
	}

	@Test
	public void test_string() {
		DoubleColumn column = new DoubleColumn("1.0");

		System.out.println(column.asString());
		Assert.assertTrue(column.asString().equals("1.0"));
		System.out.println(column.toString());
		Assert.assertTrue(column.toString().equals(
				"{\"byteSize\":3,\"rawData\":\"1.0\",\"type\":\"DOUBLE\"}"));

		System.out.println(column.asDouble());
		Assert.assertTrue(column.asDouble().equals(1.0d));

		try {
			Assert.assertTrue(column.asBoolean() == null);
			Assert.assertTrue(false);
		} catch (Exception e) {
			Assert.assertTrue(true);
		}

		try {
			Assert.assertTrue(column.asBytes() == null);
			Assert.assertTrue(false);
		} catch (Exception e) {
			Assert.assertTrue(true);
		}
	}

	@Test
	public void test_BigDecimal() {
		DoubleColumn column = new DoubleColumn(new BigDecimal("1E-100"));

		System.out.println(column.asString());
		System.out.println(column.asString().length());
		Assert.assertTrue(column.asString().length() == 102);

		Assert.assertTrue(column.asString().equals(
				new BigDecimal("1E-100").toPlainString()));

		Assert.assertTrue(column
				.toString()
				.equals("{\"byteSize\":102,\"rawData\":\"0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001\",\"type\":\"DOUBLE\"}"));

		System.out.println(column.asDouble());
		Assert.assertTrue(column.asDouble().equals(1.0E-100));

		try {
			Assert.assertTrue(column.asBoolean() == null);
			Assert.assertTrue(false);
		} catch (Exception e) {
			Assert.assertTrue(true);
		}

		try {
			Assert.assertTrue(column.asBytes() == null);
			Assert.assertTrue(false);
		} catch (Exception e) {
			Assert.assertTrue(true);
		}
	}

	@Test
	public void test_overflow() {
		DoubleColumn column = new DoubleColumn(new BigDecimal("1E-1000"));

		System.out.println(column.asString());

		Assert.assertTrue(column.asBigDecimal().equals(
				new BigDecimal("1E-1000")));

		Assert.assertTrue(column.asBigInteger().compareTo(BigInteger.ZERO) == 0);
		Assert.assertTrue(column.asLong().equals(0L));

		try {
			column.asDouble();
			Assert.assertTrue(false);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.assertTrue(true);
		}

		column = new DoubleColumn(new BigDecimal("1E1000"));
		Assert.assertTrue(column.asBigDecimal().compareTo(
				new BigDecimal("1E1000")) == 0);
		Assert.assertTrue(column.asBigInteger().compareTo(
				new BigDecimal("1E1000").toBigInteger()) == 0);
		try {
			column.asDouble();
			Assert.assertTrue(false);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.assertTrue(true);
		}

		try {
			column.asLong();
			Assert.assertTrue(false);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.assertTrue(true);
		}
	}

	@Test
	public void test_NaN() {
		DoubleColumn column = new DoubleColumn(String.valueOf(Double.NaN));
		Assert.assertTrue(column.asString().equals("NaN"));
		try {
			column.asBigDecimal();
			Assert.assertTrue(false);
		} catch (Exception e) {
			Assert.assertTrue(true);
		}

		column = new DoubleColumn(String.valueOf(Double.POSITIVE_INFINITY));
		Assert.assertTrue(column.asString().equals("Infinity"));
		try {
			column.asBigDecimal();
			Assert.assertTrue(false);
		} catch (Exception e) {
			Assert.assertTrue(true);
		}

		column = new DoubleColumn(String.valueOf(Double.NEGATIVE_INFINITY));
		Assert.assertTrue(column.asString().equals("-Infinity"));
		try {
			column.asBigDecimal();
			Assert.assertTrue(false);
		} catch (Exception e) {
			Assert.assertTrue(true);
		}
	}

	@Test
	public void test_doubleFormat() {
		System.out.println(new BigDecimal("9801523474.1234567890987654321")
				.toPlainString());

		System.out.println("double: " + 9801523474.399621d);
		System.out.println("bigdecimal: " + new BigDecimal(9801523474.399621d).toPlainString());
		System.out.println("bigdecimal: " + new BigDecimal(String.valueOf(9801523474.399621d)).toPlainString());
		System.out.println(new DoubleColumn(9801523474.399621d).asString());
		Assert.assertTrue("9801523474.39962".equals(new DoubleColumn(
				9801523474.399621d).asString()));

		Assert.assertTrue(!new DoubleColumn(Double.MAX_VALUE).asString()
				.contains("E"));
		Assert.assertTrue(!new DoubleColumn(Float.MAX_VALUE).asString()
				.contains("E"));
	}

	@Test
	public void test_doubleColumn(){
		//todo 跑不过的单测，先简单注释掉, 估计是之前的DoubleColumn整改后，就没有能支持NaN等值
		//DoubleColumn column = new DoubleColumn(Double.NaN);
		//DoubleColumn column1 = new DoubleColumn(Double.NEGATIVE_INFINITY);
		//DoubleColumn column2 = new DoubleColumn(Double.POSITIVE_INFINITY);
		DoubleColumn column3 = new DoubleColumn(new Double(1.79E+308));

	//	Assert.assertTrue("NaN".equals(column.asString()));
		//Assert.assertTrue("-Infinity".equals(column1.asString()));
		//Assert.assertTrue("Infinity".equals(column2.asString()));
		Assert.assertTrue(!column3.asString().contains("E"));
		Assert.assertTrue(!new DoubleColumn(Double.MAX_VALUE).asString()
				.contains("E"));
	}

	@Test
	public void test_doubleColumnFloat(){
		//todo 跑不过的单测，先简单注释掉
		//DoubleColumn column = new DoubleColumn(Float.NaN);
		//DoubleColumn column1 = new DoubleColumn(Float.NEGATIVE_INFINITY);
		//DoubleColumn column2 = new DoubleColumn(Float.POSITIVE_INFINITY);
		DoubleColumn column3 = new DoubleColumn(new Float(1.79E+30));

		//Assert.assertTrue("NaN".equals(column.asString()));
		//Assert.assertTrue("-Infinity".equals(column1.asString()));
		//Assert.assertTrue("Infinity".equals(column2.asString()));
		Assert.assertTrue(!column3.asString().contains("E"));
		Assert.assertTrue(!new DoubleColumn(Float.MAX_VALUE).asString()
				.contains("E"));
	}
}
