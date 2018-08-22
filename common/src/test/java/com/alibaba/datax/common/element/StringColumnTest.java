package com.alibaba.datax.common.element;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;

import org.junit.Assert;
import org.junit.Test;

import com.alibaba.datax.common.base.BaseTest;
import com.alibaba.datax.common.exception.DataXException;

public class StringColumnTest extends BaseTest {

	@Test
	public void test_double() {
		DoubleColumn real = new DoubleColumn("3.14");
		Assert.assertTrue(real.asString().equals("3.14"));
		Assert.assertTrue(real.asDouble().equals(3.14d));
		Assert.assertTrue(real.asLong().equals(3L));

		try {
			real.asBoolean();
			Assert.assertTrue(false);
		} catch (Exception e) {
			Assert.assertTrue(e instanceof DataXException);
		}

		try {
			real.asDate();
			Assert.assertTrue(false);
		} catch (Exception e) {
			Assert.assertTrue(e instanceof DataXException);
		}
	}

	@Test
	public void test_int() {
		LongColumn integer = new LongColumn("3");
		Assert.assertTrue(integer.asString().equals("3"));
		Assert.assertTrue(integer.asDouble().equals(3.0d));
		Assert.assertTrue(integer.asBoolean().equals(true));
		Assert.assertTrue(integer.asLong().equals(3L));
		System.out.println(integer.asDate());
	}

	@Test
	public void test_string() {
		StringColumn string = new StringColumn("bazhen");
		Assert.assertTrue(string.asString().equals("bazhen"));
		try {
			string.asLong();
			Assert.assertTrue(false);

		} catch (Exception e) {
			Assert.assertTrue(e instanceof DataXException);
		}
		try {
			string.asDouble();
			Assert.assertTrue(false);

		} catch (Exception e) {
			Assert.assertTrue(e instanceof DataXException);
		}
		try {
			string.asDate();
			Assert.assertTrue(false);

		} catch (Exception e) {
			Assert.assertTrue(e instanceof DataXException);
		}

		Assert.assertTrue(new String(string.asString().getBytes())
				.equals("bazhen"));
	}

	@Test
	public void test_bool() {
		StringColumn string = new StringColumn("true");
		Assert.assertTrue(string.asString().equals("true"));
		Assert.assertTrue(string.asBoolean().equals(true));

		try {
			string.asDate();
		} catch (Exception e) {
			Assert.assertTrue(e instanceof DataXException);
		}

		try {
			string.asDouble();
		} catch (Exception e) {
			Assert.assertTrue(e instanceof DataXException);
		}

		try {
			string.asLong();
		} catch (Exception e) {
			Assert.assertTrue(e instanceof DataXException);
		}
	}

	@Test
	public void test_null() throws UnsupportedEncodingException {
		StringColumn string = new StringColumn();
		Assert.assertTrue(string.asString() == null);
		Assert.assertTrue(string.asLong() == null);
		Assert.assertTrue(string.asDouble() == null);
		Assert.assertTrue(string.asDate() == null);
		Assert.assertTrue(string.asBytes() == null);
	}

	@Test
	public void test_overflow() {
		StringColumn column = new StringColumn(
				new BigDecimal("1E-1000").toPlainString());

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

		column = new StringColumn(new BigDecimal("1E1000").toPlainString());
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

		column = new StringColumn(new BigDecimal("-1E1000").toPlainString());
		Assert.assertTrue(column.asBigDecimal().compareTo(
				new BigDecimal("-1E1000")) == 0);
		Assert.assertTrue(column.asBigInteger().compareTo(
				new BigDecimal("-1E1000").toBigInteger()) == 0);
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
		StringColumn column = new StringColumn(String.valueOf(Double.NaN));
		Assert.assertTrue(column.asDouble().equals(Double.NaN));
		try {
			column.asBigDecimal();
			Assert.assertTrue(false);
		} catch (Exception e) {
			Assert.assertTrue(true);
		}

		column = new StringColumn(String.valueOf(Double.POSITIVE_INFINITY));
		Assert.assertTrue(column.asDouble().equals(Double.POSITIVE_INFINITY));
		try {
			column.asBigDecimal();
			Assert.assertTrue(false);
		} catch (Exception e) {
			Assert.assertTrue(true);
		}

		column = new StringColumn(String.valueOf(Double.NEGATIVE_INFINITY));
		Assert.assertTrue(column.asDouble().equals(Double.NEGATIVE_INFINITY));
		try {
			column.asBigDecimal();
			Assert.assertTrue(false);
		} catch (Exception e) {
			Assert.assertTrue(true);
		}
	}

    @Test
    public void testEmptyString() {
        StringColumn column = new StringColumn("");
        try {
            BigDecimal num = column.asBigDecimal();
        } catch(Exception e) {
            Assert.assertTrue(e.getMessage().contains("String [\"\"] 不能转为BigDecimal"));
        }

    }
}
