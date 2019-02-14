package com.alibaba.datax.common.element;

import org.junit.Assert;
import org.junit.Test;

import com.alibaba.datax.common.base.BaseTest;
import com.alibaba.datax.common.exception.DataXException;

public class BoolColumnTest extends BaseTest {
	@Test
	public void test_true() {
		BoolColumn bool = new BoolColumn(true);
		Assert.assertTrue(bool.asBoolean().equals(true));
		Assert.assertTrue(bool.asString().equals("true"));
		Assert.assertTrue(bool.asDouble().equals(1.0d));
		Assert.assertTrue(bool.asLong().equals(1L));

		try {
			bool.asDate();
		} catch (Exception e) {
			Assert.assertTrue(e instanceof DataXException);
		}

		try {
			bool.asBytes();
		} catch (Exception e) {
			Assert.assertTrue(e instanceof DataXException);
		}
	}

	@Test
	public void test_false() {
		BoolColumn bool = new BoolColumn(false);
		Assert.assertTrue(bool.asBoolean().equals(false));
		Assert.assertTrue(bool.asString().equals("false"));
		Assert.assertTrue(bool.asDouble().equals(0.0d));
		Assert.assertTrue(bool.asLong().equals(0L));

		try {
			bool.asDate();
		} catch (Exception e) {
			Assert.assertTrue(e instanceof DataXException);
		}

		try {
			bool.asBytes();
		} catch (Exception e) {
			Assert.assertTrue(e instanceof DataXException);
		}
	}

	@Test
	public void test_null() {
		BoolColumn bool = new BoolColumn();
		Assert.assertTrue(bool.asBoolean() == null);
		Assert.assertTrue(bool.asString() == null);
		Assert.assertTrue(bool.asDouble() == null);
		Assert.assertTrue(bool.asLong() == null);

		try {
			bool.asDate();
		} catch (Exception e) {
			Assert.assertTrue(e instanceof DataXException);
		}

		try {
			bool.asBytes();
		} catch (Exception e) {
			Assert.assertTrue(e instanceof DataXException);
		}
	}

	@Test
	public void test_nullReference() {
		Boolean b = null;
		BoolColumn boolColumn = new BoolColumn(b);
		Assert.assertTrue(boolColumn.asBoolean() == null);
	}
}
