package com.alibaba.datax.common.exception;

import org.junit.Assert;
import org.junit.Test;

import com.alibaba.datax.common.spi.ErrorCode;

public class DataXExceptionTest {

	private DataXException dataXException;

	@Test
	public void basicTest() {
		ErrorCode errorCode = FakeErrorCode.FAKE_ERROR_CODE_ONLY_FOR_TEST_00;
		String errorMsg = "basicTest";
		dataXException = DataXException.asDataXException(errorCode, errorMsg);
		Assert.assertEquals(errorCode.toString() + " - " + errorMsg,
				dataXException.getMessage());
	}

	@Test
	public void basicTest_中文() {
		ErrorCode errorCode = FakeErrorCode.FAKE_ERROR_CODE_ONLY_FOR_TEST_01;
		String errorMsg = "basicTest中文";
		dataXException = DataXException.asDataXException(errorCode, errorMsg);
		Assert.assertEquals(errorCode.toString() + " - " + errorMsg,
				dataXException.getMessage());
	}
}
