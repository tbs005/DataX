package com.alibaba.datax.common.exception;

import com.alibaba.datax.common.spi.ErrorCode;

public enum FakeErrorCode implements ErrorCode {

	FAKE_ERROR_CODE_ONLY_FOR_TEST_00("FakeErrorCode-00",
			"only a test, FakeErrorCode."), FAKE_ERROR_CODE_ONLY_FOR_TEST_01(
			"FakeErrorCode-01",
			"only a test, FakeErrorCode，测试中文."),

	;

	private final String code;
	private final String description;

	private FakeErrorCode(String code, String description) {
		this.code = code;
		this.description = description;
	}

	@Override
	public String getCode() {
		return this.code;
	}

	@Override
	public String getDescription() {
		return this.description;
	}

	@Override
	public String toString() {
		return String.format("Code:[%s], Describe:[%s]", this.code,
				this.description);
	}
}
