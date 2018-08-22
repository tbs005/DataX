package com.alibaba.datax.common.element;

import org.apache.commons.lang3.math.NumberUtils;
import org.junit.Test;

public class ScientificTester {
	@Test
	public void test() {
		System.out.println(NumberUtils.createBigDecimal("10E+6").toBigInteger().toString());
		System.err.println((String) null);
	}
}
