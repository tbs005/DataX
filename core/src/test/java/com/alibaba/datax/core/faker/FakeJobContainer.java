package com.alibaba.datax.core.faker;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.AbstractContainer;

/**
 * Created by jingxing on 14-9-25.
 */
public class FakeJobContainer extends AbstractContainer {
	public FakeJobContainer(Configuration configuration) {
		super(configuration);
	}

	@Override
	public void start() {
		System.out.println("Fake Job start ..");
	}
}