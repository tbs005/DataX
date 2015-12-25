package com.alibaba.datax.core.util;

import com.alibaba.datax.core.scaffold.base.CaseInitializer;
import org.junit.Assert;
import org.junit.Test;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.AbstractContainer;

public final class ClassUtilTest extends CaseInitializer {
	@Test
	public void test() {

		Assert.assertTrue(ClassUtil.instantiate(
                Dummy.class.getCanonicalName(), Dummy.class) != null);

		Dummy dummy = ClassUtil.instantiate(Dummy.class.getCanonicalName(),
                Dummy.class);
		Assert.assertTrue(dummy instanceof Dummy);

		String dataXServerJson = "{\n" +
				"\t\"core\": {\n" +
				"\t\t\"dataXServer\": {\n" +
				"\t\t\t\"address\": \"http://localhost/test\",\n" +
				"\t\t\t\"timeout\": 5000\n" +
				"\t\t}\n" +
				"\t}\n" +
				"}";
		Assert.assertTrue(ClassUtil.instantiate(
                DummyContainer.class.getCanonicalName(), DummyContainer.class,
                Configuration.from(dataXServerJson)) instanceof DummyContainer);

		Assert.assertTrue(ClassUtil.instantiate(
                DummyContainer.class.getCanonicalName(), DummyContainer.class,
                Configuration.from(dataXServerJson)) instanceof DummyContainer);
	}
}

class DummyContainer extends AbstractContainer {
	public DummyContainer(Configuration configuration) {
		super(configuration);
	}

	@Override
	public void start() {
		System.out.println(getConfiguration());
	}
}

class Dummy {
	public Dummy() {
	}
}
