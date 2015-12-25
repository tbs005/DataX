package com.alibaba.datax.core.container;

import com.alibaba.datax.common.plugin.AbstractJobPlugin;
import com.alibaba.datax.common.plugin.AbstractTaskPlugin;
import org.junit.Assert;
import org.junit.Test;

import com.alibaba.datax.common.constant.PluginType;
import com.alibaba.datax.core.util.container.LoadUtil;
import com.alibaba.datax.core.scaffold.ConfigurationProducer;
import com.alibaba.datax.core.scaffold.base.CaseInitializer;
import com.alibaba.fastjson.JSON;

public class LoadUtilTest extends CaseInitializer {

	@Test
	public void test() {
		LoadUtil.bind(ConfigurationProducer.produce());
		AbstractJobPlugin jobPlugin = LoadUtil.loadJobPlugin(
                PluginType.READER, "fakereader");
		System.out.println(JSON.toJSONString(jobPlugin));
		Assert.assertTrue(jobPlugin.getPluginName().equals("fakereader"));

		AbstractTaskPlugin taskPlugin = LoadUtil.loadTaskPlugin(
                PluginType.READER, "fakereader");
		System.out.println(JSON.toJSONString(taskPlugin));
		Assert.assertTrue(taskPlugin.getPluginName().equals("fakereader"));

	}

}
