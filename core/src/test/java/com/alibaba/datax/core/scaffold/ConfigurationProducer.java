package com.alibaba.datax.core.scaffold;

import java.io.File;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.util.ConfigParser;

public final class ConfigurationProducer {

	public static Configuration produce() {
		String path = ConfigurationProducer.class.getClassLoader()
				.getResource(".").getFile();
		return ConfigParser.parse(path + File.separator + "all.json");
	}
}
