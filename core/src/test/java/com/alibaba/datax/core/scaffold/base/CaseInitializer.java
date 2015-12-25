package com.alibaba.datax.core.scaffold.base;

import com.alibaba.datax.core.util.container.CoreConstant;
import org.apache.commons.lang.StringUtils;
import org.junit.BeforeClass;

import java.io.File;

public class CaseInitializer {
	@BeforeClass
	public static void beforeClass() {
        CoreConstant.DATAX_HOME = CaseInitializer.class.getClassLoader()
                .getResource(".").getFile();

        CoreConstant.DATAX_CONF_PATH = StringUtils.join(new String[]{
                CoreConstant.DATAX_HOME, "conf", "core.json"}, File.separator);

        CoreConstant.DATAX_CONF_LOG_PATH = StringUtils.join(
                new String[] { CoreConstant.DATAX_HOME, "conf", "logback.xml" }, File.separator);

        CoreConstant.DATAX_PLUGIN_HOME = StringUtils.join(
                new String[] { CoreConstant.DATAX_HOME, "plugin" }, File.separator);

        CoreConstant.DATAX_PLUGIN_READER_HOME = StringUtils.join(
                new String[] { CoreConstant.DATAX_HOME, "plugin", "reader" }, File.separator);

        CoreConstant.DATAX_PLUGIN_WRITER_HOME = StringUtils.join(
                new String[] { CoreConstant.DATAX_HOME, "plugin", "writer" }, File.separator);

        CoreConstant.DATAX_BIN_HOME = StringUtils.join(new String[] {
                CoreConstant.DATAX_HOME, "bin" }, File.separator);

        CoreConstant.DATAX_JOB_HOME = StringUtils.join(new String[] {
                CoreConstant.DATAX_HOME, "job" }, File.separator);

        CoreConstant.DATAX_SECRET_PATH = StringUtils.join(new String[] {
                CoreConstant.DATAX_HOME, "conf", ".secret.properties"}, File.separator);
	}
}
