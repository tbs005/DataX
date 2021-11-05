package com.alibaba.datax.core;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.scaffold.base.CaseInitializer;
import com.alibaba.datax.core.util.ConfigParser;
import com.alibaba.datax.core.util.ExceptionTracker;
import com.alibaba.datax.core.util.container.LoadUtil;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;

/**
 * Created by jingxing on 14-9-25.
 */
public class EngineTest extends CaseInitializer {
	private Configuration configuration;

	@Before
	public void setUp() {
		String path = EngineTest.class.getClassLoader().getResource(".")
				.getFile();

		this.configuration = ConfigParser.parse(path + File.separator
				+ "all.json");
		LoadUtil.bind(this.configuration);
	}


	public void test_entry() throws Throwable {
		String jobConfig = this.configuration.toString();

		String jobFile = "./job.json";
		FileWriter writer = new FileWriter(jobFile);
		writer.write(jobConfig);
		writer.flush();
		writer.close();
		String[] args = { "-job", jobFile, "-mode", "standalone" };

		Engine.entry(args);
	}

    @Test
    public void testNN() {
        try {
            throwEE();
        } catch (Exception e) {
            String tarce = ExceptionTracker.trace(e);
            if(e instanceof NullPointerException) {
                System.out.println(tarce);
            }
        }
    }

    public static void throwEE() {
        String aa = null;
        aa.toString();
        //throw new NullPointerException();
    }

}