package com.alibaba.datax.common.base;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.BeforeClass;

import com.alibaba.datax.common.element.ColumnCast;
import com.alibaba.datax.common.element.ColumnCastTest;
import com.alibaba.datax.common.util.Configuration;
import org.junit.Test;

public class BaseTest {

	@BeforeClass
	public static void beforeClass() throws IOException {
		String path = ColumnCastTest.class.getClassLoader().getResource(".")
				.getFile();
		ColumnCast.bind(Configuration.from(FileUtils.readFileToString(new File(
				StringUtils.join(new String[] { path, "all.json" },
						File.separator)))));
	}

    @Test
    public void emptyTest() {
        
    }
}
