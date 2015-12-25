package com.alibaba.datax.core.faker;

import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by jingxing on 14-9-12.
 */
public class FakeExceptionReader extends Reader {
    public static final class Job extends Reader.Job {
        @Override
        public List<Configuration> split(int adviceNumber) {
            Configuration jobParameter = this.getPluginJobConf();
            System.out.println(jobParameter);

            List<Configuration> splitConfigurationList = new ArrayList<Configuration>();
            for (int i = 0; i < 1024; i++) {
                Configuration oneConfig = Configuration.newDefault();
                List<String> jdbcUrlArray = new ArrayList<String>();
                jdbcUrlArray.add(String.format(
                        "jdbc:mysql://localhost:3305/db%04d", i));
                oneConfig.set("jdbcUrl", jdbcUrlArray);

                List<String> tableArray = new ArrayList<String>();
                tableArray.add(String.format("jingxing_%04d", i));
                oneConfig.set("table", tableArray);

                splitConfigurationList.add(oneConfig);
            }

            return splitConfigurationList;
        }

        @Override
        public void init() {
            System.out.println("fake reader job initialized!");
        }

        @Override
        public void destroy() {
            System.out.println("fake reader job destroyed!");
        }
    }

    public static final class Task extends Reader.Task {
        @Override
        public void startRead(RecordSender lineSender) {
            throw new RuntimeException("just for test");
        }

        @Override
        public void prepare() {
            System.out.println("fake reader task prepared!");
        }

        @Override
        public void post() {
            System.out.println("fake reader task posted!");
        }

        @Override
        public void init() {
            System.out.println("fake reader task initialized!");
        }

        @Override
        public void destroy() {
            System.out.println("fake reader task destroyed!");
        }
    }
}
