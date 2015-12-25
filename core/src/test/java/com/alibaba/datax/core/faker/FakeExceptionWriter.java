package com.alibaba.datax.core.faker;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;

/**
 * Created by jingxing on 14-9-12.
 */
public class FakeExceptionWriter extends Writer {
    public static final class Job extends Writer.Job {

        @Override
        public List<Configuration> split(int readerSlicesNumber) {
            Configuration jobParameter = this.getPluginJobConf();
            System.out.println(jobParameter);

            List<Configuration> splitConfigurationList = new ArrayList<Configuration>();
            for(int i=0; i<1024; i++) {
                Configuration oneConfig = Configuration.newDefault();
                List<String> jdbcUrlArray = new ArrayList<String>();
                jdbcUrlArray.add(String.format("odps://localhost:3305/db%04d", i));
                oneConfig.set("odpsUrl", jdbcUrlArray);

                List<String> tableArray = new ArrayList<String>();
                tableArray.add(String.format("odps_jingxing_%04d", i));
                oneConfig.set("table", tableArray);

                splitConfigurationList.add(oneConfig);
            }

            return splitConfigurationList;
        }

        @Override
        public void init() {
            System.out.println("fake writer job initialized!");
        }

        @Override
        public void destroy() {
            System.out.println("fake writer job destroyed!");
        }
    }

    public static final class Task extends Writer.Task {

        @Override
        public void startWrite(RecordReceiver lineReceiver) {
            throw new RuntimeException("just for test");
        }

        @Override
        public void prepare() {
            System.out.println("fake writer task prepared!");
        }

        @Override
        public void post() {
            System.out.println("fake writer task posted!");
        }

        @Override
        public void init() {
            System.out.println("fake writer task initialized!");
        }

        @Override
        public void destroy() {
            System.out.println("fake writer task destroyed!");
        }
    }
}
