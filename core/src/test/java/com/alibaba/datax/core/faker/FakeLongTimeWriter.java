package com.alibaba.datax.core.faker;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by jingxing on 14/12/24.
 */
public class FakeLongTimeWriter extends Writer {
    public static final class Job extends Writer.Job {

        @Override
        public List<Configuration> split(int readerSlicesNumber) {
            Configuration jobParameter = this.getPluginJobConf();
            System.out.println(jobParameter);

            List<Configuration> splitConfigurationList = new ArrayList<Configuration>();
            Configuration oneConfig = Configuration.newDefault();
            List<String> jdbcUrlArray = new ArrayList<String>();
            jdbcUrlArray.add(String.format("odps://localhost:3305/db%04d", 0));
            oneConfig.set("odpsUrl", jdbcUrlArray);

            List<String> tableArray = new ArrayList<String>();
            tableArray.add(String.format("odps_jingxing_%04d", 0));
            oneConfig.set("table", tableArray);

            splitConfigurationList.add(oneConfig);

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
            Record record = null;
            while ((record = lineReceiver.getFromReader()) != null) {

            }
            for(int i=0; i<2; i++) {
                System.out.println("writer sleep 10s ...");
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
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
