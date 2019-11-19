package com.alibaba.datax.core.faker;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.core.util.FrameworkErrorCode;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by jingxing on 14-9-2.
 */
public class FakeWriter extends Writer {
	public static final class Job extends Writer.Job {

		@Override
		public List<Configuration> split(int readerSlicesNumber) {
			Configuration jobParameter = this.getPluginJobConf();
			System.out.println(jobParameter);

			List<Configuration> splitConfigurationList = new ArrayList<Configuration>();
			for (int i = 0; i < 1024; i++) {
				Configuration oneConfig = Configuration.newDefault();
				List<String> jdbcUrlArray = new ArrayList<String>();
				jdbcUrlArray.add(String.format("odps://localhost:3305/db%04d",
						i));
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

        public void preHandler(Configuration jobConfiguration){
            jobConfiguration.set("job.preHandler.test","writePreDone");
        }

        public void postHandler(Configuration jobConfiguration){
            jobConfiguration.set("job.postHandler.test","writePostDone");
        }
	}

	public static final class Task extends Writer.Task {

		@Override
		public void startWrite(RecordReceiver lineReceiver) {
			Record record = null;

			while ((record = lineReceiver.getFromReader()) != null) {
				this.getTaskPluginCollector().collectDirtyRecord(
						record,
						DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR,
								"TEST"), "TEST");
			}

			for (int i = 0; i < 10; i++) {
				this.getTaskPluginCollector().collectMessage("bazhen-writer",
						"bazhen");
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
