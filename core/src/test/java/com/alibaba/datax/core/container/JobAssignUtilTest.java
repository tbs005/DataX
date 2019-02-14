package com.alibaba.datax.core.container;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.container.util.JobAssignUtil;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import java.io.File;
import java.util.List;

public class JobAssignUtilTest {
    @Test
    public void test_01() {
        Configuration configuration = Configuration.from(new File(JobAssignUtil.class.getResource("/job/job.json").getFile()));
        configuration.set("job.content[0].taskId", 0);
        configuration.set("job.content[1].taskId", 1);
        System.out.println(configuration.beautify());
        int channelNumber = 3;
        int channelsPerTaskGroup = 1;
        List<Configuration> result = JobAssignUtil.assignFairly(configuration, channelNumber, channelsPerTaskGroup);

        System.out.println("===================================");
        for (Configuration conf : result) {
            System.out.println(conf.beautify());
            System.out.println("----------------");
        }

        System.out.println(configuration);
    }

    @Test
    public void test_02() {

        String jobString = "{\"job\":{\"setting\":{},\"content\":[{\"reader\":{\"type\":\"fakereader\",\"parameter\":{}},\"writer\":{\"type\":\"fakewriter\",\"parameter\":{}}},{\"reader\":{\"type\":\"fakereader\",\"parameter\":{}},\"writer\":{\"type\":\"fakewriter\",\"parameter\":{}}},{\"reader\":{\"type\":\"fakereader\",\"parameter\":{}},\"writer\":{\"type\":\"fakewriter\",\"parameter\":{}}},{\"reader\":{\"type\":\"fakereader\",\"parameter\":{}},\"writer\":{\"type\":\"fakewriter\",\"parameter\":{}}},{\"reader\":{\"type\":\"fakereader\",\"parameter\":{}},\"writer\":{\"type\":\"fakewriter\",\"parameter\":{}}},{\"reader\":{\"type\":\"fakereader\",\"parameter\":{}},\"writer\":{\"type\":\"fakewriter\",\"parameter\":{}}},{\"reader\":{\"type\":\"fakereader\",\"parameter\":{}},\"writer\":{\"type\":\"fakewriter\",\"parameter\":{}}},{\"reader\":{\"type\":\"fakereader\",\"parameter\":{}},\"writer\":{\"type\":\"fakewriter\",\"parameter\":{}}},{\"reader\":{\"type\":\"fakereader\",\"parameter\":{}},\"writer\":{\"type\":\"fakewriter\",\"parameter\":{}}},{\"reader\":{\"type\":\"fakereader\",\"parameter\":{}},\"writer\":{\"type\":\"fakewriter\",\"parameter\":{}}},{\"reader\":{\"type\":\"fakereader\",\"parameter\":{}},\"writer\":{\"type\":\"fakewriter\",\"parameter\":{}}},{\"reader\":{\"type\":\"fakereader\",\"parameter\":{}},\"writer\":{\"type\":\"fakewriter\",\"parameter\":{}}},{\"reader\":{\"type\":\"fakereader\",\"parameter\":{}},\"writer\":{\"type\":\"fakewriter\",\"parameter\":{}}}]}}";

        Configuration configuration = Configuration.from(jobString);

        int taskNumber = StringUtils.countMatches(jobString, "fakereader");
        System.out.println("taskNumber:" + taskNumber);
        for (int i = 0; i < taskNumber; i++) {
            configuration.set("job.content[" + i + "].taskId", i);
        }
//        System.out.println(configuration.beautify());
        int channelNumber = 13;
        int channelsPerTaskGroup = 5;
        List<Configuration> result = JobAssignUtil.assignFairly(configuration, channelNumber, channelsPerTaskGroup);

        System.out.println("===================================");
        for (Configuration conf : result) {
            System.out.println(conf.beautify());
            System.out.println("----------------");
        }

//        System.out.println(configuration);
    }
}
