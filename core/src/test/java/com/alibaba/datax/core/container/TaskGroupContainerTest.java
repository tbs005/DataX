package com.alibaba.datax.core.container;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.faker.FakeExceptionReader;
import com.alibaba.datax.core.faker.FakeExceptionWriter;
import com.alibaba.datax.core.faker.FakeLongTimeWriter;
import com.alibaba.datax.core.faker.FakeOneReader;
import com.alibaba.datax.core.job.meta.State;
import com.alibaba.datax.core.scaffold.base.CaseInitializer;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.LocalTGCommunicationManager;
import com.alibaba.datax.core.statistics.container.communicator.AbstractContainerCommunicator;
import com.alibaba.datax.core.taskgroup.TaskGroupContainer;
import com.alibaba.datax.core.util.ConfigParser;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.alibaba.datax.core.util.container.LoadUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class TaskGroupContainerTest extends CaseInitializer {
    private Configuration configuration;
    private int taskNumber;

    @Before
    public void setUp() {
        String path = TaskGroupContainerTest.class.getClassLoader()
                .getResource(".").getFile();

        this.configuration = ConfigParser.parse(path + File.separator
                + "all.json");
        LoadUtil.bind(this.configuration);

        int channelNumber = 5;
        taskNumber = channelNumber + 3;
        this.configuration.set(CoreConstant.DATAX_CORE_CONTAINER_JOB_ID, 0);
        this.configuration.set(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_ID, 1);
        this.configuration.set(
                CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_SLEEPINTERVAL, 200);
        this.configuration.set(
                CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_REPORTINTERVAL, 1000);
        this.configuration.set(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_CHANNEL,
                channelNumber);
        Configuration jobContent = this.configuration.getListConfiguration(
                CoreConstant.DATAX_JOB_CONTENT).get(0);
        List<Configuration> jobContents = new ArrayList<Configuration>();
        for (int i = 0; i < this.taskNumber; i++) {
            Configuration newJobContent = jobContent.clone();
            newJobContent.set(CoreConstant.TASK_ID, i);
            jobContents.add(newJobContent);
        }
        this.configuration.set(CoreConstant.DATAX_JOB_CONTENT, jobContents);

        LocalTGCommunicationManager.clear();
        LocalTGCommunicationManager.registerTaskGroupCommunication(
                1, new Communication());

    }

    @Test
    public void testStart() throws InterruptedException {
        TaskGroupContainer taskGroupContainer = new TaskGroupContainer(this.configuration);
        taskGroupContainer.start();

        AbstractContainerCommunicator collector = taskGroupContainer.getContainerCommunicator();
        while (true) {
            State totalTaskState = collector.collectState();
            if (totalTaskState.isRunning()) {
                Thread.sleep(1000);
            } else {
                break;
            }
        }

        Communication totalTaskCommunication = collector.collect();
        List<String> messages = totalTaskCommunication.getMessage("bazhen-reader");
        Assert.assertTrue(!messages.isEmpty());

        messages = totalTaskCommunication.getMessage("bazhen-writer");
        Assert.assertTrue(!messages.isEmpty());

        messages = totalTaskCommunication.getMessage("bazhen");
        Assert.assertNull(messages);

        State state = totalTaskCommunication.getState();

        Assert.assertTrue("task finished", state.equals(State.SUCCEEDED));
    }

    @Test(expected = RuntimeException.class)
    public void testReaderException() {
        this.configuration.set("plugin.reader.fakereader.class",
                FakeExceptionReader.class.getCanonicalName());
        TaskGroupContainer taskGroupContainer = new TaskGroupContainer(this.configuration);
        taskGroupContainer.start();
    }

    @Test(expected = RuntimeException.class)
    public void testWriterException() {
        this.configuration.set("plugin.writer.fakewriter.class",
                FakeExceptionWriter.class.getName());
        TaskGroupContainer taskGroupContainer = new TaskGroupContainer(this.configuration);
        taskGroupContainer.start();
    }

    @Test
    public void testLongTimeWriter() {
        this.configuration.set("plugin.writer.fakewriter.class",
                FakeOneReader.class.getName());
        this.configuration.set("plugin.writer.fakewriter.class",
                FakeLongTimeWriter.class.getName());
        this.configuration.set(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_CHANNEL,
                1);
        Configuration jobContent = this.configuration.getListConfiguration(
                CoreConstant.DATAX_JOB_CONTENT).get(0);
        List<Configuration> jobContents = new ArrayList<Configuration>();
        jobContents.add(jobContent);
        this.configuration.set(CoreConstant.DATAX_JOB_CONTENT, jobContents);

        TaskGroupContainer taskGroupContainer = new TaskGroupContainer(this.configuration);
        taskGroupContainer.start();
        Assert.assertTrue(State.SUCCEEDED ==
                taskGroupContainer.getContainerCommunicator().collect().getState());
    }
}
