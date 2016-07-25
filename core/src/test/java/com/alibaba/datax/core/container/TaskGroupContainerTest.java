package com.alibaba.datax.core.container;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.faker.*;
import com.alibaba.datax.core.scaffold.base.CaseInitializer;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.CommunicationTool;
import com.alibaba.datax.core.statistics.communication.LocalTGCommunicationManager;
import com.alibaba.datax.core.statistics.container.communicator.AbstractContainerCommunicator;
import com.alibaba.datax.core.taskgroup.TaskGroupContainer;
import com.alibaba.datax.core.transport.transformer.*;
import com.alibaba.datax.core.util.ConfigParser;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.alibaba.datax.core.util.container.LoadUtil;
import com.alibaba.datax.dataxservice.face.domain.enums.State;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

public class TaskGroupContainerTest extends CaseInitializer {
    private Configuration configuration;
    private Configuration configurationFakeTransformer;
    private Configuration configurationRealTransformer;
    private int taskNumber;

    @Before
    public void setUp() {
        String path = TaskGroupContainerTest.class.getClassLoader()
                .getResource(".").getFile();

        this.configuration = ConfigParser.parse(path + File.separator
                + "all0.json");

        LoadUtil.bind(configuration);

        initConfiguration(this.configuration);

        this.configurationFakeTransformer = ConfigParser.parse(path + File.separator
                + "allHasFakeTransformer.json");
        initConfiguration(this.configurationFakeTransformer);
        this.configurationRealTransformer = ConfigParser.parse(path + File.separator
                + "allHasRealTransformer.json");
        initConfiguration(this.configurationRealTransformer);

    }

    private void initConfiguration(Configuration configuration) {

        int channelNumber = 5;
        taskNumber = channelNumber + 3;
        configuration.set(CoreConstant.DATAX_CORE_CONTAINER_JOB_ID, 0);
        configuration.set(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_ID, 1);
        configuration.set(
                CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_SLEEPINTERVAL, 200);
        configuration.set(
                CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_REPORTINTERVAL, 1000);
        configuration.set(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_CHANNEL,
                channelNumber);
        Configuration jobContent = configuration.getListConfiguration(
                CoreConstant.DATAX_JOB_CONTENT).get(0);
        List<Configuration> jobContents = new ArrayList<Configuration>();
        for (int i = 0; i < this.taskNumber; i++) {
            Configuration newJobContent = jobContent.clone();
            newJobContent.set(CoreConstant.TASK_ID, i);
            jobContents.add(newJobContent);
        }
        configuration.set(CoreConstant.DATAX_JOB_CONTENT, jobContents);

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

        Communication res = null;
        try {
            Method com = TaskGroupContainer.class.getDeclaredMethod("reportTaskGroupCommunication", Communication.class, int.class);
            com.setAccessible(true);
            res = (Communication) com.invoke(taskGroupContainer, new Communication(), 1);
            System.out.println("TaskGroup => " + CommunicationTool.Stringify.getSnapshot(res));
        } catch (Exception e) {
            e.printStackTrace();
        }

        Assert.assertTrue(res != null);
        Assert.assertEquals(res.getLongCounter(CommunicationTool.TOTAL_READ_RECORDS).longValue(), 40);
        Assert.assertEquals(res.getLongCounter(CommunicationTool.TRANSFORMER_SUCCEED_RECORDS).longValue(), 0);
        Assert.assertEquals(res.getLongCounter(CommunicationTool.TRANSFORMER_FAILED_RECORDS).longValue(), 0);
        Assert.assertEquals(res.getLongCounter(CommunicationTool.TRANSFORMER_FILTER_RECORDS).longValue(), 0);
        Assert.assertEquals(res.getLongCounter(CommunicationTool.TRANSFORMER_USED_TIME).longValue(), 0);

        System.out.println(res.getMessage());
    }


    @Test
    public void testFakeTransformer() {

        TransformerRegistry.registTransformer(new FakeSubstrTransformer());
        TransformerRegistry.registTransformer(new FakeReplaceTransformer());
        TransformerRegistry.registTransformer(new FakeGroovyTransformer());

        LoadUtil.bind(configurationFakeTransformer);
        this.configurationFakeTransformer.set("plugin.writer.fakewriter.class",
                FakeOneReader.class.getName());
        this.configurationFakeTransformer.set("plugin.writer.fakewriter.class",
                FakeLongTimeWriter.class.getName());
        this.configurationFakeTransformer.set(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_CHANNEL,
                1);
        Configuration jobContent = this.configurationFakeTransformer.getListConfiguration(
                CoreConstant.DATAX_JOB_CONTENT).get(0);
        List<Configuration> jobContents = new ArrayList<Configuration>();
        jobContents.add(jobContent);
        this.configurationFakeTransformer.set(CoreConstant.DATAX_JOB_CONTENT, jobContents);

        TaskGroupContainer taskGroupContainer = new TaskGroupContainer(this.configurationFakeTransformer);
        taskGroupContainer.start();
        Assert.assertTrue(State.SUCCEEDED ==
                taskGroupContainer.getContainerCommunicator().collect().getState());

        Communication res = null;
        try {
            Method com = TaskGroupContainer.class.getDeclaredMethod("reportTaskGroupCommunication", Communication.class, int.class);
            com.setAccessible(true);
            res = (Communication) com.invoke(taskGroupContainer, new Communication(), 1);
            System.out.println("TaskGroup => " + CommunicationTool.Stringify.getSnapshot(res));
        } catch (Exception e) {
            e.printStackTrace();
        }

        Assert.assertTrue(res != null);
        Assert.assertEquals(res.getLongCounter(CommunicationTool.TOTAL_READ_RECORDS).longValue(), 30);
        Assert.assertEquals(res.getLongCounter(CommunicationTool.TRANSFORMER_SUCCEED_RECORDS).longValue(), 10);
        Assert.assertEquals(res.getLongCounter(CommunicationTool.TRANSFORMER_FAILED_RECORDS).longValue(), 0);
        Assert.assertEquals(res.getLongCounter(CommunicationTool.TRANSFORMER_FILTER_RECORDS).longValue(), 10);
    }

    @Test
    public void testRealTransformer() {

        LoadUtil.bind(configurationRealTransformer);
        this.configurationRealTransformer.set("plugin.writer.fakewriter.class",
                FakeOneReader.class.getName());
        this.configurationRealTransformer.set("plugin.writer.fakewriter.class",
                FakeLongTimeWriter.class.getName());
        this.configurationRealTransformer.set(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_CHANNEL,
                1);
        Configuration jobContent = this.configurationRealTransformer.getListConfiguration(
                CoreConstant.DATAX_JOB_CONTENT).get(0);
        List<Configuration> jobContents = new ArrayList<Configuration>();
        jobContents.add(jobContent);
        this.configurationRealTransformer.set(CoreConstant.DATAX_JOB_CONTENT, jobContents);

        TaskGroupContainer taskGroupContainer = new TaskGroupContainer(this.configurationRealTransformer);
        taskGroupContainer.start();
        Assert.assertTrue(State.SUCCEEDED ==
                taskGroupContainer.getContainerCommunicator().collect().getState());

        Communication res = null;
        try {
            Method com = TaskGroupContainer.class.getDeclaredMethod("reportTaskGroupCommunication", Communication.class, int.class);
            com.setAccessible(true);
            res = (Communication) com.invoke(taskGroupContainer, new Communication(), 1);
            System.out.println("TaskGroup => " + CommunicationTool.Stringify.getSnapshot(res));
        } catch (Exception e) {
            e.printStackTrace();
        }

        Assert.assertTrue(res != null);
        Assert.assertEquals(res.getLongCounter(CommunicationTool.TOTAL_READ_RECORDS).longValue(), 30);
        Assert.assertEquals(res.getLongCounter(CommunicationTool.TRANSFORMER_SUCCEED_RECORDS).longValue(), 10);
        Assert.assertEquals(res.getLongCounter(CommunicationTool.TRANSFORMER_FAILED_RECORDS).longValue(), 0);
        Assert.assertEquals(res.getLongCounter(CommunicationTool.TRANSFORMER_FILTER_RECORDS).longValue(), 10);
        Assert.assertTrue(res.getLongCounter(CommunicationTool.TRANSFORMER_USED_TIME).longValue() > 0);

    }

}
