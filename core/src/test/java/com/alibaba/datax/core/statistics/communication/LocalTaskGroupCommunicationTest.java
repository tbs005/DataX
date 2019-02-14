package com.alibaba.datax.core.statistics.communication;

import com.alibaba.datax.dataxservice.face.domain.enums.State;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class LocalTaskGroupCommunicationTest {
    private final int taskGroupNumber = 5;

    @Before
    public void setUp() {
        LocalTGCommunicationManager.clear();
        for (int index = 0; index < taskGroupNumber; index++) {
            LocalTGCommunicationManager.registerTaskGroupCommunication(
                    index, new Communication());
        }
    }

    @Test
    public void LocalCommunicationTest() {
        Communication jobCommunication =
                LocalTGCommunicationManager.getJobCommunication();
        Assert.assertTrue(jobCommunication.getState().equals(State.RUNNING));

        for (int index : LocalTGCommunicationManager.getTaskGroupIdSet()) {
            Communication communication = LocalTGCommunicationManager
                    .getTaskGroupCommunication(index);
            communication.setState(State.SUCCEEDED);
            LocalTGCommunicationManager.updateTaskGroupCommunication(
                    index, communication);
        }

        jobCommunication = LocalTGCommunicationManager.getJobCommunication();
        Assert.assertTrue(jobCommunication.getState().equals(State.SUCCEEDED));
    }

    @Test(expected = IllegalArgumentException.class)
    public void noTaskGroupIdForUpdate() {
        LocalTGCommunicationManager.updateTaskGroupCommunication(
                this.taskGroupNumber + 1, new Communication());
    }

    @Test(expected = IllegalArgumentException.class)
    public void noTaskGroupIdForGet() {
        LocalTGCommunicationManager.getTaskGroupCommunication(-1);
    }
}
