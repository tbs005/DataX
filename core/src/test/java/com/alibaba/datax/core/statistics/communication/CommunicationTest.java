package com.alibaba.datax.core.statistics.communication;

import com.alibaba.datax.core.job.meta.State;
import org.junit.Assert;
import org.junit.Test;

public class CommunicationTest {
    @Test
    public void commonTest() {
        Communication comm = new Communication();

        String longKey = "long";
        Assert.assertEquals(0, (long) comm.getLongCounter(longKey));
        long longValue = 100;
        comm.setLongCounter(longKey, longValue);
        String doubleKey = "double";
        Assert.assertEquals(0.0, comm.getDoubleCounter(doubleKey), 0.01);
        double doubleValue = 0.01d;
        comm.setDoubleCounter(doubleKey, doubleValue);
        comm.setState(State.SUCCEEDED);
        comm.setThrowable(new RuntimeException("runtime exception"));
        long now = System.currentTimeMillis();
        comm.setTimestamp(now);

        Assert.assertEquals(longValue, (long) comm.getLongCounter("long"));
        Assert.assertEquals(doubleValue, comm.getDoubleCounter("double"), 0.01);
        Assert.assertTrue(State.SUCCEEDED.equals(comm.getState()));
        Assert.assertTrue(comm.getThrowable() instanceof RuntimeException);
        Assert.assertEquals(now, comm.getTimestamp());

        comm.reset();
        Assert.assertTrue(State.RUNNING.equals(comm.getState()));
        Assert.assertTrue(comm.getTimestamp() >= now);

        long delta = 5;
        comm.increaseCounter(longKey, delta);
        Assert.assertEquals(delta, (long) comm.getLongCounter(longKey));

        String messageKey = "message";
        comm.addMessage(messageKey, "message1");
        comm.addMessage(messageKey, "message2");
        Assert.assertEquals(2, comm.getMessage(messageKey).size());
    }

    @Test
    public void setStateTest() {
        Communication comm = new Communication();
        Assert.assertTrue(State.RUNNING.equals(comm.getState()));

        comm.setState(State.SUCCEEDED);
        Assert.assertTrue(State.SUCCEEDED.equals(comm.getState()));

        comm.setState(State.FAILED);
        Assert.assertTrue(State.FAILED.equals(comm.getState()));

        comm.setState(State.SUCCEEDED);
        Assert.assertTrue(State.FAILED.equals(comm.getState()));
    }

    @Test
    public void cloneTest() {
        Communication comm0 = new Communication();
        String longKey = "long";
        long longValue = 5;
        long timestamp = comm0.getTimestamp();
        comm0.setLongCounter(longKey, longValue);
        comm0.setState(State.SUCCEEDED);

        Communication comm1 = comm0.clone();

        Assert.assertEquals(longValue, (long) comm1.getLongCounter(longKey));
        Assert.assertEquals(timestamp, comm1.getTimestamp());
        Assert.assertTrue(comm0.getState().equals(comm1.getState()));
    }

    @Test
    public void mergeTest() {
        Communication comm1 = new Communication();
        comm1.setLongCounter("long", 5);
        comm1.setDoubleCounter("double", 5.1);
        comm1.setState(State.SUCCEEDED);
        comm1.setThrowable(new RuntimeException("run exception"));
        comm1.addMessage("message", "message1");

        Communication comm2 = new Communication();
        comm2.setLongCounter("long", 5);
        comm2.setDoubleCounter("double", 5.1);
        comm2.setState(State.FAILED);
        comm2.setThrowable(new IllegalArgumentException(""));
        comm2.addMessage("message", "message2");

        Communication comm = comm1.mergeFrom(comm2);
        Assert.assertEquals(10, (long) comm.getLongCounter("long"));
        Assert.assertEquals(10.2, comm.getDoubleCounter("double"), 0.01);
        Assert.assertTrue(State.FAILED.equals(comm.getState()));
        Assert.assertTrue(comm.getThrowable() instanceof RuntimeException);
        Assert.assertEquals(2, comm.getMessage("message").size());
    }

    @Test
    public void testMergeStateFrom() {
        Communication comm1 = new Communication();
        Communication comm2 = new Communication();

        comm1.setState(State.FAILED, true);
        comm2.setState(State.KILLED, true);
        Assert.assertTrue(comm1.mergeStateFrom(comm2) == State.FAILED);

        comm1.setState(State.RUNNING, true);
        comm2.setState(State.SUCCEEDED, true);
        Assert.assertTrue(comm1.mergeStateFrom(comm2) == State.RUNNING);

        comm1.setState(State.FAILED, true);
        comm2.setState(State.SUCCEEDED, true);
        Assert.assertTrue(comm1.mergeStateFrom(comm2) == State.FAILED);

        comm1.setState(State.SUBMITTING, true);
        comm2.setState(State.SUCCEEDED, true);
        Assert.assertTrue(comm1.mergeStateFrom(comm2) == State.RUNNING);

    }
}
