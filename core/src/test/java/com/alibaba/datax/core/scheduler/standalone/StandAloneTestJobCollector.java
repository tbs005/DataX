package com.alibaba.datax.core.scheduler.standalone;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.container.collector.AbstractCollector;
import com.alibaba.datax.dataxservice.face.domain.enums.State;

import java.util.List;

public class StandAloneTestJobCollector extends AbstractCollector {

    public void registerCommunication(List<Configuration> configurationList) {
        System.out.println("register ok");
    }

    public void report(Communication communication) {
        System.out.println("job report 2");
    }

    public Communication collect() {
        return new Communication() {{
            this.setState(State.SUCCEEDED);
        }};
    }

    @Override
    public void registerTGCommunication(List<Configuration> taskGroupConfigurationList) {

    }

    @Override
    public void registerTaskCommunication(List<Configuration> taskConfigurationList) {

    }

    @Override
    public Communication collectFromTask() {
        return null;
    }

    @Override
    public Communication collectFromTaskGroup() {
        return null;
    }
}
