package com.alibaba.datax.core.scheduler.standalone;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.taskgroup.TaskGroupContainer;

/**
 * Created by jingxing on 14-9-4.
 */
public class StandAloneTestTaskGroupContainer extends TaskGroupContainer {
    public StandAloneTestTaskGroupContainer(Configuration configuration) {
        super(configuration);
    }

    @Override
    public void start() {
        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("start standAlone test task container");
    }
}
