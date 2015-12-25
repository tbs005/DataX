package com.alibaba.datax.plugin.writer.otswriter.model;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.aliyun.openservices.ots.OTS;

/**
 * 控制Task的并发数目
 *
 */
public class OTSBatchWriteRowTaskManager {

    private OTS ots = null;
    private TaskPluginCollector collector = null;
    private OTSBlockingExecutor executorService = null;
    private OTSConf conf = null;

    private static final Logger LOG = LoggerFactory.getLogger(OTSBatchWriteRowTaskManager.class);

    public OTSBatchWriteRowTaskManager(
            OTS ots,
            TaskPluginCollector collector,
            OTSConf conf) {
        this.ots = ots;
        this.collector = collector;
        this.conf = conf;
        
        executorService = new OTSBlockingExecutor(conf.getConcurrencyWrite());
    }

    public void execute(List<OTSLine> lines) throws Exception {
        LOG.debug("Begin execute.");
        executorService.execute(new OTSBatchWriterRowTask(collector, ots, conf, lines));
        LOG.debug("End execute.");
    }

    public void close() throws Exception {
        LOG.debug("Begin close.");
        executorService.shutdown();
        LOG.debug("End close.");
    }
}
