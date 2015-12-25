package com.alibaba.datax.plugin.writer.otswriter.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.aliyun.openservices.ots.OTS;

/**
 * 请求满足特定条件就发送数据
 */
public class OTSSendBuffer {
    private int totalSize = 0;
    
    private OTSConf conf = null;
    private OTSBatchWriteRowTaskManager manager = null;

    private Map<OTSRowPrimaryKey, OTSLine> lines = new HashMap<OTSRowPrimaryKey, OTSLine>();

    public OTSSendBuffer(
            OTS ots,
            TaskPluginCollector collector,
            OTSConf conf) {
        this.conf = conf;
        this.manager = new OTSBatchWriteRowTaskManager(ots, collector, conf);
    }

    public void write(OTSLine line) throws Exception {
        // 检查是否满足发送条件
        if (lines.size() >= conf.getBatchWriteCount() || 
            ((totalSize + line.getDataSize()) > conf.getRestrictConf().getRequestTotalSizeLimition() && totalSize > 0)) {
            
            manager.execute(new ArrayList<OTSLine>(lines.values()));
            lines.clear();
            totalSize = 0;
        }
        OTSRowPrimaryKey oPk = new OTSRowPrimaryKey(line.getPk().getPrimaryKey());
        OTSLine old = lines.get(oPk);
        if (old != null) {
            totalSize -= old.getDataSize(); // 移除相同PK的行的数据大小
        }
        lines.put(oPk, line);
        totalSize += line.getDataSize();
    }

    public void flush() throws Exception {
        // 发送最后剩余的数据
        if (!lines.isEmpty()) {
            manager.execute(new ArrayList<OTSLine>(lines.values()));
        }
    }

    public void close() throws Exception {
        flush();
        manager.close();
    }
}
