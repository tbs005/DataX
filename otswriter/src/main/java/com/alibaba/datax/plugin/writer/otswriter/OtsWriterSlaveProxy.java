package com.alibaba.datax.plugin.writer.otswriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSConf;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSConst;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSErrorMessage;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSLine;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSSendBuffer;
import com.alibaba.datax.plugin.writer.otswriter.utils.DefaultNoRetry;
import com.alibaba.datax.plugin.writer.otswriter.utils.GsonParser;
import com.aliyun.openservices.ots.ClientConfiguration;
import com.aliyun.openservices.ots.OTS;
import com.aliyun.openservices.ots.OTSClient;
import com.aliyun.openservices.ots.OTSServiceConfiguration;


public class OtsWriterSlaveProxy {

    private static final Logger LOG = LoggerFactory.getLogger(OtsWriterSlaveProxy.class);

    private OTSConf conf = null;
    private OTSSendBuffer buffer = null;
    private OTS ots = null;

    public void init(Configuration configuration) {
        conf = GsonParser.jsonToConf(configuration.getString(OTSConst.OTS_CONF));
        
        ClientConfiguration clientConfigure = new ClientConfiguration();
        clientConfigure.setIoThreadCount(conf.getIoThreadCount());
        clientConfigure.setMaxConnections(conf.getConcurrencyWrite());
        clientConfigure.setSocketTimeoutInMillisecond(conf.getSocketTimeout());
        clientConfigure.setConnectionTimeoutInMillisecond(conf.getConnectTimeout());

        OTSServiceConfiguration otsConfigure = new OTSServiceConfiguration();
        otsConfigure.setRetryStrategy(new DefaultNoRetry());

        ots = new OTSClient(
                conf.getEndpoint(),
                conf.getAccessId(),
                conf.getAccessKey(),
                conf.getInstanceName(),
                clientConfigure,
                otsConfigure);
    }

    public void close() {
        ots.shutdown();
    }
    
    public void write(RecordReceiver recordReceiver, TaskPluginCollector collector) throws Exception {
        LOG.info("write begin.");
        int expectColumnCount = conf.getPrimaryKeyColumn().size() + conf.getAttributeColumn().size();
        Record record = null;
        buffer = new OTSSendBuffer(ots, collector, conf);
        while ((record = recordReceiver.getFromReader()) != null) {
            
            LOG.debug("Record Raw: {}", record.toString());
            
            int columnCount = record.getColumnNumber();
            if (columnCount != expectColumnCount) {
                // 如果Column的个数和预期的个数不一致时，认为是系统故障或者用户配置Column错误，异常退出
                throw new IllegalArgumentException(String.format(OTSErrorMessage.RECORD_AND_COLUMN_SIZE_ERROR, columnCount, expectColumnCount));
            }
            
            OTSLine line = null;
            
            // 类型转换
            try {
                line = new OTSLine(conf.getTableName(), conf.getOperation(), record, conf.getPrimaryKeyColumn(), conf.getAttributeColumn());
            } catch (IllegalArgumentException e) {
                LOG.warn("Convert fail。", e);
                collector.collectDirtyRecord(record, e.getMessage());
                continue;
            }
            buffer.write(line);
        }
        buffer.close();
        LOG.info("write end.");
    }
}
