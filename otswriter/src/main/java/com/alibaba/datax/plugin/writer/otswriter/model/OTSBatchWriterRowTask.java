package com.alibaba.datax.plugin.writer.otswriter.model;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.plugin.writer.otswriter.callable.BatchWriteRowCallable;
import com.alibaba.datax.plugin.writer.otswriter.callable.PutRowChangeCallable;
import com.alibaba.datax.plugin.writer.otswriter.callable.UpdateRowChangeCallable;
import com.alibaba.datax.plugin.writer.otswriter.utils.Common;
import com.alibaba.datax.plugin.writer.otswriter.utils.RetryHelper;
import com.aliyun.openservices.ots.OTS;
import com.aliyun.openservices.ots.OTSErrorCode;
import com.aliyun.openservices.ots.OTSException;
import com.aliyun.openservices.ots.model.BatchWriteRowResult;
import com.aliyun.openservices.ots.model.BatchWriteRowResult.RowStatus;
import com.aliyun.openservices.ots.model.BatchWriteRowRequest;
import com.aliyun.openservices.ots.model.Error;
import com.aliyun.openservices.ots.model.PutRowRequest;
import com.aliyun.openservices.ots.model.PutRowResult;
import com.aliyun.openservices.ots.model.RowPutChange;
import com.aliyun.openservices.ots.model.RowUpdateChange;
import com.aliyun.openservices.ots.model.UpdateRowRequest;
import com.aliyun.openservices.ots.model.UpdateRowResult;

public class OTSBatchWriterRowTask implements Runnable {
    private TaskPluginCollector collector = null;
    private OTS ots = null;
    private OTSConf conf = null;
    private List<OTSLine> otsLines = new ArrayList<OTSLine>();

    private boolean isDone = false;
    private int retryTimes = 0;

    private static final Logger LOG = LoggerFactory.getLogger(OTSBatchWriterRowTask.class);

    public OTSBatchWriterRowTask(
            TaskPluginCollector collector,
            OTS ots,
            OTSConf conf, 
            List<OTSLine> lines
            ) {
        this.collector = collector;
        this.ots = ots;
        this.conf = conf;

        this.otsLines.addAll(lines);
    }

    @Override
    public void run() {
        LOG.debug("Begin run");
        try {
            sendAll(otsLines);
        } catch (InterruptedException e) {
            LOG.error(e.getMessage());
        }
        LOG.debug("End run");
    }

    public boolean isDone() {
        return this.isDone;
    }

    private boolean isExceptionForSendOneByOne(Exception e) {
        if (e instanceof OTSException) {
            OTSException ee = (OTSException)e;
            if (ee.getErrorCode().equals(OTSErrorCode.INVALID_PARAMETER)|| 
                ee.getErrorCode().equals(OTSErrorCode.REQUEST_TOO_LARGE)
               ) {
                return true;
            }
        }
        return false;
    }
    
    private BatchWriteRowRequest createRequset(List<OTSLine> lines) {
        BatchWriteRowRequest newRequst = new BatchWriteRowRequest();
        switch (conf.getOperation()) {
            case PUT_ROW:
                for (OTSLine l : lines) {
                    newRequst.addRowPutChange((RowPutChange) l.getAttr());
                }
                break;
            case UPDATE_ROW:
                for (OTSLine l : lines) {
                    newRequst.addRowUpdateChange((RowUpdateChange) l.getAttr());
                }
                break;
            default:
                throw new RuntimeException(String.format(OTSErrorMessage.OPERATION_PARSE_ERROR, conf.getOperation()));
        }
        return newRequst;
    }

    private void sendLine(OTSLine line) throws InterruptedException {
        try {
            switch (conf.getOperation()) {
                case PUT_ROW:
                    PutRowRequest putRowRequest = new PutRowRequest();
                    putRowRequest.setRowChange((RowPutChange) line.getAttr());
                    PutRowResult putResult = RetryHelper.executeWithRetry(
                            new PutRowChangeCallable(ots, putRowRequest),
                            conf.getRetry(), 
                            conf.getSleepInMilliSecond());
                    LOG.debug("Requst ID : {}", putResult.getRequestID());
                    break;
                case UPDATE_ROW:
                    UpdateRowRequest updateRowRequest = new UpdateRowRequest();
                    updateRowRequest.setRowChange((RowUpdateChange) line.getAttr());
                    UpdateRowResult updateResult = RetryHelper.executeWithRetry(
                            new UpdateRowChangeCallable(ots, updateRowRequest), 
                            conf.getRetry(), 
                            conf.getSleepInMilliSecond());
                    LOG.debug("Requst ID : {}", updateResult.getRequestID());
                    break;
            }
        } catch (Exception e) {
            LOG.error("Can not send line to OTS. {}", e.getMessage());
            collector.collectDirtyRecord(line.getRecord(), e.getMessage());
        }
    }

    private void sendAllOneByOne(List<OTSLine> lines) throws InterruptedException {
        for (OTSLine l : lines) {
            sendLine(l);
        }
    }

    private void sendAll(List<OTSLine> lines) throws InterruptedException {
        Thread.sleep(Common.getDelaySendMillinSeconds(retryTimes, conf.getSleepInMilliSecond()));
        try {
            BatchWriteRowRequest batchWriteRowRequest = createRequset(lines);
            BatchWriteRowResult result = RetryHelper.executeWithRetry(
                    new BatchWriteRowCallable(ots, batchWriteRowRequest), 
                    conf.getRetry(), 
                    conf.getSleepInMilliSecond());

            LOG.debug("Requst ID : {}", result.getRequestID());
            List<LineAndError> errors = getLineAndError(result, lines);
            if (!errors.isEmpty()){
                if(retryTimes < conf.getRetry()) {
                    retryTimes++;
                    LOG.debug("Retry times : {}", retryTimes);
                    List<OTSLine> newLines = new ArrayList<OTSLine>();
                    for (LineAndError re : errors) {
                        if (RetryHelper.canRetry(re.getError().getCode())) {
                            RetryHelper.logManager.addException(re.getError(), result.getRequestID());
                            newLines.add(re.getLine());
                        } else {
                            LOG.error("Can not retry, record row to collector. {}", re.getError().getMessage());
                            collector.collectDirtyRecord(re.getLine().getRecord(), re.getError().getMessage());
                        }   
                    }
                    if (!newLines.isEmpty()) {
                        sendAll(newLines);
                    }
                } else {
                    LOG.error("Retry times more than limition. RetryTime : {}", retryTimes); 
                    Common.collectDirtyRecord(collector, errors);
                }
            }
        } catch (Exception e) {
            LOG.debug("Send data fail.", e);
            if (isExceptionForSendOneByOne(e)) {
                if (lines.size() == 1) {
                    LOG.error("Can not retry for Exception : {}", e.getMessage()); 
                    Common.collectDirtyRecord(collector, lines, e.getMessage());
                } else {
                    // 进入单行发送的分支
                    sendAllOneByOne(lines);
                }
            } else {
                LOG.error("Can not send lines to OTS. {}", e.getMessage());
                Common.collectDirtyRecord(collector, lines, e.getMessage());
            }
        }
    }

    private List<LineAndError> getLineAndError(BatchWriteRowResult result, List<OTSLine> lines) {
        List<LineAndError> errors = new ArrayList<LineAndError>();

        switch(conf.getOperation()) {
            case PUT_ROW:
                List<RowStatus> putStatus = result.getPutRowStatus(conf.getTableName());
                for (int i = 0; i < putStatus.size(); i++) {
                    if (!putStatus.get(i).isSucceed()) {
                        errors.add(new LineAndError(lines.get(i), putStatus.get(i).getError()));
                    }
                }
                break;
            case UPDATE_ROW:
                List<RowStatus> updateStatus = result.getUpdateRowStatus(conf.getTableName());
                for (int i = 0; i < updateStatus.size(); i++) {
                    if (!updateStatus.get(i).isSucceed()) {
                        errors.add(new LineAndError(lines.get(i), updateStatus.get(i).getError()));
                    }
                }
                break;
            default:
                throw new RuntimeException(String.format(OTSErrorMessage.OPERATION_PARSE_ERROR, conf.getOperation()));
        }
        return errors;
    }

    public class LineAndError {
        private OTSLine line;
        private Error error;

        public LineAndError(OTSLine record, Error error) {
            this.line = record;
            this.error = error;
        }

        public OTSLine getLine() {
            return line;
        }

        public Error getError() {
            return error;
        }
    }
}
