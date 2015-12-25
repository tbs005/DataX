package com.alibaba.datax.plugin.writer.otswriter.model;

import java.util.List;

public class OTSConf {
    private String endpoint= null;
    private String accessId = null;
    private String accessKey = null;
    private String instanceName = null;
    private String tableName = null;
   
    private List<OTSPKColumn> primaryKeyColumn = null;
    private List<OTSAttrColumn> attributeColumn = null;
   
    private int retry =  -1;
    private int sleepInMilliSecond = -1;
    private int batchWriteCount = -1;
    private int concurrencyWrite = -1;
    private int ioThreadCount = -1;
    private int socketTimeout = -1;
    private int connectTimeout = -1;
    
    private OTSOpType operation = null;
    
    private RestrictConf restrictConf = null;
    
    //限制项
    public class RestrictConf {
        private int requestTotalSizeLimition = -1;
        
        public int getRequestTotalSizeLimition() {
            return requestTotalSizeLimition;
        }
        public void setRequestTotalSizeLimition(int requestTotalSizeLimition) {
            this.requestTotalSizeLimition = requestTotalSizeLimition;
        }
    }

    public RestrictConf getRestrictConf() {
        return restrictConf;
    }
    public void setRestrictConf(RestrictConf restrictConf) {
        this.restrictConf = restrictConf;
    }
    public OTSOpType getOperation() {
        return operation;
    }
    public void setOperation(OTSOpType operation) {
        this.operation = operation;
    }
    public List<OTSPKColumn> getPrimaryKeyColumn() {
        return primaryKeyColumn;
    }
    public void setPrimaryKeyColumn(List<OTSPKColumn> primaryKeyColumn) {
        this.primaryKeyColumn = primaryKeyColumn;
    }
    
    public int getConcurrencyWrite() {
        return concurrencyWrite;
    }
    public void setConcurrencyWrite(int concurrencyWrite) {
        this.concurrencyWrite = concurrencyWrite;
    }
    public int getBatchWriteCount() {
        return batchWriteCount;
    }
    public void setBatchWriteCount(int batchWriteCount) {
        this.batchWriteCount = batchWriteCount;
    }
    public String getEndpoint() {
        return endpoint;
    }
    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }
    public String getAccessId() {
        return accessId;
    }
    public void setAccessId(String accessId) {
        this.accessId = accessId;
    }
    public String getAccessKey() {
        return accessKey;
    }
    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }
    public String getInstanceName() {
        return instanceName;
    }
    public void setInstanceName(String instanceName) {
        this.instanceName = instanceName;
    }
    public String getTableName() {
        return tableName;
    }
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
    public List<OTSAttrColumn> getAttributeColumn() {
        return attributeColumn;
    }
    public void setAttributeColumn(List<OTSAttrColumn> attributeColumn) {
        this.attributeColumn = attributeColumn;
    }
    public int getRetry() {
        return retry;
    }
    public void setRetry(int retry) {
        this.retry = retry;
    }
    public int getSleepInMilliSecond() {
        return sleepInMilliSecond;
    }
    public void setSleepInMilliSecond(int sleepInMilliSecond) {
        this.sleepInMilliSecond = sleepInMilliSecond;
    }
    public int getIoThreadCount() {
        return ioThreadCount;
    }
    public void setIoThreadCount(int ioThreadCount) {
        this.ioThreadCount = ioThreadCount;
    }
    public int getSocketTimeout() {
        return socketTimeout;
    }
    public void setSocketTimeout(int socketTimeout) {
        this.socketTimeout = socketTimeout;
    }
    public int getConnectTimeout() {
        return connectTimeout;
    }
    public void setConnectTimeout(int connectTimeout) {
        this.connectTimeout = connectTimeout;
    }
}