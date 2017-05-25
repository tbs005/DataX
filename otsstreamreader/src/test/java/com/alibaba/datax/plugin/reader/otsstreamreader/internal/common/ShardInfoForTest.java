package com.alibaba.datax.plugin.reader.otsstreamreader.internal.common;

public class ShardInfoForTest {
    private String shardId;
    private String parentId;
    private String parentSiblingId;
    private long startTime;
    private long endTime;
    private int rowNum;

    /**
     * siblingId只是为了方便使用MockOTS创建split后两个shard，
     * 第一个shard的siblingId为第二个shard，第二个shard的siblingId为null。
     */
    private String siblingId;

    public ShardInfoForTest(String shardId, long startTime, long endTime, int rowNum) {
       this(shardId, null, null, startTime, endTime, rowNum);
    }

    public ShardInfoForTest(String shardId, String parentId, String parentSiblingId, long startTime, long endTime, int rowNum) {
        this(shardId, parentId, parentSiblingId, null, startTime, endTime, rowNum);
    }

    public ShardInfoForTest(String shardId, String parentId, String parentSiblingId, String siblingId, long startTime, long endTime, int rowNum) {
        this.shardId = shardId;
        this.parentId = parentId;
        this.parentSiblingId = parentSiblingId;
        this.siblingId = siblingId;
        this.startTime = startTime;
        this.endTime = endTime;
        this.rowNum = rowNum;
    }

    public String getShardId() {
        return shardId;
    }

    public void setShardId(String shardId) {
        this.shardId = shardId;
    }

    public String getParentId() {
        return parentId;
    }

    public void setParentId(String parentId) {
        this.parentId = parentId;
    }

    public String getParentSiblingId() {
        return parentSiblingId;
    }

    public void setParentSiblingId(String parentSiblingId) {
        this.parentSiblingId = parentSiblingId;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public int getRowNum() {
        return rowNum;
    }

    public void setRowNum(int rowNum) {
        this.rowNum = rowNum;
    }

    public String getSiblingId() {
        return siblingId;
    }

    public void setSiblingId(String siblingId) {
        this.siblingId = siblingId;
    }
}