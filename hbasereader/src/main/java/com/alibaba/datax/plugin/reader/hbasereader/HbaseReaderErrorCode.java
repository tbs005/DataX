package com.alibaba.datax.plugin.reader.hbasereader;

import com.alibaba.datax.common.spi.ErrorCode;

public enum HbaseReaderErrorCode implements ErrorCode {
    REQUIRED_VALUE("HbaseReader-00", "您缺失了必须填写的参数值."),
    ILLEGAL_VALUE("HbaseReader-01", "您配置的值不合法."),
    PREPAR_READ_ERROR("HbaseReader-02", "准备读取 Hbase 时出错."),
    SPLIT_ERROR("HbaseReader-03", "切分 Hbase 表时出错."),
    INIT_TABLE_ERROR("HbaseReader-04", "初始化 Hbase 抽取表时出错."),

    ;

    private final String code;
    private final String description;

    private HbaseReaderErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.description;
    }

    @Override
    public String toString() {
        return String.format("Code:[%s], Description:[%s]. ", this.code,
                this.description);
    }
}
