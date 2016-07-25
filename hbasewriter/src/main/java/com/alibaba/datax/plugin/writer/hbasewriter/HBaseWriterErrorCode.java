package com.alibaba.datax.plugin.writer.hbasewriter;

import com.alibaba.datax.common.spi.ErrorCode;

/**
 * Created by qifeng.sxm on 2015/12/11.
 */
public enum HBaseWriterErrorCode implements ErrorCode {
	
	HBASE_TABLE_CONF_ERROR("HBaseWriter-01","HBase表名没有配置"),
	HBASE_ROWKEY_CONF_ERROR("HBaseWriter-02","HBase表主键没有配置或配置不正确,格式:0|string"),
	HBASE_COLUMN_CONF_ERROR("HBaseWriter-03","HBase表字段没有配置或配置不正确,格式:1|string|cf:request_time,2|string|cf:host,3|string|cf:uri"),
	HBASE_NULLMODE_CONF_ERROR("HBaseWriter-04","HBase表数据空值默认值没有配置"),
	HBASE_ENCODE_CONF_ERROR("HBaseWriter-05","HBase表数据编码没有配置"),
	HBASE_BATCHROWS_CONF_ERROR("HBaseWriter-06","HBase表数据导入批量单位没有配置"),
	HBASE_CONF_ERROR("HBaseWriter-07","HBase客户端configuration没有配置或配置不正确"),
	
	ZK_PARENT_CONF_ERROR("HBaseWriter-08","HBase在ZooKeeper中的根目录没有配置或配置不正确,格式:/hbase"),
	ZK_QUORUM_CONF_ERROR("HBaseWriter-09","ZooKeeper集群可服务主机列表没有配置或配置不正确,格式:ip1,ip2,ip3"),
	ZK_CLIENTPORT_CONF_ERROR("HBaseWriter-10","ZooKeeper集群客户端连接端口没有配置或配置不正确"),
    ILLEGAL_VALUES_ERROR("HBaseWriter-11", "配置字段个数出现不一致"),
    
    HBASE_RUNNING_ERROR("HBaseWriter-12", "HBase客户端运行时异常");

    HBaseWriterErrorCode(String code, String desc) {
        this.code = code;
        this.desc = desc;
    }


    private String code;
    private String desc;

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.desc;
    }
}
