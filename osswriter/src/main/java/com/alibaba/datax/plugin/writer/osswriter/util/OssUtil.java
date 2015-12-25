package com.alibaba.datax.plugin.writer.osswriter.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.osswriter.Constant;
import com.alibaba.datax.plugin.writer.osswriter.Key;
import com.alibaba.datax.plugin.writer.osswriter.OssWriterErrorCode;
import com.aliyun.oss.ClientConfiguration;
import com.aliyun.oss.OSSClient;

public class OssUtil {
    public static OSSClient initOssClient(Configuration conf) {
        String endpoint = conf.getString(Key.ENDPOINT);
        String accessId = conf.getString(Key.ACCESSID);
        String accessKey = conf.getString(Key.ACCESSKEY);
        ClientConfiguration ossConf = new ClientConfiguration();
        ossConf.setSocketTimeout(Constant.SOCKETTIMEOUT);
        OSSClient client = null;
        try {
            client = new OSSClient(endpoint, accessId, accessKey, ossConf);

        } catch (IllegalArgumentException e) {
            throw DataXException.asDataXException(
                    OssWriterErrorCode.ILLEGAL_VALUE, e.getMessage());
        }

        return client;
    }
}
