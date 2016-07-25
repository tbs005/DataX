package com.alibaba.datax.plugin.reader.otsreader.utils;

import com.aliyun.openservices.ots.internal.OTSRetryStrategy;

public class DefaultNoRetry extends OTSRetryStrategy {

    @Override
    public boolean shouldRetry(String action, Exception ex, int retries) {
        return false;
    }

}