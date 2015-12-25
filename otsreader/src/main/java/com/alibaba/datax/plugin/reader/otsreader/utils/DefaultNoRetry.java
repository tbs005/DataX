package com.alibaba.datax.plugin.reader.otsreader.utils;

import com.aliyun.openservices.ots.internal.OTSRetryStrategy;

public class DefaultNoRetry implements OTSRetryStrategy {

    @Override
    public boolean shouldRetry(String action, Exception ex, int retries) {
        return false;
    }

    @Override
    public long getPauseDelay(String s, Exception e, int i) {
        return 0;
    }

}