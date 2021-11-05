package com.alibaba.datax.plugin.reader.otsstreamreader.internal.common;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.net.URL;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.config.OTSStreamReaderConfig;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.utils.OTSHelper;
import com.alicloud.openservices.tablestore.*;
import com.alicloud.openservices.tablestore.model.*;

public class ConfigurationHelper {

    public static Configuration loadConf() {
        InputStream f;
        try {
            String resource = Configuration.class.getClassLoader().getResource("datax_streamreader_conf.json").getFile();
            f = new FileInputStream(resource);
            Configuration p = Configuration.from(f);
            return p;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public static Configuration loadConf(String dataTable, String statusTable,
                                         long startTimestampMillis, long endTimestampMillis) {
        Configuration configuration = loadConf();
        configuration.set("dataTable", dataTable);
        configuration.set("statusTable", statusTable);
        configuration.set("startTimestampMillis", startTimestampMillis);
        configuration.set("endTimestampMillis", endTimestampMillis);
        return configuration;
    }

    public static OTSStreamReaderConfig loadReaderConfig(String dataTable, String statusTable,
                                                         long startTimestampMillis, long endTimestampMillis) {
        Configuration configuration = loadConf(dataTable, statusTable, startTimestampMillis, endTimestampMillis);
        OTSStreamReaderConfig config = OTSStreamReaderConfig.load(configuration);
        return config;
    }

    public static SyncClientInterface getOTSFromConfig() {
        OTSStreamReaderConfig config = loadReaderConfig("a", "b", 1, 2);
        return OTSHelper.getOTSInstance(config);
    }

    public static AsyncClient getOTSAsyncFromConfig() {
        OTSStreamReaderConfig config = loadReaderConfig("a", "b", 1, 2);
        AsyncClient otsAsync = new AsyncClient(config.getEndpoint(), config.getAccessId(), config.getAccessKey(), config.getInstanceName());
        return otsAsync;
    }
}

