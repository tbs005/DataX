package com.alibaba.datax.plugin.reader.otsstreamreader.internal.config;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.OTSStreamReaderException;
import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TestOTSStreamReaderConfig {

    @Test
    public void testLoad() {
        Configuration configuration = Configuration.newDefault();
        configuration.set("endpoint", " http://1.1.1.1:8080/ ");
        configuration.set("accessId", " accessId ");
        configuration.set("accessKey", " accessKey ");
        configuration.set("instanceName", " test-inst ");
        configuration.set("dataTable", " dataTable");
        configuration.set("statusTable", "statusTable ");
        configuration.set("startTimestampMillis", 123);
        configuration.set("endTimestampMillis", "000456");

        OTSStreamReaderConfig config = OTSStreamReaderConfig.load(configuration);
        assertEquals(config.getEndpoint(), "http://1.1.1.1:8080/");
        assertEquals(config.getAccessId(), "accessId");
        assertEquals(config.getAccessKey(), "accessKey");
        assertEquals(config.getInstanceName(), "test-inst");
        assertEquals(config.getDataTable(), "dataTable");
        assertEquals(config.getStatusTable(), "statusTable");
        assertEquals(config.getStartTimestampMillis(), 123);
        assertEquals(config.getEndTimestampMillis(), 456);

        {
            for (int i = 0; i < 6; i++) {
                configuration = Configuration.newDefault();
                if (i != 0) {
                    configuration.set("endpoint", " http://1.1.1.1:8080/ ");
                }
                if (i != 1) {
                    configuration.set("accessId", " accessId ");
                }
                if (i != 2) {
                    configuration.set("accessKey", " accessKey ");
                }
                if (i != 3) {
                    configuration.set("instanceName", " test-inst ");
                }
                if (i != 4) {
                    configuration.set("dataTable", " dataTable");
                }
                if (i != 5) {
                    configuration.set("statusTable", "statusTable ");
                }

                try {
                    config = OTSStreamReaderConfig.load(configuration);
                    fail();
                } catch (IllegalArgumentException ex) {
                    assertEquals("missing the key.", ex.getMessage());
                }
            }
        }

        {
            for (int i = 0; i < 3; i++) {
                configuration = Configuration.newDefault();
                configuration.set("endpoint", " http://1.1.1.1:8080/ ");
                configuration.set("accessId", " accessId ");
                configuration.set("accessKey", " accessKey ");
                configuration.set("instanceName", " test-inst ");
                configuration.set("dataTable", " dataTable");
                configuration.set("statusTable", "statusTable ");
                if (i == 1) {
                    configuration.set("startTimestampMillis", 123);
                }
                if (i == 2) {
                    configuration.set("endTimestampMillis", 123);
                }
                try {
                    config = OTSStreamReaderConfig.load(configuration);
                    fail();
                } catch (OTSStreamReaderException ex) {
                    assertEquals("Must set date or time range millis or time range string, please check your config.", ex.getMessage());
                }
            }
        }

        {
            for (int i = 0; i < 3; i++) {
                configuration = Configuration.newDefault();
                configuration.set("endpoint", " http://1.1.1.1:8080/ ");
                configuration.set("accessId", " accessId ");
                configuration.set("accessKey", " accessKey ");
                configuration.set("instanceName", " test-inst ");
                configuration.set("dataTable", " dataTable");
                configuration.set("statusTable", "statusTable ");
                configuration.set("date", "20150924");
                if (i == 0) {
                    configuration.set("startTimestampMillis", 1);
                    configuration.set("endTimestampMillis", 100);
                }
                if (i == 1) {
                    configuration.set("startTimestampMillis", 123);
                }
                if (i == 2) {
                    configuration.set("endTimestampMillis", 123);
                }
                try {
                    config = OTSStreamReaderConfig.load(configuration);
                    fail();
                } catch (OTSStreamReaderException ex) {
                    assertEquals("Can't set date and time range both, please check your config.", ex.getMessage());
                }
            }
        }

        {
            configuration = Configuration.newDefault();
            configuration.set("endpoint", " http://1.1.1.1:8080/ ");
            configuration.set("accessId", " accessId ");
            configuration.set("accessKey", " accessKey ");
            configuration.set("instanceName", " test-inst ");
            configuration.set("dataTable", " dataTable");
            configuration.set("statusTable", "statusTable ");
            configuration.set("date", "  20150924 ");

            config = OTSStreamReaderConfig.load(configuration);
            assertEquals(config.getStartTimestampMillis(), 1443024000000L);
            assertEquals(config.getEndTimestampMillis(), 1443110400000L);
        }

        {
            configuration = Configuration.newDefault();
            configuration.set("endpoint", " http://1.1.1.1:8080/ ");
            configuration.set("accessId", " accessId ");
            configuration.set("accessKey", " accessKey ");
            configuration.set("instanceName", " test-inst ");
            configuration.set("dataTable", " dataTable");
            configuration.set("statusTable", "statusTable ");
            configuration.set("startTimestampMillis", 100000000);
            configuration.set("endTimestampMillis", 1000000);

            try {
                config = OTSStreamReaderConfig.load(configuration);
                fail();
            } catch (OTSStreamReaderException ex) {
                assertEquals("EndTimestamp must be larger than startTimestamp.", ex.getMessage());
            }
        }
    }
}
