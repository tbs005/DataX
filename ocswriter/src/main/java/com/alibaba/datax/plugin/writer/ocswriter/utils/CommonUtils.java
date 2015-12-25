package com.alibaba.datax.plugin.writer.ocswriter.utils;

/**
 * Time:    2015-05-12 15:02
 */
public class CommonUtils {

    public static void sleepInMs(long time) {
        try{
            Thread.sleep(time);
        } catch (InterruptedException e) {
            //
        }
    }
}
