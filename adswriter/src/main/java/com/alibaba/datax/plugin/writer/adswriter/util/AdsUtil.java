package com.alibaba.datax.plugin.writer.adswriter.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.adswriter.load.AdsHelper;
import com.alibaba.datax.plugin.writer.adswriter.AdsWriterErrorCode;
import com.alibaba.datax.plugin.writer.adswriter.load.TransferProjectConf;
import com.alibaba.datax.plugin.writer.adswriter.odps.FieldSchema;
import com.alibaba.datax.plugin.writer.adswriter.odps.TableMeta;
import com.alibaba.datax.plugin.writer.odpswriter.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by judy.lt on 2015/1/30.
 */
public class AdsUtil {
    private static final Logger LOG = LoggerFactory.getLogger(AdsUtil.class);

    /*检查配置文件中必填的配置项是否都已填
    * */
    public static void checkNecessaryConfig(Configuration originalConfig, String writeMode) {
        //检查ADS必要参数
        originalConfig.getNecessaryValue(Key.ADS_URL,
                AdsWriterErrorCode.REQUIRED_VALUE);
        originalConfig.getNecessaryValue(Key.USERNAME,
                AdsWriterErrorCode.REQUIRED_VALUE);
        originalConfig.getNecessaryValue(Key.PASSWORD,
                AdsWriterErrorCode.REQUIRED_VALUE);
        originalConfig.getNecessaryValue(Key.SCHEMA,
                AdsWriterErrorCode.REQUIRED_VALUE);
        if(Constant.LOADMODE.equals(writeMode)) {
            originalConfig.getNecessaryValue(Key.Life_CYCLE,
                    AdsWriterErrorCode.REQUIRED_VALUE);
            Integer lifeCycle = originalConfig.getInt(Key.Life_CYCLE);
            if (lifeCycle <= 0) {
                throw DataXException.asDataXException(AdsWriterErrorCode.INVALID_CONFIG_VALUE, "配置项[lifeCycle]的值必须大于零.");
            }
            originalConfig.getNecessaryValue(Key.ADS_TABLE,
                    AdsWriterErrorCode.REQUIRED_VALUE);
            Boolean overwrite = originalConfig.getBool(Key.OVER_WRITE);
            if (overwrite == null) {
                throw DataXException.asDataXException(AdsWriterErrorCode.REQUIRED_VALUE, "配置项[overWrite]是必填项.");
            }
        }
    }

    /*生成AdsHelp实例
    * */
    public static AdsHelper createAdsHelper(Configuration originalConfig){
        //Get adsUrl,userName,password,schema等参数,创建AdsHelp实例
        String adsUrl = originalConfig.getString(Key.ADS_URL);
        String userName = originalConfig.getString(Key.USERNAME);
        String password = originalConfig.getString(Key.PASSWORD);
        String schema = originalConfig.getString(Key.SCHEMA);
        return new AdsHelper(adsUrl,userName,password,schema);
    }

    public static AdsHelper createAdsHelperWithOdpsAccount(Configuration originalConfig) {
        String adsUrl = originalConfig.getString(Key.ADS_URL);
        String userName = originalConfig.getString(TransferProjectConf.KEY_ACCESS_ID);
        String password = originalConfig.getString(TransferProjectConf.KEY_ACCESS_KEY);
        String schema = originalConfig.getString(Key.SCHEMA);
        return new AdsHelper(adsUrl, userName, password, schema);
    }

    /*生成ODPSWriter Plugin所需要的配置文件
    * */
    public static Configuration generateConf(Configuration originalConfig, String odpsTableName, TableMeta tableMeta, TransferProjectConf transConf){
        Configuration newConfig = originalConfig.clone();
        newConfig.set(Key.ODPSTABLENAME, odpsTableName);
        newConfig.set(Key.ODPS_SERVER, transConf.getOdpsServer());
        newConfig.set(Key.TUNNEL_SERVER,transConf.getOdpsTunnel());
        newConfig.set(Key.ACCESS_ID,transConf.getAccessId());
        newConfig.set(Key.ACCESS_KEY,transConf.getAccessKey());
        newConfig.set(Key.PROJECT,transConf.getProject());
        newConfig.set(Key.TRUNCATE, true);
        newConfig.set(Key.PARTITION,null);
//        newConfig.remove(Key.PARTITION);
        List<FieldSchema> cols = tableMeta.getCols();
        List<String> allColumns = new ArrayList<String>();
        if(cols != null && !cols.isEmpty()){
            for(FieldSchema col:cols){
                allColumns.add(col.getName());
            }
        }
        newConfig.set(Key.COLUMN,allColumns);
        return newConfig;
    }

    /*生成ADS数据导入时的source_path
    * */
    public static String generateSourcePath(String project, String tmpOdpsTableName, String odpsPartition){
        StringBuilder builder = new StringBuilder();
        String partition = transferOdpsPartitionToAds(odpsPartition);
        builder.append("odps://").append(project).append("/").append(tmpOdpsTableName);
        if(odpsPartition != null && !odpsPartition.isEmpty()){
            builder.append("/").append(partition);
        }
        return builder.toString();
    }

    public static String transferOdpsPartitionToAds(String odpsPartition){
        if(odpsPartition == null || odpsPartition.isEmpty())
            return null;
        String adsPartition = formatPartition(odpsPartition);;
        String[] partitions = adsPartition.split("/");
        for(int last = partitions.length; last > 0; last--){

            String partitionPart = partitions[last-1];
            String newPart = partitionPart.replace(".*", "*").replace("*", ".*");
            if(newPart.split("=")[1].equals(".*")){
                adsPartition = adsPartition.substring(0,adsPartition.length()-partitionPart.length());
            }else{
                break;
            }
            if(adsPartition.endsWith("/")){
                adsPartition = adsPartition.substring(0,adsPartition.length()-1);
            }
        }
        if (adsPartition.contains("*"))
            throw DataXException.asDataXException(AdsWriterErrorCode.ODPS_PARTITION_FAILED, "");
        return adsPartition;
    }

    public static String formatPartition(String partition) {
        return partition.trim().replaceAll(" *= *", "=")
                .replaceAll(" */ *", ",").replaceAll(" *, *", ",")
                .replaceAll("'", "").replaceAll(",", "/");
    }
}
