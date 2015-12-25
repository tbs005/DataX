package com.alibaba.datax.plugin.writer.adswriter.insert;

import com.alibaba.datax.common.exception.DataXException;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.ListUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.writer.adswriter.AdsException;
import com.alibaba.datax.plugin.writer.adswriter.AdsWriterErrorCode;
import com.alibaba.datax.plugin.writer.adswriter.ads.ColumnInfo;
import com.alibaba.datax.plugin.writer.adswriter.ads.TableInfo;
import com.alibaba.datax.plugin.writer.adswriter.load.AdsHelper;
import com.alibaba.datax.plugin.writer.adswriter.util.Key;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;


public class AdsInsertUtil {

    private static final Logger LOG = LoggerFactory
            .getLogger(AdsInsertUtil.class);

    public Connection getAdsConnect(Configuration conf) {
        String userName = conf.getString(Key.USERNAME);
        String passWord = conf.getString(Key.PASSWORD);
        String adsURL = conf.getString(Key.ADS_URL);
        String schema = conf.getString(Key.SCHEMA);
        String jdbcUrl = "jdbc:mysql://" + adsURL + "/" + schema + "?useUnicode=true&characterEncoding=UTF-8";

        Connection connection = DBUtil.getConnection(DataBaseType.ADS, userName, passWord, jdbcUrl);
        return connection;
    }


    public static List<String> getAdsTableColumnNames(Configuration conf) {
        List<String> tableColumns = new ArrayList<String>();
        String userName = conf.getString(Key.USERNAME);
        String passWord = conf.getString(Key.PASSWORD);
        String adsUrl = conf.getString(Key.ADS_URL);
        String schema = conf.getString(Key.SCHEMA);
        String tableName = conf.getString(Key.ADS_TABLE);
        AdsHelper adsHelper = new AdsHelper(adsUrl, userName, passWord, schema);
        TableInfo tableInfo= null;
        try {
            tableInfo = adsHelper.getTableInfo(tableName);
        } catch (AdsException e) {
            throw DataXException.asDataXException(AdsWriterErrorCode.GET_ADS_TABLE_MEATA_FAILED, e);
        }

        List<ColumnInfo> columnInfos = tableInfo.getColumns();
        for(ColumnInfo columnInfo: columnInfos) {
            tableColumns.add(columnInfo.getName());
        }

        LOG.info("table:[{}] all columns:[\n{}\n].", tableName,
                StringUtils.join(tableColumns, ","));
        return tableColumns;
    }

    public static Triple<List<String>, List<Integer>, List<String>> getColumnMetaData
            (Configuration configuration, List<String> userColumns) {
        Triple<List<String>, List<Integer>, List<String>> columnMetaData = new ImmutableTriple<List<String>, List<Integer>, List<String>>(
                new ArrayList<String>(), new ArrayList<Integer>(),
                new ArrayList<String>());

        List<ColumnInfo> columnInfoList = getAdsTableColumns(configuration);
        for(String column : userColumns) {
            for (ColumnInfo columnInfo : columnInfoList) {
                if(column.equals(columnInfo.getName())) {
                    columnMetaData.getLeft().add(columnInfo.getName());
                    columnMetaData.getMiddle().add(columnInfo.getDataType().sqlType);
                    columnMetaData.getRight().add(
                            columnInfo.getDataType().name);
                }
            }
        }
        return columnMetaData;
    }

    public static List<ColumnInfo> getAdsTableColumns(Configuration conf) {
        String userName = conf.getString(Key.USERNAME);
        String passWord = conf.getString(Key.PASSWORD);
        String adsUrl = conf.getString(Key.ADS_URL);
        String schema = conf.getString(Key.SCHEMA);
        String tableName = conf.getString(Key.ADS_TABLE);
        AdsHelper adsHelper = new AdsHelper(adsUrl, userName, passWord, schema);
        TableInfo tableInfo= null;
        try {
            tableInfo = adsHelper.getTableInfo(tableName);
        } catch (AdsException e) {
            throw DataXException.asDataXException(AdsWriterErrorCode.GET_ADS_TABLE_MEATA_FAILED, e);
        }

        List<ColumnInfo> columnInfos = tableInfo.getColumns();

        return columnInfos;
    }

    public static void dealColumnConf(Configuration originalConfig, List<String> tableColumns) {
        List<String> userConfiguredColumns = originalConfig.getList(Key.COLUMN, String.class);
        if (null == userConfiguredColumns || userConfiguredColumns.isEmpty()) {
            throw DataXException.asDataXException(DBUtilErrorCode.ILLEGAL_VALUE,
                    "您的配置文件中的列配置信息有误. 因为您未配置写入数据库表的列名称，DataX获取不到列信息. 请检查您的配置并作出修改.");
        } else {
            if (1 == userConfiguredColumns.size() && "*".equals(userConfiguredColumns.get(0))) {
                LOG.warn("您的配置文件中的列配置信息存在风险. 因为您配置的写入数据库表的列为*，当您的表字段个数、类型有变动时，可能影响任务正确性甚至会运行出错。请检查您的配置并作出修改.");

                // 回填其值，需要以 String 的方式转交后续处理
                originalConfig.set(Key.COLUMN, tableColumns);
            } else if (userConfiguredColumns.size() > tableColumns.size()) {
                throw DataXException.asDataXException(DBUtilErrorCode.ILLEGAL_VALUE,
                        String.format("您的配置文件中的列配置信息有误. 因为您所配置的写入数据库表的字段个数:%s 大于目的表的总字段总个数:%s. 请检查您的配置并作出修改.",
                                userConfiguredColumns.size(), tableColumns.size()));
            } else {
                // 确保用户配置的 column 不重复
                ListUtil.makeSureNoValueDuplicate(userConfiguredColumns, false);

                // 检查列是否都为数据库表中正确的列（通过执行一次 select column from table 进行判断）
                ListUtil.makeSureBInA(tableColumns, userConfiguredColumns, true);
            }
        }
    }


}
