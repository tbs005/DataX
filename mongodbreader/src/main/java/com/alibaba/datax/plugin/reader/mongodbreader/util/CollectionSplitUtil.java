package com.alibaba.datax.plugin.reader.mongodbreader.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.mongodbreader.KeyConstant;
import com.alibaba.datax.plugin.reader.mongodbreader.MongoDBReaderErrorCode;
import com.google.common.base.Strings;
import com.mongodb.*;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by jianying.wcj on 2015/3/19 0019.
 */
public class CollectionSplitUtil {

    public static List<Configuration> doSplit(
            Configuration originalSliceConfig,int adviceNumber,MongoClient mongoClient) {

        List<Configuration> confList = new ArrayList<Configuration>();

        String dbName = originalSliceConfig.getString(KeyConstant.MONGO_DB_NAME);

        String collectionName = originalSliceConfig.getString(KeyConstant.MONGO_COLLECTION_NAME);

        if(Strings.isNullOrEmpty(dbName) || Strings.isNullOrEmpty(collectionName) || mongoClient == null) {
            throw DataXException.asDataXException(MongoDBReaderErrorCode.ILLEGAL_VALUE,
                    MongoDBReaderErrorCode.ILLEGAL_VALUE.getDescription());
        }

        DB db = mongoClient.getDB(dbName);
        DBCollection collection = db.getCollection(collectionName);

        List<Entry> countInterval = doSplitInterval(adviceNumber,collection);
        for(Entry interval : countInterval) {
            Configuration conf = originalSliceConfig.clone();
            conf.set(KeyConstant.SKIP_COUNT,interval.interval);
            conf.set(KeyConstant.BATCH_SIZE,interval.batchSize);
            confList.add(conf);
        }
        return confList;
    }

    private static List<Entry> doSplitInterval(int adviceNumber,DBCollection collection) {

        List<Entry> intervalCountList = new ArrayList<Entry>();

        long totalCount = collection.count();
        if(totalCount < 0) {
            return intervalCountList;
        }
        // 100 6 => 16 mod 4
        long batchSize = totalCount/adviceNumber;
        for(int i = 0; i < adviceNumber; i++) {
            Entry entry = new Entry();
            /**
             * 这个判断确认不会丢失最后一页数据，
             * 因为 totalCount/adviceNumber 不整除时，如果不做判断会丢失最后一页
             */
            if(i == (adviceNumber - 1)) {
                entry.batchSize = batchSize + adviceNumber;
            } else {
                entry.batchSize = batchSize;
            }
            entry.interval = batchSize * i;
            intervalCountList.add(entry);
        }
        return intervalCountList;
    }

}

class Entry {
    Long interval;
    Long batchSize;
}
