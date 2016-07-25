package com.alibaba.datax.core.transport.transformer;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.core.scaffold.RecordProducer;
import org.junit.Assert;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * no comments.
 * Created by liqiang on 16/3/16.
 */
public class SubstrTransformerTest {

    @Test
    public void testNormal() throws Exception {

        Record record1 = RecordProducer.produceRecord();

        SubstrTransformer substrTransformer1 = new SubstrTransformer();

        substrTransformer1.evaluate(record1,1,"0","3");

        Assert.assertEquals(record1.getColumn(1).asString(), "baz");


        Record record2 = RecordProducer.produceRecord();

        SubstrTransformer substrTransformer2 = new SubstrTransformer();

        substrTransformer2.evaluate(record2,1,"0","6");

        Assert.assertEquals(record2.getColumn(1).asString(),"bazhen");


        Record record3 = RecordProducer.produceRecord();

        SubstrTransformer substrTransformer3 = new SubstrTransformer();

        substrTransformer3.evaluate(record3,1,"0","7");

        Assert.assertEquals(record3.getColumn(1).asString(),"bazhen");


        Record record4 = RecordProducer.produceRecord();

        SubstrTransformer substrTransformer4 = new SubstrTransformer();

        substrTransformer4.evaluate(record4,1,"5","7");

        Assert.assertEquals(record4.getColumn(1).asString(),"n");



        Record record5 = RecordProducer.produceRecord();

        SubstrTransformer substrTransformer5 = new SubstrTransformer();

        substrTransformer5.evaluate(record5,1,"6","7");

        Assert.assertEquals(record5.getColumn(1).asString(),"");



        Record record6 = RecordProducer.produceRecord();

        SubstrTransformer substrTransformer6 = new SubstrTransformer();

        Exception exp = null;
        try {
            substrTransformer6.evaluate(record6, 1, "7", "7");
        }catch (Exception e){
            exp = e;
        }
        Assert.assertEquals(exp.getMessage(),"Code:[TransformerErrorCode-06], Description:[Transformer run exception].  - dx_substr startIndex(7) out of range(6) - dx_substr startIndex(7) out of range(6)");


        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy");
        String expYeah = simpleDateFormat.format(new Date());

        Record record7 = RecordProducer.produceRecord();

        SubstrTransformer substrTransformer7 = new SubstrTransformer();

        substrTransformer7.evaluate(record7,3,"0","4");

        Assert.assertEquals(record7.getColumn(3).asString(),expYeah);



        Record record8 = RecordProducer.produceRecord();

        SubstrTransformer substrTransformer8 = new SubstrTransformer();

        substrTransformer8.evaluate(record8,0,"0","4");

        Assert.assertEquals(record8.getColumn(0).asString(),"1");

        Record record10 = RecordProducer.produceRecordHasNull();

        SubstrTransformer substrTransformer10 = new SubstrTransformer();

        substrTransformer10.evaluate(record10,6,"6","7");

        Assert.assertEquals(record10.getColumn(6).asString(), null);

        Exception exp2 = null;

        try {
            substrTransformer8.evaluate(record6);
        }catch (Exception e){
            exp2 = e;
        }

        Assert.assertEquals(exp2.getMessage(),"Code:[TransformerErrorCode-05], Description:[Transformer parameter illegal].  - paras:[] => dx_substr paras must be 3");


        Exception exp3 = null;

        try {
            substrTransformer8.evaluate(record6,1,7,"7");
        }catch (Exception e){
            exp3 = e;
        }

        Assert.assertEquals(exp3.getMessage(),"Code:[TransformerErrorCode-05], Description:[Transformer parameter illegal].  - paras:[1, 7, 7] => java.lang.Integer cannot be cast to java.lang.String");

    }
}