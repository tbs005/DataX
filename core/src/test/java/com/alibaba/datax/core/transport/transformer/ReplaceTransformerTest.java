package com.alibaba.datax.core.transport.transformer;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.core.scaffold.RecordProducer;
import org.junit.Assert;
import org.junit.Test;

/**
 * no comments.
 * Created by liqiang on 16/3/16.
 */
public class ReplaceTransformerTest {

    @Test
    public void testName() throws Exception {

        Record record1 = RecordProducer.produceRecord();

        ReplaceTransformer replaceTransformer1 = new ReplaceTransformer();

        replaceTransformer1.evaluate(record1,1,"0","3","****");

        Assert.assertEquals(record1.getColumn(1).asString(), "****hen");



        Record record2 = RecordProducer.produceRecord();

        ReplaceTransformer replaceTransformer2 = new ReplaceTransformer();

        replaceTransformer2.evaluate(record2,1,"0","6","****");

        Assert.assertEquals(record2.getColumn(1).asString(), "****");


        Record record3 = RecordProducer.produceRecord();

        ReplaceTransformer replaceTransformer3 = new ReplaceTransformer();

        replaceTransformer3.evaluate(record3,1,"0","7","****");

        Assert.assertEquals(record3.getColumn(1).asString(), "****");


        Record record4 = RecordProducer.produceRecord();

        ReplaceTransformer replaceTransformer4 = new ReplaceTransformer();

        replaceTransformer4.evaluate(record4,1,"5","7","****");

        Assert.assertEquals(record4.getColumn(1).asString(), "bazhe****");


        Record record5 = RecordProducer.produceRecord();

        ReplaceTransformer replaceTransformer5 = new ReplaceTransformer();

        replaceTransformer5.evaluate(record5,1,"6","7","****");

        Assert.assertEquals(record5.getColumn(1).asString(), "bazhen****");


        Record record10 = RecordProducer.produceRecordHasNull();

        ReplaceTransformer replaceTransformer10 = new ReplaceTransformer();

        replaceTransformer10.evaluate(record10,6,"6","7","****");

        Assert.assertEquals(record10.getColumn(6).asString(), null);


        Record record6 = RecordProducer.produceRecord();

        ReplaceTransformer replaceTransformer6 = new ReplaceTransformer();

        Exception exp = null;

        try {
            replaceTransformer6.evaluate(record6,1,"7","7","****");
        }catch (Exception e){
            exp = e;
        }
        Assert.assertEquals(exp.getMessage(),"Code:[TransformerErrorCode-06], Description:[Transformer run exception].  - dx_replace startIndex(7) out of range(6) - dx_replace startIndex(7) out of range(6)");


        Exception exp2 = null;

        try {
            replaceTransformer6.evaluate(record6);
        }catch (Exception e){
            exp2 = e;
        }

        Assert.assertEquals(exp2.getMessage(),"Code:[TransformerErrorCode-05], Description:[Transformer parameter illegal].  - paras:[] => dx_replace paras must be 4");


        Exception exp3 = null;

        try {
            replaceTransformer6.evaluate(record6,1,7,"7","****");
        }catch (Exception e){
            exp3 = e;
        }

        Assert.assertEquals(exp3.getMessage(),"Code:[TransformerErrorCode-05], Description:[Transformer parameter illegal].  - paras:[1, 7, 7, ****] => java.lang.Integer cannot be cast to java.lang.String");

    }
}