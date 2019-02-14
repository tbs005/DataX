package com.alibaba.datax.core.transport.transformer;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.core.scaffold.RecordProducer;
import org.junit.Assert;
import org.junit.Test;

/**
 * no comments.
 * Created by liqiang on 16/3/16.
 */
public class PadTransformerTest {

    @Test
    public void testName() throws Exception {

        Record record1 = RecordProducer.produceRecord();

        PadTransformer padTransformer1 = new PadTransformer();

        padTransformer1.evaluate(record1,1,"l","3","12345");

        Assert.assertEquals(record1.getColumn(1).asString(), "baz");


        Record record2 = RecordProducer.produceRecord();

        PadTransformer padTransformer2 = new PadTransformer();

        padTransformer2.evaluate(record2,1,"l","8","12345");

        Assert.assertEquals(record2.getColumn(1).asString(), "12bazhen");


        Record record3 = RecordProducer.produceRecord();

        PadTransformer padTransformer3 = new PadTransformer();

        padTransformer3.evaluate(record3,1,"l","14","12345");

        Assert.assertEquals(record3.getColumn(1).asString(), "12345123bazhen");


        Record record4 = RecordProducer.produceRecord();

        PadTransformer padTransformer4 = new PadTransformer();

        padTransformer4.evaluate(record4,1,"l","14","123");

        Assert.assertEquals(record4.getColumn(1).asString(), "12312312bazhen");



        Record record5 = RecordProducer.produceRecord();

        PadTransformer padTransformer5 = new PadTransformer();

        padTransformer5.evaluate(record5,1,"r","3","12345");

        Assert.assertEquals(record5.getColumn(1).asString(), "baz");



        Record record6 = RecordProducer.produceRecord();

        PadTransformer padTransformer6 = new PadTransformer();

        padTransformer6.evaluate(record6,1,"r","8","12345");

        Assert.assertEquals(record6.getColumn(1).asString(), "bazhen12");


        Record record7 = RecordProducer.produceRecord();

        PadTransformer padTransformer7 = new PadTransformer();

        padTransformer7.evaluate(record7,1,"r","14","12345");

        Assert.assertEquals(record7.getColumn(1).asString(), "bazhen12345123");


        Record record8 = RecordProducer.produceRecord();

        PadTransformer padTransformer8 = new PadTransformer();

        padTransformer8.evaluate(record8,1,"r","14","123");

        Assert.assertEquals(record8.getColumn(1).asString(), "bazhen12312312");


        Record record9 = RecordProducer.produceRecord();

        PadTransformer padTransformer9 = new PadTransformer();

        padTransformer9.evaluate(record9,1,"r","14","*");

        Assert.assertEquals(record9.getColumn(1).asString(), "bazhen********");


        Record record10 = RecordProducer.produceRecordHasNull();

        PadTransformer padTransformer10 = new PadTransformer();

        padTransformer10.evaluate(record10,6,"r","14","*");

        Assert.assertEquals(record10.getColumn(6).asString(), "**************");


        Exception exp2 = null;

        try {
            padTransformer9.evaluate(record6);
        }catch (Exception e){
            exp2 = e;
        }

        Assert.assertEquals(exp2.getMessage(),"Code:[TransformerErrorCode-05], Description:[Transformer parameter illegal].  - paras:[] => dx_pad paras must be 4");


        Exception exp3 = null;

        try {
            padTransformer9.evaluate(record6,1,7,"7","****");
        }catch (Exception e){
            exp3 = e;
        }

        Assert.assertEquals(exp3.getMessage(),"Code:[TransformerErrorCode-05], Description:[Transformer parameter illegal].  - paras:[1, 7, 7, ****] => java.lang.Integer cannot be cast to java.lang.String");


    }
}