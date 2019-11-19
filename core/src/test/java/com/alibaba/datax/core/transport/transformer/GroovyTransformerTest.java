package com.alibaba.datax.core.transport.transformer;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.core.scaffold.RecordProducer;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * no comments.
 * Created by liqiang on 16/3/20.
 */
public class GroovyTransformerTest {

    @Test
    public void testNormal() throws Exception {
        Record record = RecordProducer.produceRecord();
        GroovyTransformer groovyTransformer = new GroovyTransformer();

        /**
         * groovy 实现的subStr
         */

        String code = "Column column = record.getColumn(1);\n" +
                " String oriValue = column.asString();\n" +
                " String newValue = oriValue.substring(0, 3);\n" +
                " record.setColumn(1, new StringColumn(newValue));\n" +
                " return record;";

        Record record11 = groovyTransformer.evaluate(record, code);

        Assert.assertEquals(record11.getColumn(1).asString(), "baz");


        /**
         * groovy 实现的Replace
         */

        Record record2 = RecordProducer.produceRecord();
        GroovyTransformer groovyTransformer2 = new GroovyTransformer();

        String code2 = "Column column = record.getColumn(1);\n" +
                " String oriValue = column.asString();\n" +
                " String newValue = \"****\" + oriValue.substring(3, oriValue.length());\n" +
                " record.setColumn(1, new StringColumn(newValue));\n" +
                " return record;";

        Record record21 = groovyTransformer2.evaluate(record2, code2);

        Assert.assertEquals(record21.getColumn(1).asString(), "****hen");


        /**
         * groovy 实现的Pad
         */

        Record record3 = RecordProducer.produceRecord();
        GroovyTransformer groovyTransformer3 = new GroovyTransformer();

        String code3 = "Column column = record.getColumn(1);\n" +
                " String oriValue = column.asString();\n" +
                " String padString = \"12345\";\n" +
                " String finalPad = \"\";\n" +
                " int NeedLength = 8 - oriValue.length();\n" +
                "        while (NeedLength > 0) {\n" +
                "\n" +
                "            if (NeedLength >= padString.length()) {\n" +
                "                finalPad += padString;\n" +
                "                NeedLength -= padString.length();\n" +
                "            } else {\n" +
                "                finalPad += padString.substring(0, NeedLength);\n" +
                "                NeedLength = 0;\n" +
                "            }\n" +
                "        }\n" +
                " String newValue= finalPad + oriValue;\n" +
                " record.setColumn(1, new StringColumn(newValue));\n" +
                " return record;";

        Record record31 =groovyTransformer3.evaluate(record3, code3);

        Assert.assertEquals(record31.getColumn(1).asString(), "12bazhen");



        /**
         * groovy 实现的Filter
         */

        Record record4 = RecordProducer.produceRecord();
        GroovyTransformer groovyTransformer4 = new GroovyTransformer();

        String code4 = " return null;";

        Record record41= groovyTransformer4.evaluate(record4,code4);

        Assert.assertEquals(record41, null);

        /**
         * groovy 实现的Filter, 增加extraPackages
         */

        Record record5 = RecordProducer.produceRecord();
        GroovyTransformer groovyTransformer5 = new GroovyTransformer();


        String code5 = " SimpleDateFormat simpleDateFormat = new SimpleDateFormat(\"yyyy-MM-dd\");\n" +
                "String value = simpleDateFormat.format(new Date())+\".*\";\n" +
                "Column column = record.getColumn(3);\n" +
                "String oriValue = column.asString();\n" +
                "if (oriValue.matches(value)) {\n" +
                "            return null;\n" +
                "        } else {\n" +
                "            return record;\n" +
                "        };";
       List<String> extraPackages = new ArrayList<String>();
        extraPackages.add("import java.text.SimpleDateFormat;");

        Record record51= groovyTransformer5.evaluate(record5,code5,extraPackages);

        Assert.assertEquals(record51, null);

    }
}