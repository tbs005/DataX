package com.alibaba.datax.common.util;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class StrUtilTest {

    @Rule
    public ExpectedException ex= ExpectedException.none();


    @Test
    public void testReplaceVariable() throws Exception {
        Properties valuesMap = System.getProperties();
        valuesMap.put("animal", "quick brown fox");
        valuesMap.put("target", "lazy dog");
        String templateString = "The $animal jumped over the ${target}.";
        String resolvedString =StrUtil.replaceVariable(templateString);
        System.out.println(resolvedString);
        assertEquals(resolvedString, "The quick brown fox jumped over the lazy dog.");
    }

    @Test
    public void testCompressMiddle() throws Exception {
        assertEquals(StrUtil.compressMiddle("0123456789", 2, 2), "01...89");
        assertEquals(StrUtil.compressMiddle("0123456789", 5, 5), "0123456789");
        assertEquals(StrUtil.compressMiddle("0123456789", 6, 7), "0123456789");
        assertEquals(StrUtil.compressMiddle("0123456789", 10, 1), "0123456789");
        assertEquals(StrUtil.compressMiddle("0123456789", 20, 2), "0123456789");
        assertEquals(StrUtil.compressMiddle("0123456789", 2, 20), "0123456789");
    }


    @Test
    public void testCompressMiddleFailed() throws Exception {
        ex.expect(NullPointerException.class);
        StrUtil.compressMiddle(null, 2, 20);
    }

    @Test
    public void testCompressMiddleFailed2() throws Exception {
        ex.expect(IllegalArgumentException.class);
        StrUtil.compressMiddle("sssss", 0, 20);
    }

    @Test
    public void testCompressMiddleFailed3() throws Exception {
        ex.expect(IllegalArgumentException.class);
        StrUtil.compressMiddle("sfsdfsd", 2, -1);
    }



}