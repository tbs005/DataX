package com.alibaba.datax.common.util;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class ListUtilTest {
    private static List<String> aList = null;

    @BeforeClass
    public static void beforeClass() {
        aList = new ArrayList<String>();
        aList.add("one");
        aList.add("onE");
        aList.add("two");
        aList.add("阿里巴巴");
    }

    @Test
    public void testCheckIfValueDuplicate() {
        List<String> list = new ArrayList<String>(aList);
        list.add(aList.get(0));
        boolean result = ListUtil.checkIfValueDuplicate(list, true);
        Assert.assertTrue(list + " has no duplicate value.", result);

        list = new ArrayList<String>(aList);
        list.add(aList.get(0));
        result = ListUtil.checkIfValueDuplicate(list, false);
        Assert.assertTrue(list + " has duplicate value.", result);


        list = new ArrayList<String>(aList);
        list.add(aList.get(0));
        list.set(list.size() - 1, list.get(list.size() - 1).toUpperCase());
        result = ListUtil.checkIfValueDuplicate(list, true);
        Assert.assertTrue(list + " has duplicate value.", result == false);

        list = new ArrayList<String>(aList);
        list.add(aList.get(0));
        list.set(list.size() - 1, list.get(list.size() - 1).toUpperCase());
        result = ListUtil.checkIfValueDuplicate(list, false);
        Assert.assertTrue(list + " has duplicate value.", result);
    }

    @Test
    public void testValueToLowerCase() {
        List<String> list = new ArrayList<String>(aList);
        for (int i = 0, len = list.size(); i < len; i++) {
            list.set(i, list.get(i).toLowerCase());
        }

        Assert.assertArrayEquals(list.toArray(), ListUtil.valueToLowerCase(list).toArray());
    }

    @Test
    public void testCheckIfValueSame() {
        List<Boolean> boolList = new ArrayList<Boolean>();
        boolList.add(true);
        boolList.add(true);
        boolList.add(true);
        Assert.assertTrue(boolList + " all value same.", ListUtil.checkIfValueSame(boolList));

        boolList.add(false);
        Assert.assertTrue(boolList + "not all value same.", ListUtil.checkIfValueSame(boolList) == false);
    }

    @Test
    public void testCheckIfBInA() {
        List<String> bList = new ArrayList<String>(aList);
        bList.set(0, bList.get(0) + "_hello");
        Assert.assertTrue(bList + " not all in " + aList, ListUtil.checkIfBInA(aList, bList, false) == false);

        Assert.assertTrue(bList + " not all in " + aList, ListUtil.checkIfBInA(aList, bList, true) == false);


        bList = new ArrayList<String>(aList);
        bList.set(0, bList.get(0).toUpperCase());
        Assert.assertTrue(bList + " all in " + aList, ListUtil.checkIfBInA(aList, bList, false));


        bList = new ArrayList<String>(aList);
        bList.set(0, bList.get(0).toUpperCase());
        Assert.assertTrue(bList + " not all in " + aList, ListUtil.checkIfBInA(aList, bList, true) == false);

    }

}
