package com.alibaba.datax.common.util;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Random;

public class RangeSplitUtilTest {

    @Test
    public void testSplitString() {
        int expectSliceNumber = 3;

        String left = "00468374-8cdb-11e4-a66a-008cfac1c3b8";
        String right = "fcbc8a79-8427-11e4-a66a-008cfac1c3b8";

        String[] result = RangeSplitUtil.doAsciiStringSplit(left, right, expectSliceNumber);

        Assert.assertTrue(result.length - 1 == expectSliceNumber);
        System.out.println(Arrays.toString(result));
    }

    @Test
    public void testSplitStringRandom() {
        String left = RandomStringUtils.randomAlphanumeric(40);
        String right = RandomStringUtils.randomAlphanumeric(40);

        for (int expectSliceNumber = 1; expectSliceNumber < 100; expectSliceNumber++) {
            String[] result = RangeSplitUtil.doAsciiStringSplit(left, right, expectSliceNumber);

            Assert.assertTrue(result.length - 1 == expectSliceNumber);

            String[] clonedResult = result.clone();
//            Collections.sort(Arrays.asList(result));

            Assert.assertTrue(Arrays.toString(clonedResult).equals(Arrays.toString(result)));

            System.out.println(result);
        }
    }

    //TODO
    @Test
    public void testLong_00() {
        long count = 0;
        long left = 0;
        long right = count - 1;
        int expectSliceNumber = 3;
        long[] result = RangeSplitUtil.doLongSplit(left, right, expectSliceNumber);

        result[result.length - 1]++;
        for (int i = 0; i < result.length - 1; i++) {
            System.out.println("start:" + result[i] + " count:" + (result[i + 1] - result[i]));
        }

//        Assert.assertTrue(result.length - 1 == expectSliceNumber);
        System.out.println(Arrays.toString(result));
    }

    @Test
    public void testLong_01() {
        long count = 8;
        long left = 0;
        long right = count - 1;
        int expectSliceNumber = 3;
        long[] result = RangeSplitUtil.doLongSplit(left, right, expectSliceNumber);

        result[result.length - 1]++;
        for (int i = 0; i < result.length - 1; i++) {
            System.out.println("start:" + result[i] + " count:" + (result[i + 1] - result[i]));
        }

        Assert.assertTrue(result.length - 1 == expectSliceNumber);
        System.out.println(Arrays.toString(result));
    }

    @Test
    public void testLong() {
        long left = 8L;
        long right = 301L;
        int expectSliceNumber = 93;
        doTest(left, right, expectSliceNumber);

        for (int i = 1; i < right * 20; i++) {
            doTest(left, right, i);
        }

        System.out.println(" 测试随机值...");
        int testTimes = 200;
        for (int i = 0; i < testTimes; i++) {
            left = getRandomLong();
            right = getRandomLong();
            expectSliceNumber = getRandomInteger();
            doTest(left, right, expectSliceNumber);
        }

    }


    @Test
    public void testGetMinAndMaxCharacter() {
        Pair<Character, Character> result = RangeSplitUtil.getMinAndMaxCharacter("abc%^&");
        Assert.assertEquals('%', result.getLeft().charValue());
        Assert.assertEquals('c', result.getRight().charValue());

        result = RangeSplitUtil.getMinAndMaxCharacter("\tAabcZx");
        Assert.assertEquals('\t', result.getLeft().charValue());
        Assert.assertEquals('x', result.getRight().charValue());
    }


    //TODO 自动化测试
    @Test
    public void testDoAsciiStringSplit() {
//        String left = "adde";
//        String right = "xyz";
//        int expectSliceNumber = 4;
        String left = "a";
        String right = "z";
        int expectSliceNumber = 3;

        String[] result = RangeSplitUtil.doAsciiStringSplit(left, right, expectSliceNumber);
        System.out.println(ToStringBuilder.reflectionToString(result, ToStringStyle.SIMPLE_STYLE));

    }

    private long getRandomLong() {
        Random r = new Random();
        return r.nextLong();
    }

    private int getRandomInteger() {
        Random r = new Random();
        return Math.abs(r.nextInt(1000) + 1);
    }

    private void doTest(long left, long right, int expectSliceNumber) {
        long[] result = RangeSplitUtil.doLongSplit(left, right, expectSliceNumber);

        System.out.println(String.format("left:[%s],right:[%s],expectSliceNumber:[%s]====> splitResult:[\n%s\n].\n",
                left, right, expectSliceNumber, ToStringBuilder.reflectionToString(result, ToStringStyle.SIMPLE_STYLE)));

        Assert.assertTrue(doCheck(result, left, right, Math.abs(right - left) >
                expectSliceNumber ? expectSliceNumber : -1));
    }


    private boolean doCheck(long[] result, long left,
                            long right, int expectSliceNumber) {
        if (null == result) {
            throw new IllegalArgumentException("parameter result can not be null.");
        }

        // 调整大小顺序，确保 left<right
        if (left > right) {
            long temp = left;
            left = right;
            right = temp;
        }

        //为了方法共用，expectSliceNumber == -1 表示不对切分份数进行校验.
        boolean skipSliceNumberCheck = expectSliceNumber == -1;
        if (skipSliceNumberCheck || expectSliceNumber == result.length - 1) {
            boolean leftCheckOk = left == result[0];
            boolean rightCheckOk = right == result[result.length - 1];

            if (leftCheckOk && rightCheckOk) {
                for (int i = 0, len = result.length; i < len - 1; i++) {
                    if (result[i] > result[i + 1]) {
                        return false;
                    }
                }
                return true;
            }
        }

        return false;
    }
}
