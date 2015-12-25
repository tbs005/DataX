package com.alibaba.datax.common.util;

import org.hamcrest.core.StringContains;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class RetryUtilTest {

    private static String OK = "I am ok now.";

    private static String BAD = "I am bad now.";


    /**
     * 模拟一个不靠谱的方法，其不靠谱体现在：调用它，前2次必定失败，第3次才能成功. 运行成功时，输出为：I am ok now.
     * 运行报错时，报错中信息为：I am bad now.
     */
    static class SomeService implements Callable<String> {
        private int i = 0;

        @Override
        public String call() throws Exception {
            i++;
            if (i <= 2) {
                throw new Exception(BAD);
            }
            return OK;
        }
    }

    @Test(timeout = 3000L)
    public void test1() throws Exception {
        long startTime = System.currentTimeMillis();

        String result = RetryUtil.executeWithRetry(new SomeService(), 3, 1000L,
                false);
        long endTime = System.currentTimeMillis();
        Assert.assertEquals(result, OK);
        long executeTime = endTime - startTime;

        System.out.println("executeTime:" + executeTime);
        Assert.assertTrue(executeTime < 3 * 1000L);
    }

    @Test(timeout = 3000L)
    public void test2() throws Exception {
        long startTime = System.currentTimeMillis();
        String result = RetryUtil.executeWithRetry(new SomeService(), 4, 1000L,
                false);
        long endTime = System.currentTimeMillis();
        Assert.assertEquals(result, OK);
        long executeTime = endTime - startTime;

        System.out.println("executeTime:" + executeTime);
        Assert.assertTrue(executeTime < 3 * 1000L);
    }

    @Test(timeout = 3000L)
    public void test3() throws Exception {
        long startTime = System.currentTimeMillis();
        String result = RetryUtil.executeWithRetry(new SomeService(), 40,
                1000L, false);
        long endTime = System.currentTimeMillis();
        Assert.assertEquals(result, OK);
        long executeTime = endTime - startTime;

        System.out.println("executeTime:" + executeTime);
        Assert.assertTrue(executeTime < 3 * 1000L);
    }

    @Test(timeout = 4000L)
    public void test4() throws Exception {
        long startTime = System.currentTimeMillis();
        String result = RetryUtil.executeWithRetry(new SomeService(), 40,
                1000L, true);
        long endTime = System.currentTimeMillis();
        Assert.assertEquals(result, OK);
        long executeTime = endTime - startTime;

        System.out.println("executeTime:" + executeTime);
        Assert.assertTrue(executeTime < 4 * 1000L);
        Assert.assertTrue(executeTime > 3 * 1000L);
    }

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Test(timeout = 3000L)
    public void test5() throws Exception {
        expectedEx.expect(Exception.class);
        expectedEx.expectMessage(StringContains.containsString(BAD));

        RetryUtil.executeWithRetry(new SomeService(), 2, 100L, false);
    }

    /**
     * 线程池无法释放，后续提交被拒绝
     *
     * @throws Exception
     */
    @Test
    public void testExecutorService线程池占满() throws Exception {
        ThreadPoolExecutor executor = RetryUtil.createThreadPoolExecutor();
        expectedEx.expect(RejectedExecutionException.class);
        for (int i = 0; i < 10; i++) {
            executor.submit(new Callable<Object>() {
                @Override
                public Object call() throws Exception {
                    TimeUnit.SECONDS.sleep(10);
                    return null;
                }
            });
            System.out.println("Submit: " + i + ", running tasks: " + executor.getActiveCount());
        }

    }

    /**
     * 保持有任务运行，最多4个，所有提交过来的任务都能运行
     *
     * @throws Exception
     */
    @Test
    public void testExecutorService正常运行() throws Exception {
        ThreadPoolExecutor executor = RetryUtil.createThreadPoolExecutor();
        for (int i = 0; i < 10; i++) {
            executor.submit(new Callable<Object>() {
                @Override
                public Object call() throws Exception {
                    TimeUnit.SECONDS.sleep(4);
                    return null;
                }
            });
            System.out.println("Submit: " + i + ", running tasks: " + executor.getActiveCount());
            TimeUnit.SECONDS.sleep(1);
        }
    }

    /**
     * 线程池没有被全部占用，但是正在运行的总数超过限制，后续提交拒绝
     *
     * @throws Exception
     */
    @Test
    public void testExecutorService正在运行的总数超过限制() throws Exception {
        ThreadPoolExecutor executor = RetryUtil.createThreadPoolExecutor();
        expectedEx.expect(RejectedExecutionException.class);
        for (int i = 0; i < 10; i++) {
            executor.submit(new Callable<Object>() {
                @Override
                public Object call() throws Exception {
                    TimeUnit.SECONDS.sleep(6);
                    return null;
                }
            });
            System.out.println("Submit: " + i + ", running tasks: " + executor.getActiveCount());
            TimeUnit.SECONDS.sleep(1);
        }
    }

    @Test
    public void testExecutorService取消正在运行的任务() throws Exception {
        ThreadPoolExecutor executor = RetryUtil.createThreadPoolExecutor();
        List<Future<Object>> futures = new ArrayList<Future<Object>>(10);
        for (int i = 0; i < 10; i++) {
            Future<Object> f = executor.submit(new Callable<Object>() {
                @Override
                public Object call() throws Exception {
                    TimeUnit.SECONDS.sleep(6);
                    return null;
                }
            });
            futures.add(f);
            System.out.println("Submit: " + i + ", running tasks: " + executor.getActiveCount());

            if (i == 4) {
                for (Future<Object> future : futures) {
                    future.cancel(true);
                }
                System.out.println("Cancel all");
                System.out.println("Submit: " + i + ", running tasks: " + executor.getActiveCount());
            }

            TimeUnit.SECONDS.sleep(1);
        }
    }

    @Test
    public void testExecutorService取消方式错误() throws Exception {
        expectedEx.expect(RejectedExecutionException.class);

        ThreadPoolExecutor executor = RetryUtil.createThreadPoolExecutor();

        List<Future<Object>> futures = new ArrayList<Future<Object>>(10);
        for (int i = 0; i < 10; i++) {
            Future<Object> f = executor.submit(new Callable<Object>() {
                @Override
                public Object call() throws Exception {
                    TimeUnit.SECONDS.sleep(6);
                    return null;
                }
            });
            futures.add(f);
            System.out.println("Submit: " + i + ", running tasks: " + executor.getActiveCount());

            if (i == 4) {
                for (Future<Object> future : futures) {
                    future.cancel(false);
                }
                System.out.println("Cancel all");
            }

            TimeUnit.SECONDS.sleep(1);
        }
    }

    @Test
    public void testRetryAsync() throws Exception {
        ThreadPoolExecutor executor = RetryUtil.createThreadPoolExecutor();
        final AtomicInteger runCnt = new AtomicInteger();
        String res = RetryUtil.asyncExecuteWithRetry(new Callable<String>() {
            @Override
            public String call() throws Exception {
                runCnt.incrementAndGet();
                if (runCnt.get() < 3) {
                    TimeUnit.SECONDS.sleep(10);
                } else {
                    TimeUnit.SECONDS.sleep(1);
                }

                return OK;
            }
        }, 3, 1000L, false, 2000L, executor);
        Assert.assertEquals(res, OK);
//        Assert.assertEquals(RetryUtil.EXECUTOR.getActiveCount(), 0);
    }


    @Test
    public void testRetryAsync2() throws Exception {
        expectedEx.expect(TimeoutException.class);
        ThreadPoolExecutor executor = RetryUtil.createThreadPoolExecutor();
        String res = RetryUtil.asyncExecuteWithRetry(new Callable<String>() {
            @Override
            public String call() throws Exception {
                TimeUnit.SECONDS.sleep(10);
                return OK;
            }
        }, 3, 1000L, false, 2000L, executor);
    }

}
