package com.alibaba.datax.plugin.writer.otswriter.model;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OTSBlockingExecutor {
    private final ExecutorService exec;
    private final Semaphore semaphore;
    
    private static final Logger LOG = LoggerFactory.getLogger(OTSBlockingExecutor.class);

    public OTSBlockingExecutor(int concurrency) {
        this.exec = new ThreadPoolExecutor(
                concurrency, concurrency,  
                0L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>());
        this.semaphore = new Semaphore(concurrency);
    }

    public void execute(final Runnable task)
            throws InterruptedException {
        LOG.debug("Begin execute");
        try {
            semaphore.acquire();
            exec.execute(new Runnable() {
                public void run() {
                    try {
                        task.run();
                    } finally {
                        semaphore.release();
                    }
                }
            });
        } catch (RejectedExecutionException e) {
            semaphore.release();
            throw new RuntimeException(OTSErrorMessage.INSERT_TASK_ERROR);
        }
        LOG.debug("End execute");
    }
    
    public void shutdown() throws InterruptedException {
        this.exec.shutdown();
        while (!this.exec.awaitTermination(1, TimeUnit.SECONDS)){} 
    }
}