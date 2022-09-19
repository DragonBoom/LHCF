package indi.crawler.thread;

import java.io.Closeable;
import java.util.Iterator;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import indi.crawler.monitor.Monitor.MonitorThread;
import indi.crawler.task.CrawlerController;
import indi.exception.WrapperException;
import indi.obj.Message;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * 爬虫线程池
 * 
 * <p>2020.08.28 注意，该线程池并没有利用到父类ThreadGroup提供的方法，但在创建线程时将线程与ThreadGroup绑定起来
 * 
 * @author DragonBoom
 *
 */
@Slf4j
public class CrawlerThreadPool implements Message, Closeable {
    private CrawlerController controller;
    @Getter
    private int size;
    /** the queue to use for holding tasks before they are executed.
     *  
     * <p>用于存放准备执行的任务的队列
     */
    private LinkedBlockingQueue<Runnable> poolReadyQueue;
    /** 线程池中的线程，用于越过线程池操作线程 */
    private LinkedBlockingQueue<CrawlerThread> threads;
    private ThreadPoolExecutor executor;
    private volatile boolean retire = false;// 结束任务的标记 

    private void init(CrawlerController controller) {
        this.controller = controller;
        controller.setThreadPool(this);
        this.size = controller.getJob().getThreadCount();
        
        fullPool();
//        // 启动线程池监视器(改用Java线程池后弃用)
//        new CrawlerThreadPoolMonitorThread().startDeamon(controller);
    }

    /**
     * 
     */
    public CrawlerThreadPool(CrawlerController controller) {
        init(controller);
    }
    
    private CrawlerThread createNewThread(String name) {
        return new CrawlerThread(this, name);// will set thread group
    }
    
    /** 线程数 */
    private volatile int num = 0;

    /**
     * 线程不安全，必须在单线程环境下执行！！
     */
    public synchronized void fullPool() {
        if (retire) {
            log.debug("爬虫已结束，不再更新爬虫线程池");
            return;
        }
        if (poolReadyQueue == null) {
            poolReadyQueue = new LinkedBlockingQueue<>();
            threads = new LinkedBlockingQueue<>();
            executor = new ThreadPoolExecutor(size, size, 0L, TimeUnit.MILLISECONDS, poolReadyQueue);
        }
        int size = this.size;
        int count = getWorkCount();
        // 移除退休任务
        Iterator<CrawlerThread> iterator = threads.iterator();
        while (iterator.hasNext()) {
            CrawlerThread t = iterator.next();
            if (t.isRetire()) {
                iterator.remove();
            }
        }
        // 补充缺少的任务
        int j = 0;// 新增线程数
        while (count < size) {
            CrawlerThread t = createNewThread("Crawler Thread - " + num++);
            threads.add(t);
            executor.submit(t);
            count++;
            j++;
        }
        if (j > 0) {
            log.info("已填充爬虫池  [{}/{}]", j, size);
        }
    }

    /**
     * 结束线程池
     */
    @Override
    public void close() {
        retire = true;
        log.info("开始结束爬虫线程池");
        executor.shutdown();// 不创建新线程
        // 主动结束线程
        for (CrawlerThread t : threads) {
            t.retire();
        }
        // 等待线程池结束
        try {
            while (!executor.awaitTermination(1, TimeUnit.SECONDS)) {
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new WrapperException(e);
        }
        log.info("爬虫线程池已结束");
    }

    public CrawlerController getController() {
        return controller;
    }
    
    public int getWorkCount() {
        return executor.getActiveCount();
    }

    @Override
    public String getMessage() {
        StringBuilder sb = new StringBuilder();
        return sb.toString();
    }

    private static final Long DEFAULT_SCAN_WAITING = 10000L;
    
    /**
     * 定时监视线程池，进行补充线程等工作
     * 
     * @author DragonBoom
     *
     */
    public class CrawlerThreadPoolMonitorThread extends MonitorThread {
        
        
        private CrawlerThreadPoolMonitorThread() {
            super(DEFAULT_SCAN_WAITING);
        }

        private CrawlerThreadPoolMonitorThread(String name, Long sleepMillis) {
            super(name, sleepMillis);
        }

        @Override
        public void run0() {
            fullPool();
        }
    }
}