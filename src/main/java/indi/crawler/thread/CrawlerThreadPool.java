package indi.crawler.thread;

import java.io.Closeable;
import java.util.LinkedList;

import indi.crawler.monitor.Monitor.MonitorThread;
import indi.crawler.task.CrawlerController;
import indi.exception.WrapperException;
import indi.obj.Message;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * 爬虫线程池
 * 
 * <p>2020.08.28 注意，该线程池并没有利用到父类ThreadGroup提供的方法！！
 * 
 * @author DragonBoom
 *
 */
@Slf4j
public class CrawlerThreadPool extends ThreadGroup implements Message, Closeable {
    private static final String DEFAULT_POOL_NAME = "CrawlerThreadPool";
    private static final int DEFAULT_POOL_SIZE = 10;
    private CrawlerController controller;
    @Getter
    private int targetSize;
    private LinkedList<Thread> threads = new LinkedList<>();
    private volatile boolean retire = false;// 结束任务的标记 

    private void init(CrawlerController controller, String poolName, int size) {
        this.controller = controller;
        controller.setThreadPool(this);
        this.targetSize = size;
        fullPool();
        // 启动线程池监视器
        new CrawlerThreadPoolMonitorThread().startDeamon(controller);
    }

    /**
     * 
     * @param controller
     * @param poolName
     * @param size 线程池大小
     */
    public CrawlerThreadPool(CrawlerController controller, String poolName, int size) {
        super(poolName);
        init(controller, poolName, size);
    }

    public CrawlerThreadPool(CrawlerController controller) {
        this(controller, DEFAULT_POOL_NAME, DEFAULT_POOL_SIZE);
    }

    private CrawlerThread createNewThread(String name) {
        return new CrawlerThread(this, name);// will set thread group
    }

    /**
     * 线程不安全，必须在单线程环境下执行！！
     * 
     * <p> FIXME: 当线程中止运行时，仍会被threads引用...
     */
    public synchronized void fullPool() {
        if (retire) {
            log.debug("爬虫已结束，不再更新爬虫线程池");
            return;
        }
        int j = 0;// 新增线程数
        for (int i = 0; i < targetSize; i++) {
            Thread t = null;
            // 新增条件：线程数量不足 || 线程不存在 || 线程无效
            if (i >= threads.size() || (t = threads.get(i)) == null || !t.isAlive()) {
                j++;
                // 是否需要先结束t？ FIXME:
                t = createNewThread("Crawler Thread - " + i);
                t.start();
                // 加入集合
                threads.add(t);
            }
        }
        if (j > 0) {
            log.info("已填充爬虫池  [{}/{}]", j, targetSize);
        }
    }

    /**
     * 结束线程池
     */
    @Override
    public void close() {
        // 标记已关闭，防止再新增线程
        retire = true;
        // 先结束线程
        for (Thread thread : threads) {
            CrawlerThread crawlerThread = (CrawlerThread) thread;
            log.info("开始结束线程{}", crawlerThread);
            crawlerThread.retire();
            crawlerThread.interrupt();// 用于中断爬虫的不可控状态（通过抛异常）
            log.info("结束线程{}完成", crawlerThread);
        }
        // 再结束线程池
        this.interrupt();
        log.info("爬虫线程池已结束");
    }

    public CrawlerController getController() {
        return controller;
    }
    
    public int getWorkingThreadCount() {
        int count = 0;
        for (Thread thread : threads) {
            if (((CrawlerThread) thread).isWorking()) {
                count++;
            }
        }
        return count;
    }
    
    /**
     * 判断是否所有线程都没在工作
     * 
     * @return
     */
    public boolean isOver() {
        return getWorkingThreadCount() == 0; 
    }

    @Override
    public String getMessage() {
        StringBuilder sb = new StringBuilder();
        return sb.toString();
    }

    private final Long DEFAULT_SCAN_WAITING = 10000L;
    
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
            // TODO Auto-generated constructor stub
        }



        @Override
        public void run0() {
            fullPool();
        }
    }
}