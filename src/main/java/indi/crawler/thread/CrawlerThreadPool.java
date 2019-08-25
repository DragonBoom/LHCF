package indi.crawler.thread;

import java.util.LinkedList;

import indi.crawler.task.CrawlerController;
import indi.exception.WrapperException;
import indi.util.Message;
import lombok.extern.slf4j.Slf4j;

/**
 * 爬虫线程池
 * <p>
 * 
 * @author DragonBoom
 *
 */
@Slf4j
public class CrawlerThreadPool extends ThreadGroup implements Message {
    private static final String DEFAULT_POOL_NAME = "CrawlerThreadPool";
    private static final int DEFAULT_POOL_SIZE = 10;
    private CrawlerController controller;
    private int size;
    private LinkedList<Thread> threads = new LinkedList<>();

    private void init(CrawlerController controller, String poolName, int size) {
        this.controller = controller;
        controller.setThreadPool(this);
        this.size = size;
        fullPool();
        // 启动线程池监视器
        new CrawlerThreadPoolMonitorThread().start(); // TODO
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

    private void addNewThread(String name) {
        CrawlerThread t = new CrawlerThread(this, name);// will set thread group
        t.start();// add 2 group when start
        threads.add(t);
    }

    /**
     * 线程不安全，必须在单线程环境下执行！！
     * 
     * <p> FIXME: 当线程中止运行时，仍会被threads引用...
     */
    public synchronized void fullPool() {
        int j = 0;
        for (int i = 0; i < size; i++) {
            Thread t = null;
            if (i >= threads.size() || (t = threads.get(i)) == null || !t.isAlive()) {
                j++;
                addNewThread("Crawler Thread - " + i);
            }
        }
        if (j > 0) {
            log.info("已填充爬虫池  [{}/{}]", j, size);
        }
    }

    private void killThread(CrawlerThread thread, boolean immediate) {
        if (immediate) {
            // TODO thread.interrupt();
        }
        thread.retire();
    }

    public void shutdown() {
        for (Thread thread : threads) {
            CrawlerThread crawlerThread = (CrawlerThread) thread;
            crawlerThread.retire();
            crawlerThread.interrupt();
            log.info("结束线程{}", crawlerThread);
        }
    }

    public CrawlerController getController() {
        return controller;
    }

    public int getSize() {
        return size;
    }
    
    public int getWorkingCount() {
        int count = 0;
        for (Thread thread : threads) {
            if (((CrawlerThread) thread).isWorking()) {
                count++;
            }
        }
        return count;
    }
    
    public boolean isOver() {
        return getWorkingCount() == 0; 
    }

    @Override
    public String getMessage() {
        StringBuilder sb = new StringBuilder();
        return sb.toString();
    }

    /**
     * 定时监视线程池，进行补充线程等工作
     * 
     * @author DragonBoom
     *
     */
    public class CrawlerThreadPoolMonitorThread extends Thread {
        private final Long DEFAULT_SCAN_WAITING = 10000L;

        @Override
        public void run() {
            while (true) {
                try {
                    Thread.sleep(DEFAULT_SCAN_WAITING);
                    fullPool();
                } catch (InterruptedException e) {
                    throw new WrapperException(e);
                }
            }
        }
    }
}
