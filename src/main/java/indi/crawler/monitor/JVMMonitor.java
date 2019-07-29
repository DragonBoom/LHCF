package indi.crawler.monitor;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

import indi.crawler.task.CrawlerController;
import indi.crawler.task.CrawlerStatus;
import indi.crawler.task.Task;
import indi.crawler.task.TaskPool;
import indi.exception.WrapperException;
import lombok.extern.slf4j.Slf4j;

/**
 * 该类用于监视JVM内存，定时清理无法继续工作的出租中的爬虫
 * 
 * <p>该监视器默认启用
 *
 */
@Slf4j
public class JVMMonitor {
    private static final int TOTAL_SIZE_LIMIT = 10;
    private static final long LOOP_MILLIS = TimeUnit.MINUTES.toMillis(5);
    private CrawlerController crawlerController;
    private MemoryCollectorThread memoryCollectorThread;

    private void init() {
        memoryCollectorThread = new MemoryCollectorThread(TOTAL_SIZE_LIMIT, LOOP_MILLIS);
        memoryCollectorThread.setDaemon(true);
        memoryCollectorThread.start();
        log.info("Start JVM Monitor");
    }

    public JVMMonitor(CrawlerController crawlerController) {
        this.crawlerController = crawlerController;
        init();
    }

    public void retire() {
        memoryCollectorThread.retire();
    }

    private class MemoryCollectorThread extends Thread {
        private int sizeThreshold;
        private long loopMillis;
        private volatile boolean retire = false;

        public MemoryCollectorThread(int sizeLimit, long loopMillis) {
            super("Memory Collector Thread");
            this.sizeThreshold = sizeLimit;
            this.loopMillis = loopMillis;
        }

        public void retire() {
            retire = true;
        }

        @Override
        public void run() {
            while (!retire) {
                try {
                    Thread.sleep(loopMillis);
                } catch (InterruptedException e) {
                    throw new WrapperException(e);
                }
                // 通过外部类获取上下文池
                TaskPool pool = crawlerController.getTaskPool();
                // 如果出租的上下文没有超过最低门槛，则多等待一个回合
                int leasedSize = pool.getLeasedSize();
                if (leasedSize < sizeThreshold) {
                    try {
                        Thread.sleep(loopMillis);
                    } catch (InterruptedException e) {
                        throw new WrapperException(e);
                    }
                }
                // 开始处理出租的上下文
                // 获取出租上下文集合的副本
                Task[] leaseds = pool.cloneLeased();
                int cleanSize = 0;
                for (Task ctx : leaseds) {
                    // 清空所有处于终端状态，无法继续执行的爬虫上下文
                    CrawlerStatus status = ctx.getStatus();
                    Objects.requireNonNull(status);
                    if (status == CrawlerStatus.FINISHED || status == CrawlerStatus.ABORTED
                            || status == CrawlerStatus.INTERRUPTED) {
                        switch (status) {
                        case ABORTED:
                            log.info("爬虫任务已被中止，将进行回收 {}", ctx);
                            break;
                        case FINISHED:
                            log.debug("爬虫任务已结束，但未主动回收。将由监视器进行回收 {}", ctx);
                            break;
                        case INTERRUPTED:
                            log.warn("爬虫任务无法处理，将进行回收 {}", ctx);
                            break;
                        default:
                            break;
                        }
                        ctx.cleanup();
                        pool.removeLeased(ctx);
                        cleanSize++;
                    }
                }
                if (cleanSize > 0) {
                    // 若有清理爬虫上下文
                    log.debug("## JVM Monitor will clean [ {} / {} ] cralwer context", cleanSize,
                            leasedSize);
                    System.gc();
                } else {
                    // 若没有清理爬虫上下文，则输出出租中的爬虫上下文的情况
                    log.debug("No task complete during this time, still leaseds: {}", leasedSize);
                    leaseds = null; // for gc !!!
                }
            }
        }
    }
}
