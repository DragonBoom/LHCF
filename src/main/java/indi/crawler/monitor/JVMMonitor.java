package indi.crawler.monitor;

import java.util.Objects;
import java.util.Optional;

import indi.crawler.task.CrawlerController;
import indi.crawler.task.CrawlerStatus;
import indi.crawler.task.Task;
import indi.crawler.task.TaskPool;
import indi.crawler.thread.CrawlerThread;
import lombok.extern.slf4j.Slf4j;

/**
 * 该类用于监视JVM内存，定时清理无法继续工作的出租中的爬虫
 * 
 * <p>该监视器默认启用
 *
 * <p>2021.12.10 这个类似乎只是定期清理爬虫而已；需要确保一定能回收爬虫
 */
@Slf4j
public class JVMMonitor extends Monitor {
    private static final int TOTAL_SIZE_LIMIT = 10;
    private static final long LOOP_MILLIS = 5000;
    private CrawlerController crawlerController;

    private void init() {
        new MemoryCollectorThread(TOTAL_SIZE_LIMIT, LOOP_MILLIS).startDeamon(crawlerController);
        
        log.info("Start JVM Monitor");
    }

    public JVMMonitor(CrawlerController crawlerController) {
        this.crawlerController = crawlerController;
        init();
    }

    private class MemoryCollectorThread extends MonitorThread {
        @Deprecated
        private int sizeThreshold;

        public MemoryCollectorThread(int sizeLimit, long loopMillis) {
            super("Memory Collector Thread", loopMillis);
            this.sizeThreshold = sizeLimit;
        }

        @Override
        public void run0() {
            // 通过外部类获取上下文池
            TaskPool pool = crawlerController.getTaskPool();
            int leasedSize = pool.getLeasedSize();
//            // 如果出租的上下文没有超过最低门槛，不尝试回收
//            if (leasedSize < sizeThreshold) {
//                return;
//            }
            // 开始处理出租的上下文
            // 获取出租上下文集合的副本
            Task[] leaseds = pool.cloneLeased();
            long now = System.currentTimeMillis();
            for (Task ctx : leaseds) {
                // 清空所有处于终端状态，无法继续执行的爬虫上下文
                CrawlerStatus status = ctx.getStatus();
                Objects.requireNonNull(status);
                switch (status) {
                case ABORTED:
                    log.debug("爬虫任务已被中止，将进行回收 {}", ctx.getMessage());
                    break;
                case FINISHED:
                    log.debug("爬虫任务已结束，但未主动回收。将由监视器进行回收 {}", ctx.getMessage());
                    break;
                case INTERRUPTED:
                    log.warn("爬虫任务无法处理，将进行回收 {}", ctx.getMessage());
                    break;
                case RUNNING:
                    // 校验是否正被线程执行
                    Task threadTask = 
                        Optional.ofNullable(ctx.getThread()).map(CrawlerThread::getCurrentContext).orElse(null);
                    if (!ctx.equals(threadTask)) {
                        log.warn("爬虫任务状态为RUNNING，但未被线程执行");
                        break;
                    }
                    // 校验最大执行时间
                    if (ctx.getLeasedTime() != -1 && ctx.getMaxLeasedTime() != -1 &&
                            now - ctx.getLeasedTime() > ctx.getMaxLeasedTime()) {
                        log.warn("爬虫任务已超过最大执行时间：{}", ctx.getMessage());
                        break;
                    }
                    
                    continue;// 不舍弃
                default:
                    log.warn("出租中的爬虫状态异常：{}", ctx.getMessage());
                }
                // 舍弃任务
                ctx.checkAndSetStatus(CrawlerStatus.ABORTED);
            }
//            if (cleanSize > 0) {
//                log.info("## JVM Monitor will clean [ {} / {} ] cralwer context", cleanSize, leasedSize);
////                    System.gc();// 没必要手动gc浪费cpu资源
//            }
//            else {
//                // 若没有清理爬虫上下文，则输出出租中的爬虫上下文的情况
//                log.info("No task complete during this time, still leaseds: {}", leasedSize);
//            }
        }
    }
}
