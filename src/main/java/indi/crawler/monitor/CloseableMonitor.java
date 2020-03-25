package indi.crawler.monitor;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import indi.crawler.bootstrap.CrawlerJob;
import indi.crawler.task.CrawlerController;
import indi.crawler.task.TaskPool;
import indi.crawler.thread.CrawlerThreadPool;
import indi.exception.WrapperException;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * 检测是否所有爬虫任务都已完成或不可完成，若是则结束进程
 * 
 */
@Slf4j
public class CloseableMonitor extends Monitor {
    private CrawlerJob job;
    private CrawlerController controller;
    
    private Date beginTime;
    /**
     * 在结束进程前，将按默认顺序打印该Map的value
     */
    @Getter
    @Setter
    private static Map<String, String> toLogAtEndMap = new HashMap<>();

    private void init() {
        new CloseableMonitorThread().startNotDeamon(job.getController());
        beginTime = new Date();
        log.info("Start Closeable Monitor");
    }

    public CloseableMonitor(CrawlerJob job) {
        this.job = job;
        this.controller = job.getController();
        init();
    }
    
    /**
     * 添加在爬虫任务结束时记录的日志内容
     * 
     * @param key 日志内容的key，用于对待输入的日志做标记
     * @param msg 日志内容
     */
    public static void addLogAtEnd(String key, String msg) {
        toLogAtEndMap.put(key, msg);
    }
    
    public void close() {
        log.info("begin close all crawler");
        CrawlerThreadPool threadPool = controller.getThreadPool();
        // 关闭线程池
        threadPool.close();
        // 结束守护线程
        controller.retireMonitorThreads();
        
        Date now = new Date();
        long millis = now.getTime() - beginTime.getTime();
        log.info(
                "## Closeable Monitor close the progress which had been run {} {} and use total memory {} mb .",
                millis > 60000 ? millis / 60000 : millis / 1000,
                millis > 60000 ? "minutes" : "seconds", Runtime.getRuntime().totalMemory() / 1024 / 1024);
        
        for (Map.Entry<String, String> e : toLogAtEndMap.entrySet()) {
            log.info(e.getValue());
        }
        
        // 执行设置的回调
        log.info("正在结束爬虫-执行结束回调：");
        Optional.ofNullable(job.getCloseCallbackFun()).ifPresent(runnable -> runnable.run());
    }

    public class CloseableMonitorThread extends MonitorThread {
        private long SLEEP_MILLIS = 3000L;// 3s，频率应该尽可能的高，以避免即使只要下载很少的量也要等待很久

        public CloseableMonitorThread() {
            super("Closeable Monitor");
        }
        
        private volatile int emptyTimes = 0;
        private final Integer DEFAULT_WARN_TIMES = 5;

        @Override
        public void run() {
            try {
                Thread.sleep(2 * SLEEP_MILLIS);
            } catch (InterruptedException e) {
                throw new WrapperException(e);
            }

            while (!retire) {
                try {
                    Thread.sleep(SLEEP_MILLIS);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                TaskPool taskPool = controller.getTaskPool();
                CrawlerThreadPool threadPool = controller.getThreadPool();
                // 若爬虫池没有任务 并且 线程池没有线程正在工作，则结束任务
                if (taskPool.isEmpty() && threadPool.isOver()) {
                    // 结束爬虫任务
                    close();
                } else {
                    // 尝试打印日志
                    printLog();
                }
            }
        }
        
        
        public void printLog() {
            CrawlerThreadPool threadPool = controller.getThreadPool();
            TaskPool taskPool = controller.getTaskPool();
            // 对以下参数，若连续n次都检测到其中存在为0的，就输出日志
            int availableSize = taskPool.availableSize();
            int leasedSize = taskPool.getLeasedSize();
            int workingCount = threadPool.getWorkingCount();
            
            if (availableSize == 0 || leasedSize == 0 || workingCount == 0) {
                emptyTimes++;
            } else {
                emptyTimes = 0;
            }
            if (emptyTimes > DEFAULT_WARN_TIMES) {
                log.info("Available task count = {}, leased task count = {}, workable thread count = {}",
                        taskPool.availableSize(), taskPool.getLeasedSize(), threadPool.getWorkingCount());
            }
        }
    }
}
