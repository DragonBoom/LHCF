package indi.crawler.monitor;

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import indi.crawler.bootstrap.CrawlerJob;
import indi.crawler.task.CrawlerController;
import indi.crawler.task.TaskPool;
import indi.crawler.thread.CrawlerThreadPool;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

/**
 * 检测是否所有爬虫任务都已完成或不可完成，若是则结束工程
 * 
 */
@Slf4j
public class CloseableMonitor extends Monitor {
    /**
     * 检验是否结束工程的频率。
     * 默认取3s，频率应该尽可能的高，以避免即使只要下载很少的量也要等待很久
     */
    private static final long SLEEP_MILLIS = 3000L;
    private CrawlerJob job;
    private CrawlerController controller;
    
    private Date beginTime;
    /**
     * 在结束进程前，将按默认顺序打印该Map的value
     */
    @Getter
    @Setter
    private static Map<String, String> toLogAtEndMap = new HashMap<>();
    private static List<Supplier<String>> logFunList = new LinkedList<>();

    private void init(CrawlerJob job) {
        this.job = job;
        this.controller = job.getController();
        new CloseableMonitorThread().startNotDeamon(job.getController());
        beginTime = new Date();
        log.info("Start Closeable Monitor");
    }

    public CloseableMonitor(CrawlerJob job) {
        init(job);
    }
    
    /**
     * 添加在爬虫任务结束时输出至日志的内容
     * 
     * @param key 日志内容的key，用于对待输入的日志做标记
     * @param msg 日志内容
     */
    public static void addLogAtEnd(String key, String msg) {
        toLogAtEndMap.put(key, msg);
    }
    
    /**
     * 以注册日志supplier的形式注册在爬虫任务结束时输出的日志
     * 
     * @param logFun
     * @since 2021.04.10
     */
    public static void registLog(Supplier<String> logFun) {
        logFunList.add(logFun);
    }
    
    /***
     * 结束Job
     */
    public void close() {
        log.info("crawler job completed, begin close all crawler");
        // 结束工程
        controller.close();
        
        Date now = new Date();
        long millis = now.getTime() - beginTime.getTime();
        log.info(
                "## Closeable Monitor close the progress which had been run {} {} and use total memory {} mb .",
                millis > 60000 ? millis / 60000 : millis / 1000,
                millis > 60000 ? "minutes" : "seconds", Runtime.getRuntime().totalMemory() / 1024 / 1024);
        printLog();
        // 执行设置的回调
        log.info("正在结束爬虫-执行结束回调：");
        Optional.ofNullable(job.getCloseCallbackFun()).ifPresent(Runnable::run);
    }
    
    private void printLog() {
        for (Map.Entry<String, String> e : toLogAtEndMap.entrySet()) {
            log.info(e.getValue());
        }
        for (Supplier<String> supplier : logFunList) {
            log.info(supplier.get());
        }
    }

    public class CloseableMonitorThread extends MonitorThread {

        public CloseableMonitorThread() {
            super("Closeable Monitor", SLEEP_MILLIS);
        }
        
        /** 连续检测到爬虫池毫无进展的次数 */
        private volatile int emptyTimes = 0;
        /** 当检测到毫无进展的次数超过该阈值时，需要记录日志 */
        private final Integer defaultWarnTimes = 5;

        /**
         * {@inheritDoc}
         * 
         * <p>单线程执行，不需要考虑线程安全问题
         */
        @Override
        @SneakyThrows
        public void run0() {
            TaskPool taskPool = controller.getTaskPool();
            CrawlerThreadPool threadPool = controller.getThreadPool();
            if (taskPool == null || threadPool == null) {
                return;
            }
            // 爬虫池没有任务
            if (taskPool.isEmpty()) {
                // 等待10s后再次尝试
                TimeUnit.SECONDS.sleep(10);
                if (taskPool.isEmpty()) {
                    // 结束爬虫任务
                    close();
                }
            } else {
                // 尝试打印日志
                printLog();
            }
        }
        
        Object[] lastArgs = new Object[] {}; 
        
        public void printLog() {
            TaskPool taskPool = controller.getTaskPool();
            
            // 对以下参数，若连续n次都检测到其中存在为0的，就输出日志
            int availableSize = taskPool.availableSize();
            int leasedSize = taskPool.getLeasedSize();
            int deferralSize = taskPool.deferralSize();
            int workableSize = taskPool.workableSize();
            
            if (availableSize == 0 || leasedSize == 0) {
                emptyTimes++;
            } else {
                emptyTimes = 0;// 一旦有进展就清0
            }
            if (emptyTimes > defaultWarnTimes) {// 各数值为空一定次数后再输出日志
                Object[] args = new Object[] { availableSize, leasedSize, deferralSize, workableSize, emptyTimes,
                        defaultWarnTimes};
                // 2020.09.03 相较于上次有变化时才重新输出日志
                if (!Arrays.equals(lastArgs, args)) {
                    log.info(
                            "Available task count = {}, leased task count = {}, deferral count = {}, "
                                    + "workable task count = {}, emptyTimes/warnTimes = {}/{}",
                                    args);
                    lastArgs = args;
                }
                emptyTimes = 0;// 重置次数
            }
        }
    }
}
