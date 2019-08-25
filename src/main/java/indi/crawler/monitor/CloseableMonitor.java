package indi.crawler.monitor;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import indi.crawler.bootstrap.CrawlerJob;
import indi.crawler.task.CrawlerController;
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
public class CloseableMonitor {
    private CrawlerJob job;
    /**
     * 在结束进程前，将按默认顺序打印该Map的value
     */
    @Getter
    @Setter
    private static Map<String, String> toLogAtEndMap = new HashMap<>();

    private void init() {
        new CloseableMonitorThread().start();
        log.info("Start Closeable Monitor");
    }

    public CloseableMonitor(CrawlerJob job) {
        this.job = job;
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

    public class CloseableMonitorThread extends Thread {
        private long SLEEP_MILLIS = 50000L;
        private Date begin;

        public CloseableMonitorThread() {
            super("Closeable Monitor");
        }

        @Override
        public void run() {
            begin = new Date();
            
            try {
                Thread.sleep(2 * SLEEP_MILLIS);
            } catch (InterruptedException e) {
                throw new WrapperException(e);
            }

            while (true) {
                try {
                    Thread.sleep(SLEEP_MILLIS);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                CrawlerController controller = job.getController();
                CrawlerThreadPool threadPool = controller.getThreadPool();
                // 若爬虫池没有任务 并且 线程池没有线程正在工作，则结束任务
                if (controller.getTaskPool().isEmpty() && threadPool.isOver()) {
                    // 关闭线程池
                    threadPool.shutdown();
                    
                    Date now = new Date();
                    long millis = now.getTime() - begin.getTime();
                    log.info(
                            "## Closeable Monitor close the progress which had been run {} {} and use total memory {} mb .",
                            millis > 60000 ? millis / 60000 : millis / 1000,
                            millis > 60000 ? "minutes" : "seconds", Runtime.getRuntime().totalMemory() / 1024 / 1024);
                    
                    for (Map.Entry<String, String> e : toLogAtEndMap.entrySet()) {
                        log.info(e.getValue());
                    }
                    System.exit(0);
                }
            }
        }
    }
}
