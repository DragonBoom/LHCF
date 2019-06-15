package indi.crawler.monitor;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import indi.crawler.bootstrap.CrawlerJob;
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

    public class CloseableMonitorThread extends Thread {
        private long SLEEP_MILLIS = 5000L;
        private Date begin;

        public CloseableMonitorThread() {
            super("Closeable Monitor");
        }

        @Override
        public void run() {
            begin = new Date();
            
            try {
                Thread.sleep(2 * SLEEP_MILLIS);
            } catch (InterruptedException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }

            while (true) {
                try {
                    Thread.sleep(SLEEP_MILLIS);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                if (job.getController().getContextPool().isEmpty()) {
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
