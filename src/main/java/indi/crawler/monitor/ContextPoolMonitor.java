package indi.crawler.monitor;

import indi.crawler.bootstrap.CrawlerJob;
import indi.crawler.task.TaskPool;
import lombok.extern.slf4j.Slf4j;

/**
 * 爬虫上下文池监视器。目前的功能：
 * 
 * <li>定时打印爬虫池的信息
 *
 */
@Slf4j
public class ContextPoolMonitor extends Monitor {
	private CrawlerJob job;
	private TaskPool pool;

	private void init(long millis) {
		new ContextPoolMonitorThread(millis).startDeamon(job.getController());
		log.info("Start Context Pool Monitor");
	}

	public ContextPoolMonitor(CrawlerJob job, long millis) {
		this.job = job;
		pool = job.getController().getTaskPool();
		init(millis);
	}

	private class ContextPoolMonitorThread extends MonitorThread {

		public ContextPoolMonitorThread(long millis) {
			super("Context Pool Monitor Thread", millis);
		}

		@Override
		public void run0() {
		    log.info(new StringBuilder("## Context Pool Monitor: ").append(pool.getMessage()).toString());
		}
	}

}
