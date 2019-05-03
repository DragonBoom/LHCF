package indi.crawler.monitor;

import indi.crawler.bootstrap.CrawlerJob;
import indi.crawler.nest.ContextPool;
import lombok.extern.slf4j.Slf4j;

/**
 * 该类用于监视上下文池
 *
 */
@Slf4j
public class ContextPoolMonitor {
	private CrawlerJob job;

	private void init(long millis) {
		ContextPoolMonitorThread t = new ContextPoolMonitorThread(millis);
		t.start();
		log.info("Start Context Pool Monitor");
	}

	public ContextPoolMonitor(CrawlerJob job, long millis) {
		this.job = job;
		init(millis);
	}

	private class ContextPoolMonitorThread extends Thread {
	    private long millis;

		public ContextPoolMonitorThread(long millis) {
			super("Context Pool Monitor Thread");
			this.millis = millis;
		}

		@Override
		public void run() {
			ContextPool pool = job.getController().getContextPool();
			while (true) {// TODO ? retire?
				try {
					Thread.sleep(millis);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				log.info(pool.getMessage());
			}
		}
	}

}
