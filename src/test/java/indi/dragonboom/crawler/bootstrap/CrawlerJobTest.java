package indi.dragonboom.crawler.bootstrap;

import java.net.MalformedURLException;
import java.util.LinkedList;
import java.util.List;

import org.junit.jupiter.api.Test;

import indi.crawler.bootstrap.CrawlerJob;
import indi.crawler.result.ResultHandler;
import indi.crawler.task.SpecificTask;
import indi.crawler.task.Task;

class CrawlerJobTest {

	@Test
	void startTest() throws MalformedURLException {
		CrawlerJob job = new CrawlerJob();
		ResultHandler resultH = (c) -> {
			System.out.println("request length: " + c.getRequest().getAllHeaders().length);

			List<SpecificTask> ctxs = new LinkedList<>();
			ctxs.add(new SpecificTask("Rua", "https://www.baidu.com", null));
			return ctxs;
		};
		Task task = Task.Builder.begin("Rua").withKeepReceiveCookie()
				.withHTTPProxy("127.0.0.1", 1080).withBlockingWait(3000L).withLogDetail()
				.withResultHandler(resultH).build();
		job.register(task);
		job.start("Rua", "https://www.baidu.com", null);
		while (true) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

}
