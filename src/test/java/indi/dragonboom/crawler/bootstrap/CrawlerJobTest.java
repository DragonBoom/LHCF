package indi.dragonboom.crawler.bootstrap;

import java.net.MalformedURLException;

import org.junit.jupiter.api.Test;

import indi.crawler.bootstrap.CrawlerJob;
import indi.crawler.result.ResultHandler;
import indi.crawler.task.def.TaskDef;

class CrawlerJobTest {

	@Test
	void startTest() throws MalformedURLException {
		CrawlerJob job = CrawlerJob.build();
		ResultHandler resultH = (c, helper) -> {
			System.out.println("request length: " + c.getRequest().getAllHeaders().length);

			helper.addNewTask("Rua", "https://www.baidu.com");
			return;
		};
		TaskDef task = TaskDef.Builder.begin("Rua").withKeepReceiveCookie()
				.withHTTPProxy("127.0.0.1", 1080).withLogDetail()
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
