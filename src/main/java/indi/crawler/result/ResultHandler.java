package indi.crawler.result;

import java.util.List;

import indi.crawler.nest.CrawlerContext;
import indi.crawler.task.SpecificTask;

public interface ResultHandler {

	/**
	 * 处理结果，返回具体任务
	 * @param ctx
	 * @return 具体任务队列，将按顺序执行
	 */
	public List<SpecificTask> process(CrawlerContext ctx) throws Exception;

}
