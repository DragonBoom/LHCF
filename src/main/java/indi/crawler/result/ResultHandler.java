package indi.crawler.result;

import indi.crawler.task.Task;

public interface ResultHandler {

	/**
	 * 处理结果，返回具体任务
	 * @param ctx
	 * @return 具体任务队列，将按顺序执行
	 */
	public void process(Task ctx, ResultHelper helper) throws Exception;

}
