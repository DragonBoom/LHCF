package indi.crawler.processor.http;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import indi.crawler.task.Task;
import indi.crawler.task.def.TaskDef;
import lombok.extern.slf4j.Slf4j;

/**
 * FIXME: 实现特定任务同步阻塞等待，即对同一类任务，每隔一段时间后才会执行下一个任务
 * 
 */
@Slf4j
public class SpecificTaskBlockingWaitProcessor extends HttpProcessor {
	private final TaskDef task;
	private final long waitMillis;
	private volatile long lastOpen;
	private Lock lock;
	private LinkedBlockingQueue<Task> todoQueue;

	private void init() {
		lock = new ReentrantLock();
		lock = new ReentrantLock();
		todoQueue = new LinkedBlockingQueue<>();
	}

	/**
	 * 
	 * @param task
	 * @param waitMillis 每个间隔等待的时间
	 */
    public SpecificTaskBlockingWaitProcessor(TaskDef task, Long waitMillis) {
        this.task = task;
        this.waitMillis = waitMillis;
        init();
    }
	

}
