package indi.crawler.interceptor.http;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import indi.crawler.interceptor.InterceptorContext;
import indi.crawler.nest.CrawlerContext;
import indi.crawler.task.Task;
import indi.exception.WrapperException;
import lombok.extern.slf4j.Slf4j;

/**
 * 实现特定任务同步阻塞等待，即对同一类任务，每隔一段时间后才会执行下一个任务
 * 
 */
@Slf4j
public class SpecificTaskBlockingWaitInterceptor extends HttpInterceptor {
	private final Task task;
	private final long waitMillis;
	private volatile long lastOpen;
	private Lock lock;
	private LinkedBlockingQueue<CrawlerContext> todoQueue;

	private void init() {
		lock = new ReentrantLock();
		lock = new ReentrantLock();
		todoQueue = new LinkedBlockingQueue<>();
		
		new TimerThread().start();
	}

	/**
	 * 
	 * @param task
	 * @param waitMillis 每个间隔等待的时间
	 */
    public SpecificTaskBlockingWaitInterceptor(Task task, Long waitMillis) {
        this.task = task;
        this.waitMillis = waitMillis;
        init();
    }
	
    @Override
    public HandlerResult process(InterceptorContext iCtx) {
        // 
        if (iCtx.getCrawlerContext().getTask().equals(task)) {
            if (!lock.tryLock()) {
                // 若获取锁失败，说明有其他任务已经开始处理了，本任务只能暂缓执行
                todoQueue.add(iCtx.getCrawlerContext());
                return new HandlerResult();
            }
            // 此时已竞争到锁

            if (todoQueue.size() > 0) {
                todoQueue.add(iCtx.getCrawlerContext());
            }
            try {
                long now = System.currentTimeMillis();
                long needWaitMillis = lastOpen - now;
                lastOpen = now;
                // 若自上一个任务开始后，经过的时间还没有超过需要等待的时间，则等待完剩余时间
                if (needWaitMillis > 0) {
                    try {
                        Thread.sleep(needWaitMillis);
                    } catch (InterruptedException e) {
                        throw new WrapperException(e);
                    }
                }
            } finally {
                lock.unlock();
            }
        }
        // 等待结束，开始正式处理任务

        executeRequest(iCtx);
        receiveResponse(iCtx);
        afterReceiveResponse(iCtx);
        afterHandleResult(iCtx);

        HandlerResult result = new HandlerResult();
        return result;
    }
    
    /**
     * 线程类，定时检查是否已经超过等待间隔。超过的话，则尝试启动等待队列中爬虫任务。
     */
    public class TimerThread extends Thread {
        /**代表缓冲区间*/
        private long offset = 500;

        @Override
        public void run() {
            if (lastOpen + waitMillis + offset < System.currentTimeMillis()) {
                CrawlerContext ctx = todoQueue.poll();
                if (ctx != null) {
                    log.info("尝试启用阻塞的爬虫任务 {}", ctx);
                    ctx.getController().offer(ctx);
                }
            }
            try {
                Thread.sleep(waitMillis + offset);
            } catch (InterruptedException e) {
                log.error("{}", e);
            }
        }
    }
}
