package indi.crawler.thread;

import java.util.concurrent.TimeUnit;

import indi.crawler.exception.AbortTaskException;
import indi.crawler.task.CrawlerController;
import indi.crawler.task.CrawlerStatus;
import indi.crawler.task.Task;
import indi.obj.Message;
import indi.thread.BasicThread;
import lombok.Getter;

/**
 * 爬虫线程类，不包含具体工作的逻辑
 * 
 * @author DragonBoom
 *
 */
public class CrawlerThread extends BasicThread implements Message {
    private CrawlerController controller;
    @Getter
    private volatile boolean retire;
    private Task currentTask;
    private int workNumber;

    private void init(CrawlerThreadPool pool) {
        controller = pool.getController();
    }

    public CrawlerThread(CrawlerThreadPool pool, String threadName) {
        super(pool, threadName);
        init(pool);
    }

    @Override
    public void run() {
        while (!retire) {
            try {
                // 领取爬虫任务
                Task ctx = null;
                ctx = controller.poll();
                // 若没有领取到任务，开始休息
                if (ctx == null) {
                    TimeUnit.SECONDS.sleep(2);// 休息2s
                    continue;
                }
                currentTask = ctx;
                // 执行任务 
                controller.process(ctx);
                
                workNumber++;
                currentTask = null;
            } catch (InterruptedException e) {
                // 判断是否为领取不到任务后休息时被中断
                if (currentTask == null) {
                    // donothing
                } else {
                    e.printStackTrace();
                }
            } catch (AbortTaskException e) {
                // 爬虫主动结束任务，任务无论是否完成、无论进度如何都将遭到抛弃
                currentTask.setStatus(CrawlerStatus.ABORTED);
                controller.getTaskPool().removeLeased(currentTask);
                currentTask = null;
            } catch (Throwable throwable) {
                // 发生异常
                currentTask = null;// for isWorking
                throwable.printStackTrace();
            }
        }
    }

    public void retire() {
        retire = true;
    }

    public Task getCurrentContext() {
        return currentTask;
    }

    public boolean isWorking() {
        return currentTask != null ? true : false;
    }
    
    /**
     * 强制完成当前任务
     */
    public void completeCurrentTask() {
        throw new AbortTaskException();// 抛出非受查异常，使爬虫线程回到循环
    }

    @Override
    public String getMessage() {
        return new StringBuilder(this.getName())
            .append(" ，该爬虫已经工作了 ")
            .append(workNumber)
            .append(" 次，其当前持有的爬虫上下文为 ")
            .append(currentTask.getMessage())
            .toString();
    }

}
