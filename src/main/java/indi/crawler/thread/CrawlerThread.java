package indi.crawler.thread;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Objects;

import indi.crawler.exception.AbortTaskException;
import indi.crawler.task.CrawlerController;
import indi.crawler.task.Task;
import indi.exception.WrapperException;
import indi.obj.Message;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * 爬虫线程类，不包含具体工作的逻辑
 * 
 * <p>为了配合线程池使用，不再继承Thread接口。因此，实际执行爬虫任务的线程并非本类的实例
 * 
 * @author DragonBoom
 *
 */
@Slf4j
public class CrawlerThread implements Runnable, Message {
    private CrawlerController controller;
    @Getter
    private volatile boolean retire;
    private Task currentTask;
    /** 工作次数 */
    private int workTimes;
    @Getter
    private String name;
    @Getter
    private Thread currentThread;

    private void init(CrawlerThreadPool pool, String threadName) {
        this.controller = pool.getController();
        this.name = threadName;
    }

    public CrawlerThread(CrawlerThreadPool pool,String threadName) {
        init(pool, threadName);
    }

    /*
     * 2020.10.04 不负责处理业务上的异常
     */
    @Override
    public void run() {
        currentThread = Thread.currentThread();
        while (!retire) {
            try {
                // 领取爬虫任务
                Task ctx = null;
//                System.out.println("领取任务：" + currentThread);
                ctx = controller.poll();
                // 若没有领取到任务，开始休息
                if (ctx == null) {
//                    System.out.println("领取任务失败：" + currentThread);
                    try {
                        TimeUnit.SECONDS.sleep(2);// 休息2s
                    } catch (InterruptedException e) {
                    }
                    continue;
                }
                currentTask = ctx;
                Thread.interrupted();// 清除可能被无视的中断状态
                ctx.setThread(this);
                // 执行任务 
                controller.process(ctx);
                
                workTimes++;
            } catch (AbortTaskException e) {
                // 爬虫主动结束任务
                log.debug("主动结束任务：{}", Optional.ofNullable(currentTask).map(Task::getMessage).orElse(null));
            } catch (Exception e) {
                // 理论上不会执行到这里
                throw new WrapperException(e);
            }
            currentTask = null;
        }
    }

    public void retire() {
        retire = true;
    }

    public Task getCurrentContext() {
        return currentTask;
    }

    public boolean isWorking() {
        return currentTask != null;
    }
    
    /**
     * 主动结束当前任务
     * 
     * 注意，执行该方法的线程可能不是创建爬虫的线程，但这种情况下无法保证立即结束
     * 
     * @param task 仅当本对象对应的爬虫线程当前处理的是该任务时才将其强制完成
     */
    public void completeCurrentTask(Task task) {
        if (Thread.currentThread() == currentThread) {
            // 正在被执行该任务的线程执行，不需要考虑已切换到其他任务的情况
            if (!Objects.equal(currentTask, task)) {
                return;
            }
            throw new AbortTaskException();// 抛出非受查异常，使爬虫线程立即结束任务
        } else {
            // 需要注意的是，不能用Thread.interrupt()来中断传输HTTP数据：目前传输数据时虽然用到了Channel，但并没有
            // 真正的实现可中断
        }
    }

    @Override
    public String getMessage() {
        return new StringBuilder(name)
            .append(" ，该爬虫已经工作了 ")
            .append(workTimes)
            .append(" 次，其当前持有的爬虫上下文为 ")
            .append(currentTask.getMessage())
            .toString();
    }

}
