package indi.crawler.thread;

import indi.crawler.task.CrawlerController;
import indi.crawler.task.Task;
import indi.util.Message;
import lombok.Getter;

/**
 * 爬虫线程类，不包含具体工作的逻辑
 * 
 * @author DragonBoom
 *
 */
public class CrawlerThread extends Thread implements Message {
    private CrawlerController controller;
    @Getter
    private volatile boolean retire;
    private Task currentContext;
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
                    continue;
                }
                currentContext = ctx;
                // 执行任务 
                controller.process(ctx);
                
                workNumber++;
                currentContext = null;
            } catch (Throwable throwable) {
                throwable.printStackTrace();
//                LoggerUtils.getLogger().warn(throwable.getStackTrace().toString());
            }
        }
    }

    public void retire() {
        retire = true;
    }

    public Task getCurrentContext() {
        return currentContext;
    }

    public boolean isWorking() {
        return currentContext != null ? true : false;
    }

    @Override
    public String getMessage() {
        return new StringBuilder(this.getName())
            .append(" ，该爬虫已经工作了 ")
            .append(workNumber)
            .append(" 次，其当前持有的爬虫上下文为 ")
            .append(currentContext.getMessage())
            .toString();
    }

}
