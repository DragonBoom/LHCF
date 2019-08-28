package indi.crawler.task;

import java.util.concurrent.TimeUnit;

import indi.crawler.bootstrap.CrawlerJob;
import indi.crawler.monitor.JVMMonitor;
import indi.crawler.processor.ProcessorChain;
import indi.crawler.thread.CrawlerThreadPool;
import lombok.Getter;
import lombok.Setter;

/**
 * 
 * 爬虫控制器，中间枢纽，负责各模块的调度
 * 
 * @author DragonBoom
 *
 */
public class CrawlerController {
    public final static Long DEFAULT_TIME_TO_LIVE = 64L;
    public final static TimeUnit DEFAULT_TIME_TO_LIVE_UNIT = TimeUnit.SECONDS;
    @Getter
    private TaskPool taskPool;
    @Getter
    @Setter
    private CrawlerThreadPool threadPool;// 懒加载
    @Getter
    private CrawlerJob job;
    @Getter
    private ProcessorChain chain;
    @Getter
    @Setter
    private TaskFactory taskFactory;

    /**
     * 初始化
     */
    private void init(CrawlerJob job) {
//        String redisURI = job.getRedisURI();
//        if (redisURI != null) {
//            taskPool = new RedisMQCrawlerTaskPool(redisURI, this);
//        } else {
//            taskPool = new BlockingQueueTaskPool();
//        }
        threadPool = new CrawlerThreadPool(this);

        chain = new ProcessorChain(this);
        
        taskFactory = new TaskFactory(this);
        
        new JVMMonitor(this);
    }

    public CrawlerController(CrawlerJob job) {
        this.job = job;
        init(job);
    }

    public TaskPool getPool() {
        if (taskPool == null) {
            initTaskPool();
        }
        return taskPool;
    }
    
    private synchronized void initTaskPool() {// FIXME: 缩小锁的粒度
        if (taskPool == null) {
            String redisURI = job.getRedisURI();
            if (redisURI != null) {
                taskPool = new RedisMQCrawlerTaskPool(redisURI, this);
            } else {
                taskPool = new BlockingQueueTaskPool();
            }
        }
    }

    /**
     *  开始执行爬虫任务
     */
    public void process(Task ctx) {
        chain.process(ctx);
    }
    
    public boolean offer(Task ctx) {
        if (taskPool == null) {
            initTaskPool();
        }
        return taskPool.offer(ctx);
    }
    
    public Task poll() {
        if (taskPool == null) {
            initTaskPool();
        }
        return taskPool.poll();
    }
}
