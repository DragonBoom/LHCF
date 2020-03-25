package indi.crawler.task;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import indi.crawler.bootstrap.CrawlerJob;
import indi.crawler.filter.TaskFilter;
import indi.crawler.monitor.JVMMonitor;
import indi.crawler.monitor.Monitor.MonitorThread;
import indi.crawler.processor.ProcessorChain;
import indi.crawler.thread.CrawlerThreadPool;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * 
 * 爬虫控制器，中间枢纽，负责各模块的调度
 * 
 * @author DragonBoom
 *
 */
@Slf4j
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
    
    @Getter
    @Setter
    private List<MonitorThread> monitorThreads = new LinkedList<>();// 监视器线程集合

    /**
     * 初始化
     */
    private void init(CrawlerJob job) {
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
                taskPool = new BlockingQueueTaskPool(this);
            }
        }
    }

    /**
     * 开始执行爬虫任务
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
    
    /**
     * 回收爬虫，将回收的功能从offer方法拆出来，提高可读性
     * 
     * @param ctx
     * @return
     */
    public boolean recover(Task ctx) {
        return taskPool.recover(ctx);

    }
    
    public Task poll() {
        if (taskPool == null) {
            initTaskPool();
        }
        return taskPool.poll();
    }
    
    /**
     * 结束所有监视器线程
     */
    public void retireMonitorThreads() {
        for (MonitorThread monitorThread : monitorThreads) {
            if (!monitorThread.isRetire()) {
                monitorThread.retire();
            }
        }
    }
    
    /**
     * 结束整个爬虫任务。注意，不同于爬虫任务完成，该方法将不会执行设置的爬虫任务结束时的回调
     * 
     * @see indi.crawler.monitor.CloseableMonitor#close()
     */
    public void close() {
        log.info("开始强制结束爬虫任务");
        // 关闭线程池
        threadPool.close();
        // 结束守护线程
        retireMonitorThreads();
        log.info("结束强制爬虫任务完成");
    }
    
    /**
     * 添加过滤器
     * 
     * @author DragonBoom
     * @since 2020.03.25
     * @param filter
     * @return
     */
    public boolean addFilter(TaskFilter filter) {
        log.info("添加爬虫任务过滤器:{}", filter);
        return taskPool.addFilter(filter);
    }
}
