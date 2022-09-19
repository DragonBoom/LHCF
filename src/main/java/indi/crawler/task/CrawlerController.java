package indi.crawler.task;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import indi.crawler.bootstrap.CrawlerJob;
import indi.crawler.filter.TaskFilter;
import indi.crawler.monitor.JVMMonitor;
import indi.crawler.monitor.Monitor.MonitorThread;
import indi.crawler.processor.ProcessorChain;
import indi.crawler.thread.CrawlerThread;
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
    public static final Long DEFAULT_TIME_TO_LIVE = 64L;
    public static final TimeUnit DEFAULT_TIME_TO_LIVE_UNIT = TimeUnit.SECONDS;
    @Getter
    private TaskPool taskPool;// 懒加载
    @Getter
    @Setter
    private CrawlerThreadPool threadPool;
    @Getter
    private CrawlerJob job;
    @Getter
    private ProcessorChain chain;
    @Getter
    @Setter
    private TaskFactory taskFactory;
    private List<MonitorThread> monitors;// 监视器线程集合

    /**
     * 初始化
     */
    private void init(CrawlerJob job) {
        this.job = job;
        monitors = new LinkedList<>();
        threadPool = new CrawlerThreadPool(this);
        chain = new ProcessorChain(this);
        taskFactory = new TaskFactory(this);
        new JVMMonitor(this);
    }

    public CrawlerController(CrawlerJob job) {
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
    public void process(Task task) {
        initProcessStatus(task);
        chain.process(task);
    }
    
    private void initProcessStatus(Task task) {
        task.checkAndSetStatus(CrawlerStatus.RUNNING);
        task.setAttempts(task.getAttempts() + 1);
        task.getTaskDef().addTotalCounts();
    }
    
    public boolean offer(Task task) {
        return getPool().offer(task);
    }
    
    /**
     * 延期执行爬虫
     * 
     * @param ctx
     * @param wakeUpTime 唤醒时间，毫秒
     * @return 成功true，失败false
     */
    public boolean deferral(Task ctx, Long wakeUpTime) {
        return getPool().deferral(ctx, wakeUpTime);

    }
    
    public Task poll() {
        return getPool().poll();
    }
    
    /**
     * 结束所有监视器线程
     */
    public void retireMonitorThreads() {
        for (MonitorThread monitorThread : monitors) {
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
        // 结束守护线程
        retireMonitorThreads();
        // 关闭线程池
        threadPool.close();
        log.info("强制结束爬虫任务完成");
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
        return getPool().addFilter(filter);
    }
    
    /**
     * 
     * @param thread
     * @return 监视器数量
     * @since 2020.10.04
     */
    public synchronized int addMonitor(MonitorThread thread) {
        monitors.add(thread);
        return monitors.size();
    }
    
    /**
     * 统一的修改爬虫状态的入口。
     * 
     * <p>当且仅当爬虫状态不符合预期时，通过爬虫任务自带的可重入锁加锁，然后执行给定的函数
     * 
     * @param task
     * @param 修改爬虫状态的函数
     * @since 2021.12.11
     */
    public void changeStatus(Task task, CrawlerStatus status, Runnable setStatusFun) {
        ReentrantLock lock = task.getStatusLock();
        lock.lock();
        try {
            CrawlerStatus oldStatus = task.getStatus();
            if (!oldStatus.equals(status)) {
                if (oldStatus.equals(CrawlerStatus.ABORTED)) {
                    log.info("已忽略对主动中断任务的状态的变更");
                } else {
                    setStatusFun.run();
                }
            }
        } finally {
            lock.unlock();
        }
        checkStatus(task);
    }
    
    /**
     * 统一的校验爬虫状态的入口
     * 
     * @param task
     * @since 2021.12.11
     */
    public void checkStatus(Task task) {
        CrawlerStatus status = task.getStatus();
        
        switch (status) {
        case ABORTED:
            CrawlerThread thread = task.getThread();
            if (thread != null) {
                thread.completeCurrentTask(task);
                getPool().remove(task);
            }
            break;
        default :
        }
    }
    
}
