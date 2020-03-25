package indi.crawler.task;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.TreeMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.http.client.utils.DateUtils;

import indi.crawler.filter.TaskFilter;
import indi.crawler.monitor.Monitor.MonitorThread;
import indi.crawler.recoder.CommonRecorder;
import indi.crawler.recoder.Recorder;
import indi.crawler.task.def.TaskDef;
import lombok.extern.slf4j.Slf4j;

/**
 * 基于内存的阻塞队列的爬虫池。
 * 
 * <p>在一开始的实现中，通过映射，将阻塞队列按任务定义与任务状态进行了分类，结构为：<爬虫任务定义，<状态，爬虫任务>>，目的是为了便于管理。
 * 后面发现并不需要按任务定义分类，遂简化为只按任务定义分类，从而提高执行效率（减少遍历次数与锁的粒度）、开发效率。
 * 
 * @author DragonBoom
 *
 */
@Slf4j
public class BlockingQueueTaskPool implements TaskPool {
    private static final int DEFAULT_RECENTLY_WAKE_UP = 0;
    
    private CrawlerController controller;

    /**
     * 就绪队列，该队列中的爬虫上下文随时可以取出进行处理<br>
     */
    private TreeMap<TaskDef, PriorityQueue<Task>> availables;
    /**
     * 等待队列，任务超时后会离开出租集合，进入该队列；注意该队列按唤醒时间进行排序
     */
    private PriorityBlockingQueue<Task> deferrals;
    /** 定时处理延期任务的线程 */
    DeferralRecoverThread deferralRecoverThread;
    
    /**最近唤醒的时间(millis)*/
    private volatile long recentlyWakeUpRecord;
    /**
     * 已出租爬虫集合
     */
    private PriorityQueue<Task> leaseds;
    
    private Lock recentlyWakeUpLock;
    
    private Recorder recorder;
    private List<TaskFilter> filters;

    
    /**
     * 之后可以考虑把初始化逻辑解耦出来
     */
    protected void init() {
        recentlyWakeUpRecord = DEFAULT_RECENTLY_WAKE_UP;
        recentlyWakeUpLock = new ReentrantLock();
        availables = new TreeMap<>();
        deferrals = new PriorityBlockingQueue<>(100, new Task.DeferralContextComparator());
        deferralRecoverThread = new DeferralRecoverThread(3000L);// per 3 second
        deferralRecoverThread.startDeamon(controller);
        
        leaseds = new PriorityQueue<>();
        recorder = new CommonRecorder();
        
        filters = new LinkedList<>();
    }

    public BlockingQueueTaskPool(CrawlerController controller) {
        log.info("使用本地内存的阻塞队列爬虫任务池");
        this.controller = controller;
        init();
    }

    /**
     * offer:若插入失败返回false而不是抛出异常
     * 
     * <p>需要保证<b>爬虫任务最多只存在于可用队列、出租集合及延时队列的其中一个</b>
     * 
     * @return
     */
    @Override
    public boolean offer(Task ctx) {
        // 不处理延期任务
        if (ctx.getStatus() == CrawlerStatus.DEFERRED) {
            // 会执行到这一步只可能是逻辑有问题，直接抛异常
            throw new RuntimeException("尝试新增该延期任务：" + ctx);
        }
        
        // 检查是否有重复记录，若有则不处理 FIXME: 待完善
        TaskDef taskDef = ctx.getTaskDef();
        if (taskDef.isNeedCheckRecord()) {
            if (recorder.chechAndRecord(ctx)) {
                return false;
            }
        }
        // 以防万一，从出租集合里移除context
        removeLeased(ctx);
        
        // 向可用队列添加任务
        // !!! 大坑！对TreeMap而言，仅以比较结果，而不是equals方法判断映射结果
        // 因此，需要保证treeMap的key的比较结果都不同，才能保证key/value间是__映射__关系
        PriorityQueue<Task> queue = Optional.ofNullable(availables.get(taskDef))
                .orElseGet(() -> {
                    // 若找不到任务定义对应的阻塞队列，则初始化一个
                    PriorityQueue<Task> priorityQueue = new PriorityQueue<>();
                    availables.put(taskDef, priorityQueue);
                    return priorityQueue;
                });
        // 插入并返回
        return queue.offer(ctx);
    }

    @Override
    public boolean recover(Task ctx) {
        // 不处理非延期任务
        if (ctx.getStatus() != CrawlerStatus.DEFERRED) {
            // 会执行到这一步只可能是逻辑有问题，直接抛异常
            throw new RuntimeException("尝试回收该非延期任务：" + ctx);
        }
        boolean result = false;
        // 缓存延时任务
        // a. 尝试更新最近（最小）唤醒时间
        long millis = ctx.getWakeUpTime();
        recentlyWakeUpLock.lock();
        try {
            if (millis < recentlyWakeUpRecord) {
                recentlyWakeUpRecord = millis;
            }
        } finally {
            recentlyWakeUpLock.unlock();
        }
        // b. 从出租集合里移除context
        boolean haveContext = removeLeased(ctx);
        if (haveContext) {
            // c. 若移除成功，则缓存
            deferrals.offer(ctx);
            result = true;
        } else {
            throw new RuntimeException("尝试非法回收没有出租记录的爬虫任务：" + ctx);
        }
    
        return result;
    }

    @Override
    public Task poll() {
        // 按任务类型优先级、任务优先级取出新创建的任务
        Task ctx = getAvailableTask(availables);
        
        // FIXME: 这里存在问题，一旦在这一步线程崩溃，就会导致任务丢失。。
        // finally 对于取出的任务，添加至出租队列
        if (ctx != null) {
            leaseds.add(ctx);
        }
        return ctx;
    }
    
    /**
     * 获取可用的爬虫任务
     * 
     * @author DragonBoom
     * @since 2020.03.25
     * @param availables
     */
    private Task getAvailableTask(TreeMap<TaskDef, PriorityQueue<Task>> availables) {
        Task ctx = null;
        // 按任务定义的优先级，取出状态-任务的映射
        for (Entry<TaskDef, PriorityQueue<Task>> entry : availables.entrySet()) {
            TaskDef taskDef = entry.getKey();
            
            // 进行过滤器的是否执行的判断
            boolean isExecute = true;
            if (filters != null && filters.size() > 0) {
                for (TaskFilter filter : filters) {
                    isExecute = filter.isExecute(taskDef, Thread.currentThread(), controller);
                    if (!isExecute) {
                        break;// 不执行，跳过该任务定义；注意这里只是结束对过滤器的遍历
                    }
                }
            }
            if (!isExecute) {
                continue;// 不执行，跳过该任务定义
            }
            PriorityQueue<Task> queue = entry.getValue();
            
            // 再按上下文优先级取出上下文
            ctx = queue.poll();
            if (ctx != null) {
                break;
            }
            
            // 执行过滤器的预处理
            if (filters != null && filters.size() > 0) {
                for (TaskFilter filter : filters) {
                    ctx = filter.preHandle(ctx, Thread.currentThread());
                }
            }
        }
        return ctx;
    }

    @Override
    public Task[] cloneLeased() {
        final Object[] array;
//        Lock lock = leasedsLock;// avoid get field
        array = leaseds.toArray();

        Task[] tasks = new Task[array.length];
        for (int i = 0; i < tasks.length; i++) {
            tasks[i] = (Task) array[i];
        }
        return tasks;
    }

    @Override
    public int getLeasedSize() {
        return leaseds.size();
    }

    /**
     * 移除出租的爬虫
     */
    @Override
    public boolean removeLeased(Task ctx) {
        // 从出租队列移除爬虫
        boolean result = false;
        result = leaseds.remove(ctx);
        return result;
    }

    @Override
    public int workableSize() {
        return availableSize() + deferrals.size();
    }
    
    @Override
    public int availableSize() {
        int size = 0;
        for (Entry<TaskDef, PriorityQueue<Task>> e : availables.entrySet()) {
            // 累加每种任务定义的可用任务数
            size += e.getValue().size();
        }
        return size;
    }
    
    @Override
    public boolean isEmpty() {
        return getLeasedSize() == 0 && workableSize() == 0;
    }
    

    /**
     * 打印爬虫池信息，形如：[任务1:20][任务2:30] deferrials- [10], recently wake up is : 2020.03.25 | leaseds-[30]
     */
    @Override
    public String getMessage() {
        StringBuilder sb = new StringBuilder();
        sb.append("availables--");
        // 遍历每个任务定义
        for (Entry<TaskDef, PriorityQueue<Task>> e : availables.entrySet()) {
            PriorityQueue<Task> tasks = e.getValue();
            // 遍历每个任务状态
            int queueSize = tasks.size();
            if (queueSize > 0) {
                sb.append(" [ ").append(e.getKey().getName()).append(":").append(queueSize).append(" ] ");
            }
        }
        sb.append(" deferrials- [ ").append(deferrals.size()).append(" ], recently wake up is :")
                .append(DateUtils.formatDate(new Date(recentlyWakeUpRecord), "yyyy-MM-dd"))
                .append(" | ")
                .append(" leaseds-[")
                .append(getLeasedSize())
                .append("]");
        return sb.toString();
    }
    
    @Override
    public synchronized boolean addFilter(TaskFilter filter) {
        this.filters.add(filter);
        return true;
    }

    /**
     * 定时将延期任务移动至可用队列的线程
     * 
     * @author wzh
     * @since 2020.03.25
     */
    private class DeferralRecoverThread extends MonitorThread {
        private Long millis;
        
        public DeferralRecoverThread(Long millis) {
            this.millis = millis;
        }
        
        @Override
        public void run() {
            while (retire) {
                try {
                    TimeUnit.MILLISECONDS.sleep(millis);
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                long now = System.currentTimeMillis();
                if (deferrals.size() > 0 && now > recentlyWakeUpRecord) {
                    // 取出第一个任务（即唤醒时间最小的任务）
                    Task ctx = deferrals.poll();
                    if (ctx != null) {
                        // 判断是否需要将任务放回延期队列
                        long wt = ctx.getWakeUpTime();
                        if (wt > now) {
                            // 若任务还没有超时，将取出的任务归还
                            // 注意，这里将按唤醒时间重新排序
                            deferrals.add(ctx);
                            ctx = null;
                            wt = now;
                        } else {
                            // 任务已超时,ctx不为null
                        }
                        // 若实际的最近唤醒时间大于缓存的记录，则更新
                        if (wt > recentlyWakeUpRecord) {
                            recentlyWakeUpLock.lock();
                            try {
                                if (wt > recentlyWakeUpRecord) {
                                    recentlyWakeUpRecord = wt;
                                }
                            } finally {
                                recentlyWakeUpLock.unlock();
                            }
                        }
                    }
                    
                    if (ctx != null) {
                        // 不需要将任务放回延期队列，将任务添加至可用队列
                        log.info("【DeferralRecoverThread】：回收爬虫任务：{}", ctx);
                        offer(ctx);
                    }
                }
            }
        }
    }

}
