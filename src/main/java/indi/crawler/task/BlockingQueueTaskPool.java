package indi.crawler.task;

import java.util.Arrays;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.TreeMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import org.apache.http.client.utils.DateUtils;

import indi.crawler.filter.TaskFilter;
import indi.crawler.monitor.Monitor.MonitorThread;
import indi.crawler.recoder.CommonRecorder;
import indi.crawler.recoder.Recorder;
import indi.crawler.task.def.TaskDef;
import indi.exception.WrapperException;
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
     * 
     * 注意这个对象(TreeMap)的使用线程不安全，但PriorityBlockingQueue的使用线程安全，因此仅添加队列时需要加锁
     */
    private TreeMap<TaskDef, PriorityBlockingQueue<Task>> availables;
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
     * 
     * <p>此前类型为线程不安全的PriorityQueue，会导致线程安全问题，已更正
     */
    private PriorityBlockingQueue<Task> leaseds;
    
    private Lock recentlyWakeUpLock;
    
    private Recorder recorder;
    private List<TaskFilter> filters;

    
    /**
     * 之后可以考虑把初始化逻辑解耦出来 FIXME:
     */
    protected void init() {
        recentlyWakeUpRecord = DEFAULT_RECENTLY_WAKE_UP;
        recentlyWakeUpLock = new ReentrantLock();
        availables = new TreeMap<>();
        deferrals = new PriorityBlockingQueue<>(100, new Task.DeferralContextComparator());
        deferralRecoverThread = new DeferralRecoverThread(100L);
        deferralRecoverThread.startDeamon(controller);
        
        leaseds = new PriorityBlockingQueue<>();
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
        Objects.requireNonNull(ctx);
//        // 不处理延期任务
        // 暂时用该方法添加延期任务
//        if (ctx.getStatus() == CrawlerStatus.DEFERRED) {
//            // 会执行到这一步只可能是逻辑有问题，直接抛异常
//            throw new RuntimeException("尝试新增该延期任务：" + ctx);
//        }
        
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
        PriorityBlockingQueue<Task> queue = Optional.ofNullable(availables.get(taskDef))
                .orElseGet(() -> {
                    synchronized (taskDef) {// availables线程不安全，需要加锁，锁的粒度为任务定义
                        // 若找不到任务定义对应的阻塞队列，则初始化一个
                        PriorityBlockingQueue<Task> priorityQueue = new PriorityBlockingQueue<>();
                        availables.put(taskDef, priorityQueue);
                        return priorityQueue;
                    }
                });
        // 插入并返回
        return queue.offer(ctx);
    }

    @Override
    public boolean deferral(Task ctx, Long wakeUpTime) {
        ctx.setWakeUpTime(wakeUpTime);
        
        // 不处理非延期任务
        CrawlerStatus status = ctx.getStatus();
        if (status != CrawlerStatus.DEFERRED && status != CrawlerStatus.BLOCKING_TIME) {
            // 会执行到这一步只可能是逻辑有问题，直接抛异常
            throw new RuntimeException("尝试回收该非延期任务：" + ctx.getMessage());
        }
        
        // 缓存延时任务
        // a. 尝试更新最近（最小）唤醒时间为更小值
        if (wakeUpTime < recentlyWakeUpRecord) {
            recentlyWakeUpLock.lock();
            try {
                if (wakeUpTime < recentlyWakeUpRecord) {
                    recentlyWakeUpRecord = wakeUpTime;
                }
            } finally {
                recentlyWakeUpLock.unlock();
            }
        }
        
        // b. 尝试从出租集合里移除爬虫任务
        // 可能移除失败（如任务被拦截器拦截，取自可用队列，但尚未添加至出租队列）
        removeLeased(ctx);
        // d. 移除成功才缓存，移除失败表明爬虫任务来源不明，无法处理
        return deferrals.offer(ctx);
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
    private Task getAvailableTask(TreeMap<TaskDef, PriorityBlockingQueue<Task>> availables) {
        Task ctx = null;
        // 按任务定义的优先级，取出任务队列
        for (Entry<TaskDef, PriorityBlockingQueue<Task>> entry : availables.entrySet()) {
            PriorityBlockingQueue<Task> queue = entry.getValue();
            
            // 从队列，按优先级取出爬虫任务
            // 由于PriorityBlockingQueue是线程安全的，以下逻辑，每个线程操作的ctx应该各不相同，不会冲突
            while (true) {
                ctx = queue.poll();
                if (ctx != null) {
                    boolean isExecute = true;
                    // 调用过滤器，判断是否继续执行该任务
                    if (filters != null) {
                        // for-for循环性能较差，后续考虑优化 FIXME:
                        for (TaskFilter filter : filters) {
                            isExecute = filter.isExecute(ctx, Thread.currentThread(), controller);
                            if (!isExecute) {
                                // 一旦有过滤器判断不予执行，就结束遍历过滤器
                                break;
                            }
                        }
                    }
                    if (isExecute) {
                        // 爬虫可执行，结束循环
                        break;
                    }
                    // 爬虫不可执行，继续循环获取爬虫任务（抛弃不可执行爬虫）
                } else {
                    // 队列为空，结束循环
                    break;
                }
            }
            // 若获取到任务，结束遍历任务定义；否则继续尝试获取低级任务定义的任务
            if (ctx != null) {
                break;
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
        Objects.requireNonNull(ctx);
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
        for (Entry<TaskDef, PriorityBlockingQueue<Task>> e : availables.entrySet()) {
            // 累加每种任务定义的可用任务数
            size += e.getValue().size();
        }
        return size;
    }
    
    @Override
    public int deferralSize() {
        return deferrals.size();
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
        sb.append("availables--[ ");
        // 遍历每个任务定义
        for (Entry<TaskDef, PriorityBlockingQueue<Task>> e : availables.entrySet()) {
            PriorityBlockingQueue<Task> tasks = e.getValue();
            // 遍历每个任务状态
            int queueSize = tasks.size();
            if (queueSize > 0) {
                sb.append(" [ ").append(e.getKey().getName()).append(":").append(queueSize).append(" ] ");
            }
        }
        sb.append(" ]");
        sb.append(" deferrials- [ ").append(deferrals.size()).append(" ], recently wake up is :")
                .append(DateUtils.formatDate(new Date(recentlyWakeUpRecord), "yyyy-MM-dd"))
                .append(" | ")
                .append(" leaseds-[")
                .append(getLeasedSize())
                .append("]");
        return sb.toString();
    }
    
    @Override
    public String getLeasedDetail() {
        String detail = leaseds
                .stream()
                .map(task -> "<" + task.getTaskDefName() + " / " + task.getUri() + ">")
                .collect(Collectors.joining(", "));
        return detail;
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

        /**
         * 
         * @param millis 遍历超时队列的时间，由于每次只出队一个任务，该参数决定了唤醒的最高频率，因此必须尽可能的小
         */
        public DeferralRecoverThread(Long millis) {
            super("DeferralRecoverThread", millis);
            log.info("init DeferralRecoverThread: {} millis", millis);
        }
        
        @Override
        public void run0() {
            while (!retire) {
                long now = System.currentTimeMillis();
                if (now > recentlyWakeUpRecord && deferrals.size() > 0) {
                    // 将等待队列中所有超过唤醒时间的任务出队
                    Task firstTask = deferrals.peek();
                    Long wt = Optional.ofNullable(firstTask).map(Task::getWakeUpTime).orElse(null);
                    // 注意：这里并发执行时有线程安全问题：如果同时有多个线程出队，会导致超前出队
                    // 因为实际只有一个线程在执行，因此目前没有问题
                    while (firstTask != null && wt < now) {
                        Task task = deferrals.poll();// 该任务不一定等于firstTask，但唤醒时间一定小于等于firstTask
                        offer(task);
                        log.info("【DeferralRecoverThread】：唤醒爬虫任务：{}", task.getMessage());
                        
                        // 处理下一个任务
                        firstTask = deferrals.peek();
                        wt = Optional.ofNullable(firstTask).map(Task::getWakeUpTime).orElse(null);
                    }
                    updateRecentlyWakeUpRecord(wt);
                }
            }
        }
        
        /**
         * 更新最近唤醒时间为更大值
         * 
         * @author DragonBoom
         * @since 2020.07.23
         * @param nextWakeUpTime
         */
        private void updateRecentlyWakeUpRecord(Long nextWakeUpTime) {
            if (nextWakeUpTime == null) {
                return;
            }
            
            if (nextWakeUpTime > recentlyWakeUpRecord) {
                recentlyWakeUpLock.lock();
                try {
                    if (nextWakeUpTime > recentlyWakeUpRecord) {
                        recentlyWakeUpRecord = nextWakeUpTime;
                    }
                } finally {
                    recentlyWakeUpLock.unlock();
                }
            }
        }
    }

}
