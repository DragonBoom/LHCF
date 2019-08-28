package indi.crawler.task;

import java.util.Date;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;

import indi.crawler.recoder.CommonRecorder;
import indi.crawler.recoder.Recorder;
import indi.crawler.task.def.TaskDef;
import lombok.extern.slf4j.Slf4j;

/**
 * TODO: 考虑把等待队列中的任务移到Redis/消息队列中，以对付任务量过大/需要保存任务记录的情况
 * 
 * @author DragonBoom
 *
 */
@Slf4j
public class BlockingQueueTaskPool implements TaskPool {
    private static final int DEFAULT_RECENTLY_WAKE_UP = 0;

    /**
     * 就绪队列，该队列中的爬虫上下文随时可以取出进行处理<br>
     * 使用Map结构，使得可以由Task直接找到其就绪队列<br>
     * 使用TreeMap，使得取值可以按照优先顺序进行<br>
     */
    private TreeMap<TaskDef, ImmutableMap<CrawlerStatus, PriorityQueue<Task>>> availables;
    /**
     * 等待队列
     */
    private PriorityBlockingQueue<Task> deferrals;
    /**最近唤醒的时间(millis)*/
    private volatile long recentlyWakeUpRecord;
    private Set<Task> leaseds; // 出租集合，记录出租的Context
    private Lock availablesLock;
    private Lock leasedsLock;
    private Lock recentlyWakeUpLock;
    private Recorder recorder;

    
    /**
     * 之后可以考虑把初始化逻辑解耦出来
     */
    protected void init() {
        recentlyWakeUpRecord = DEFAULT_RECENTLY_WAKE_UP;
        recentlyWakeUpLock = new ReentrantLock();
        availablesLock = new ReentrantLock();
        leasedsLock = new ReentrantLock();
        availables = new TreeMap<>();
        deferrals = new PriorityBlockingQueue<>(100, new Task.DeferralContextComparator());
        leaseds = new HashSet<>();
        recorder = new CommonRecorder();
    }

    public BlockingQueueTaskPool() {
        log.info("使用本地内存的阻塞队列爬虫任务池");
        init();
    }

    /**
     * offer:若插入失败返回false而不是抛出异常
     * 
     * @return
     */
    @Override
    public boolean offer(Task ctx) {
        // 检查记录
        if (ctx.getTaskDef().isNeedCheckRocord()) {
            if (recorder.chechAndRecord(ctx)) {
                return false;
            }
        }
        boolean result = false;
        // FIXME: 不需要根据status判断状态
        // 根据CrawlerStatus判断任务状态
        if (ctx.getStatus() == CrawlerStatus.DEFERRED) {
            // 缓存延时任务
            // a. 更新最近（最小）唤醒时间
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
            }
        }
        
        // 添加任务
        TaskDef task = ctx.getTaskDef();
        availablesLock.lock();
        try {
            // !!! 大坑！对TreeMap而言，仅以比较结果，而不是equals方法判断映射结果
            // 因此，需要保证treeMap的key的比较结果都不同，才能保证key/value间是__映射__关系
            ImmutableMap<CrawlerStatus, PriorityQueue<Task>> statusMap = Optional.ofNullable(availables.get(task))
                    .orElseGet(() -> {
                        Builder<CrawlerStatus, PriorityQueue<Task>> imMapBuilder = ImmutableMap.builder();
                        for (CrawlerStatus status : CrawlerStatus.values()) {
                            imMapBuilder.put(status, new PriorityQueue<>());
                        }
                        ImmutableMap<CrawlerStatus, PriorityQueue<Task>> imMap = imMapBuilder.build();
                        availables.put(task, imMap);
                        return imMap;
                    });
            
            PriorityQueue<Task> queue = statusMap.get(ctx.getStatus());
            result = queue.offer(ctx);
        } finally {
            availablesLock.unlock();
        }
        return result;
    }

    @Override
    public Task poll() {
        Task ctx = null;
        // 1 优先从延时等待队列取任务
        long now = 0;
        if (deferrals.size() > 0 && (now = System.currentTimeMillis()) > recentlyWakeUpRecord) {
            // 取出context
            ctx = deferrals.poll();
            if (ctx != null) {
                long wt = ctx.getWakeUpTime();
                if (wt < now) {
                    // 若context还没有超时
                    // 将取出的context归还
                    deferrals.add(ctx);
                    ctx = null;
                } else {
                    // 若context已经超时，则ctx不为null
                }
                // 更新最近唤醒时间
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
        // 2
        if (ctx == null) {
            availablesLock.lock();
            try {
                // 按任务类型优先级取出队列
                // FIXME:
                for (Entry<TaskDef, ImmutableMap<CrawlerStatus, PriorityQueue<Task>>> entry : availables.entrySet()) {
                    PriorityQueue<Task> queue = entry.getValue().get(CrawlerStatus.CREATED);
                    // 再按上下文优先级取出上下文
                    ctx = queue.poll();
                    if (ctx != null) {
                        break;
                    }
                }
            } finally {
                availablesLock.unlock();
            }
        }
        // finally
        if (ctx != null) {
            leasedsLock.lock();
            try {
                leaseds.add(ctx);
            } finally {
                leasedsLock.unlock();
            }
        }
        return ctx;
    }

    /**
     * 返回当前等待队列数组，对该数组的操作是安全的，但需要注意内存开销！！
     * 
     * @return
     */
    @Override
    public Task[] cloneLeased() {
        final Object[] array;
        Lock lock = leasedsLock;// avoid get field
        lock.lock();
        try {
            array = leaseds.toArray();
        } finally {
            lock.unlock();
        }

        Task[] tasks = new Task[array.length];
        for (int i = 0; i < tasks.length; i++) {
            tasks[i] = (Task) array[i];
        }
        return tasks;
    }

    @Override
    public int getLeasedSize() {
        leasedsLock.lock();
        try {
            return leaseds.size();
        } finally {
            leasedsLock.unlock();
        }
    }

    /**
     * 移除出租的爬虫
     */
    @Override
    public boolean removeLeased(Task ctx) {
        boolean result = false;
        leasedsLock.lock();
        try {
            result = leaseds.remove(ctx);
        } finally {
            leasedsLock.unlock();
        }
        return result;
    }

    @Override
    public int workableSize() {
        return availableSize() + deferrals.size();
    }
    
    @Override
    public int availableSize() {
        availablesLock.lock();
        int size = 0;
        try {
            for (Entry<TaskDef, ImmutableMap<CrawlerStatus, PriorityQueue<Task>>> e : availables.entrySet()) {
                ImmutableMap<CrawlerStatus, PriorityQueue<Task>> imMap = e.getValue();
                for (Entry<CrawlerStatus, PriorityQueue<Task>> e1 : imMap.entrySet()) {
                    size += e1.getValue().size();
                }
            }
        } finally {
            availablesLock.unlock();
        }
        return size;
    }
    
    @Override
    public boolean isEmpty() {
        return getLeasedSize() == 0 && workableSize() == 0;
    }
    

    @Override
    public String getMessage() {
        StringBuilder sb = new StringBuilder();
        sb.append("availables--");
        availablesLock.lock();
        try {
            for (Entry<TaskDef, ImmutableMap<CrawlerStatus, PriorityQueue<Task>>> e : availables.entrySet()) {
                ImmutableMap<CrawlerStatus, PriorityQueue<Task>> imMap = e.getValue();
                for (Entry<CrawlerStatus, PriorityQueue<Task>> e1 : imMap.entrySet()) {
                    int queueSize = e1.getValue().size();
                    if (queueSize > 0) {
                        sb.append(" [ ").append(e.getKey().getName()).append("-").append(e1.getKey()).append(":")
                                .append(queueSize).append(" ] ");
                    }
                }
            }
        } finally {
            availablesLock.unlock();
        }
        sb.append(" deferrials- [ ").append(deferrals.size()).append(" ] recently wake up is :")
                .append(new Date(recentlyWakeUpRecord))
                .append(" | ")
                .append(" leaseds-[")
                .append(getLeasedSize())
                .append("]");
        return sb.toString();
    }
}
