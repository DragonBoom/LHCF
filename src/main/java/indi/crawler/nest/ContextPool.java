package indi.crawler.nest;

import java.util.Date;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import indi.crawler.recoder.CommonRecorder;
import indi.crawler.recoder.Recorder;
import indi.crawler.task.Task;
import indi.util.Message;

public class ContextPool implements Message {
    private static final int DEFAULT_RECENTLY_WAKE_UP = 0;

    /**
     * 就绪队列，该队列中的爬虫上下文随时可以取出进行处理<br>
     * 使用Map结构，使得可以由Task直接找到其就绪队列<br>
     * 使用TreeMap，使得取值可以按照优先顺序进行<br>
     */
    private TreeMap<Task, PriorityQueue<CrawlerContext>> availables;
    /**
     * 等待队列
     */
    private PriorityBlockingQueue<CrawlerContext> deferrals;
    private volatile long recentlyWakeUp;
    private Set<CrawlerContext> leaseds; // 出租集合，记录出租的Context
    private Lock availablesLock;
    private Lock leasedsLock;
    private Lock recentlyWakeUpLock;
    private Recorder recorder;

    
    /**
     * 之后可以考虑把初始化逻辑解耦出来
     */
    protected void init() {
        recentlyWakeUp = DEFAULT_RECENTLY_WAKE_UP;
        recentlyWakeUpLock = new ReentrantLock();
        availablesLock = new ReentrantLock();
        leasedsLock = new ReentrantLock();
        availables = new TreeMap<>();
        deferrals = new PriorityBlockingQueue<>(100, new CrawlerContext.DeferralContextComparator());
        leaseds = new HashSet<>();
        recorder = new CommonRecorder();
    }

    public ContextPool() {
        init();
    }

    /**
     * offer:若插入失败返回false而不是抛出异常
     * 
     * @return
     */
    public boolean offer(CrawlerContext ctx) {
        // 检查记录
        if (ctx.getTask().isNeedCheckRocord()) {
            if (recorder.chechAndRecord(ctx)) {
                return false;
            }
        }
        boolean result = false;
        // 根据CrawlerStatus判断任务状态
        if (ctx.getStatus() == CrawlerStatus.DEFERRED) {
            // 缓存延时任务
            // a. 更新最近（最小）唤醒时间
            long millis = ctx.getWakeUpTime();
            recentlyWakeUpLock.lock();
            try {
                if (millis < recentlyWakeUp) {
                    recentlyWakeUp = millis;
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
        if (ctx.getStatus() == CrawlerStatus.CREATED || ctx.getStatus() == CrawlerStatus.RUNNING) {
            // 添加就绪任务
            Task task = ctx.getTask();
            availablesLock.lock();
            try {
                // !!! 大坑！对TreeMap而言，仅以比较结果，而不是equals方法判断映射结果
                // 因此，需要保证treeMap的key的比较结果都不同，才能保证key/value间是__映射__关系
                PriorityQueue<CrawlerContext> queue = availables.get(task);
                if (queue == null) {
                    queue = new PriorityQueue<>();
                    availables.put(task, queue);
                }
                result = queue.offer(ctx);
            } finally {
                availablesLock.unlock();
            }
        }
        return result;
    }

    public CrawlerContext poll(Task task) {
        leasedsLock.lock();
        CrawlerContext ctx = null;
        try {
            PriorityQueue<CrawlerContext> priorityQueue = availables.get(task);
            ctx = priorityQueue.poll();
        } finally {
            leasedsLock.unlock();
        }
        return ctx;
    }

    public CrawlerContext poll() {
        CrawlerContext ctx = null;
        // 1 优先从延时等待队列取任务
        long now = 0;
        if (deferrals.size() > 0 && (now = System.currentTimeMillis()) > recentlyWakeUp) {
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
                    if (wt > recentlyWakeUp) {
                        recentlyWakeUp = wt;
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
                for (Entry<Task, PriorityQueue<CrawlerContext>> entry : availables.entrySet()) {
                    PriorityQueue<CrawlerContext> queue = entry.getValue();
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
    public Object[] getLeased() {
        Object[] result = null;
        Lock lock = leasedsLock;// avoid get field
        lock.lock();
        try {
            result = leaseds.toArray();
        } finally {
            lock.unlock();
        }
        return result;
    }

    public int getLeasedSize() {
        Lock lock = leasedsLock;// avoid get field
        lock.lock();
        int size = 0;
        try {
            size = leaseds.size();
        } finally {
            lock.unlock();
        }
        return size;
    }

    /**
     * 移除出租的爬虫
     */
    public boolean removeLeased(CrawlerContext ctx) {
        boolean result = false;
        leasedsLock.lock();
        try {
            result = leaseds.remove(ctx);
        } finally {
            leasedsLock.unlock();
        }
        return result;
    }

    public int size() {
        return availableSize() + deferrals.size();
    }
    
    public int availableSize() {
        int size = 0;
        availablesLock.lock();
        try {
            for (Entry<Task, PriorityQueue<CrawlerContext>> e : availables.entrySet()) {
                size += e.getValue().size();
            }
        } finally {
            availablesLock.unlock();
        }
        return size;
    }
    
    
    public boolean isEmpty() {
        return getLeasedSize() == 0 && size() == 0;
    }
    

    @Override
    public String getMessage() {
        StringBuilder sb = new StringBuilder("## Context Pool Monitor: ");
        sb.append("availables--");
        availablesLock.lock();
        try {
            for (Entry<Task, PriorityQueue<CrawlerContext>> e : availables.entrySet()) {
                sb.append(" [ ").append(e.getKey().getName()).append("-").append(e.getValue().size())
                        .append(" ] ");
            }
        } finally {
            availablesLock.unlock();
        }
        sb.append(" deferrials- [ ").append(deferrals.size()).append(" ] recently wake up is :")
                .append(new Date(recentlyWakeUp))
                .append(" | ")
                .append(" leaseds-[")
                .append(getLeasedSize())
                .append("]");
        return sb.toString();
    }
}
