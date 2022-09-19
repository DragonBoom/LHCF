package indi.crawler.task;

import java.io.Serializable;
import java.net.URI;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import indi.crawler.filter.TaskFilter;
import indi.crawler.task.def.TaskDef;
import indi.crawler.util.RedisUtils;
import indi.exception.WrapperException;
import io.lettuce.core.KeyValue;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

/**
 * 基于Redis消息队列的爬虫任务池
 * 
 * <p>Redis的消息队列通过 lpush 与 brpop 实现
 * 
 * @author DragonBoom
 *
 */
@Slf4j
public class RedisMQCrawlerTaskPool implements TaskPool {
    private CrawlerController controller;
    private String redisURI;
//    private RedisClient client;
//    private RedisCommands<String,Object> commands;
//    private ThreadLocal<RedisCommands<String,Object>> commandsThreadLocal;
    
    private PriorityBlockingQueue<Task> leaseds;
    
    private String KEY_PREFIX = "TASK-";
    
    private List<TaskDef> taskDefs;// 线程不安全的队列
    /**Redis 键数组，按优先级从大到小排列*/
    private String[] redisKeys;
    
    private Lock taskDefsLock; 
    
    private void init(String redisURI) {
        taskDefs = new LinkedList<>();
        taskDefsLock = new ReentrantLock();
        redisKeys = new String[] {"init"};// init key 
//        
//        client = RedisClient.create(redisURI);
//        client.setDefaultTimeout(Duration.ofMinutes(2));
//        
//        commandsThreadLocal = new ThreadLocal<RedisCommands<String,Object>>() {
//
//            @Override
//            protected RedisCommands<String, Object> initialValue() {
//                StatefulRedisConnection<String, Object> connect = client.connect(new StringObjectRedisCodec());
//                return connect.sync();
//            }
//        };

        
        leaseds = new PriorityBlockingQueue<>();
    }
    
    public RedisMQCrawlerTaskPool(String redisURI, CrawlerController controller) {
        log.info("使用Redis消息队列爬虫任务池");
        this.redisURI = redisURI;
        this.controller = controller;
        init(redisURI);
    }
    
    @Override
    public String getMessage() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean offer(Task task) {
        // TODO check record
        // 获取redis key
        String redisListKey = getRedisListKey(task.getTaskDef());
        TaskDef taskDef = task.getTaskDef();
        if (!taskDefs.contains(taskDef)) {// 判断条件为 < 0 而不是!= -1
            taskDefsLock.lock();
            try {
                // 没有处理过该任务定义，则添加其redis key，并按任务定义优先级重新排序
                if (!taskDefs.contains(taskDef)) {
                    taskDefs.add(taskDef);
                    taskDefs.sort((c1, c2) -> {
                        return c2.getPriority() - c1.getPriority();
                    });
                    List<String> keyList = taskDefs.stream()
                            .map(this::getRedisListKey)
                            .collect(Collectors.toList());
                    redisKeys = new String[keyList.size()];
                    keyList.toArray(redisKeys);
                }
            } finally {
                taskDefsLock.unlock();
            }
        }
        // remove from leaseds
        leaseds.remove(task);
        
        SimpleTask simpleTask = new SimpleTask(task.getTaskDefName(), task.getRequestEntityStr(), task.getUri());
        RedisUtils.getAsyncCommands(redisURI).lpush(redisListKey, simpleTask);
        return true;
    }
    

    @Override
    public boolean deferral(Task task, Long wakeUpTime) {
        // 暂不实现。。
        return offer(task);
    }
    
    @Getter
    @Setter
    @ToString
    @AllArgsConstructor
    @NoArgsConstructor
    private static final class SimpleTask implements Serializable {
        private static final long serialVersionUID = 1L;
        private String taskDefName;
        private String requestEntityStr;
        private URI uri;
        
    }
    
    private String getRedisListKey(TaskDef taskDef) {
        return new StringBuilder(KEY_PREFIX).append("(").append(taskDef.getPriority()).append(")").append("-")
                .append(taskDef.getName()).toString();
    }
    
    /**pop阻塞时间*/
    private int brpopTimeoutSeconds = 3;
    /**pop失败睡眠时间*/
    private int brpopFailDeferralSeconds = 4;
    
    private Lock pollLock = new ReentrantLock();
    
    
    private volatile Long wakeUpMillis = System.currentTimeMillis();
    
    /**
     * 由于Redis是单线程，当poll阻塞时，无法push
     */
    @Override
    public Task poll() {
//        // 若第一个插入还没有完成，等待后返回null
//        long sleepMillis = getNeedSleepMillis();
//        
//        if (sleepMillis > 0) {
//            try {
//                Thread.sleep(sleepMillis);
//            } catch (InterruptedException e) {
//                throw new WrapperException(e);
//            }
//            // 直接返回null，表明该线程拿不到爬虫任务
//            return null;
//        }
        KeyValue<String, Object> kv = null;
        pollLock.lock();
        try {
//            sleepMillis = getNeedSleepMillis();
//            if (sleepMillis > 0) {
//                // 直接返回null，表明该线程拿不到爬虫任务
//                return null;
//            }
            kv = RedisUtils.getAsyncCommands(redisURI).brpop(brpopTimeoutSeconds, redisKeys).get();
            // 只要有一个线程poll超时或队列为空时，其他所有线程就等待固定的一段时间，确保不因为poll阻塞导致无法push，从而造成死循环
            // 2019.08.25: 理论上不应该出现这种多个线程一起阻塞的情况。之前应该是因为不同线程共用同一个连接才导致了阻塞
            if (kv == null) {
//                // pop失败，计算下一次pop的最快时间
//                Calendar calendar = Calendar.getInstance();
//                calendar.add(Calendar.SECOND, (int) brpopFailDeferralSeconds);
//                wakeUpMillis = calendar.getTimeInMillis();
//                log.warn("pop fail will sleep {} seconds", brpopFailDeferralSeconds);
                return null;
            }
        } catch (InterruptedException | ExecutionException e) {
            // 结束线程时，由于线程仍在这里阻塞，会导致无意义的报错，因此这里加这个判断 FIXME: 为啥？
            e.printStackTrace();
        } finally {
            pollLock.unlock();
        }
        Object obj = kv.getValue();
        if (obj instanceof SimpleTask) {
            TaskFactory taskFactory = controller.getTaskFactory();
            SimpleTask simpleTask = (SimpleTask) obj;

            Task task = taskFactory.build(simpleTask.getTaskDefName(), simpleTask.getUri(), simpleTask.getRequestEntityStr());
            // ??
            task.setController(controller);
            TaskDef taskDef = controller.getJob().getTaskDef(task.getTaskDefName());
            task.setTaskDef(taskDef);
            // add to leaseds
            leaseds.add(task);
//            System.out.println("poll + " + task);
            return task;
        } else {
            throw new IllegalArgumentException("类型错误, " + obj.getClass());
        }
    }
    
    /**
     * 获取需要睡眠的时间
     * 
     * @return
     */
    private long getNeedSleepMillis() {
        return redisKeys.length == 0 ? this.brpopTimeoutSeconds : wakeUpMillis - getCurrentMillis();
    }
    
    Cache<String, Long> nowCache = CacheBuilder.newBuilder().expireAfterWrite(2, TimeUnit.SECONDS).build();
    
    /**
     * 缓存当前时间，避免频繁请求当前时间浪费资源
     * 
     * FIXME: 有这个必要吗？
     */
    private long getCurrentMillis() {
        try {
            return nowCache.get("now", () -> System.currentTimeMillis());
        } catch (ExecutionException e) {
            throw new WrapperException(e);
        }
    }

    @Override
    public Task[] cloneLeased() {
        // 原子地获取数组
        Object[] array = leaseds.toArray();
        
        // 改变数组类型
        Task[] result = new Task[array.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = (Task) array[i];
        }

        return result;
    }

    @Override
    public boolean remove(Task ctx) {
        return leaseds.remove(ctx);
    }

    @Override
    public int getLeasedSize() {
        return leaseds.size();
    }

    @Override
    public int workableSize() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int availableSize() {
        int result = 0;
        for (String redisKey : redisKeys) {
            try {
                result += RedisUtils.getAsyncCommands(redisURI).llen(redisKey).get();
            } catch (InterruptedException | ExecutionException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        return result;
    }

    @Override
    public int deferralSize() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public boolean isEmpty() {
//        return false;
        
        return (availableSize() + getLeasedSize()) == 0;
    }

    @Override
    public boolean addFilter(TaskFilter filter) {
        return false;
    }

}
