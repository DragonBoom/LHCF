/**
 * 
 */
package indi.crawler.filter;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import indi.crawler.task.CrawlerController;
import indi.crawler.task.CrawlerStatus;
import indi.crawler.task.Task;
import indi.crawler.task.def.TaskDef;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * 实现任务的同步阻塞等待。对同一类任务，每隔一段时间后才会执行下一个任务。通过过滤器的形式实现。
 * 
 * <p>以过滤器的形式，在执行任务后计算阻塞时间，在阻塞时间内拒绝执行其他同类任务，会导致爬虫线程不断地重试直至超过阻塞时间，并且
 * 重试时线程可能因为竞争锁而挂起，实际效率非常低！
 * 
 * <p>2020.07.23：
 * 优化方案：参考ASQ模式，将阻塞任务从爬虫池中取出并保存起来，当经过阻塞时间后再投入爬虫池，避免不断重试。可能需要守护线程来自动将
 * 爬虫任务投入爬虫池。似乎可将爬虫任务添加到爬虫池的延期处理队列即可，但若需如此，需要一次性为所有爬虫任务计算开始执行时间
 * 
 * <p>需要注意爬虫任务阻塞唤醒后仍需经过该拦截器，如何判断爬虫任务曾经阻塞过？暂时靠不拦截已阻塞任务实现
 * 
 * <p>TODO:应该将延期功能与爬虫池分离开来？
 * 
 * @author wzh
 * @since 2020.03.25
 */
@Slf4j
@NoArgsConstructor
public class BlockingWaitFilter extends TaskFilter {
    /** 存放任务定义与所需阻塞时间的映射 */
    private Map<TaskDef, Long> blockingMillisMap = new HashMap<>();
    /** 存放任务定义与下一次可开始时间的映射 */
    private Map<TaskDef, Long> nextMillisMap = new ConcurrentHashMap<>();
    /** 保存阻塞中的任务，用于避免重复阻塞 */
    private LinkedBlockingQueue<Task> blockingQueue = new LinkedBlockingQueue<>();
    
    
    /**
     * 为任务定义添加阻塞策略
     * 
     * @author DragonBoom
     * @since 2020.03.25
     * @param taskDef
     * @param millis
     */
    public synchronized void addBlock(TaskDef taskDef, Long millis) {
        blockingMillisMap.put(taskDef, millis);
    }

    @Override
    public boolean isExecute(Task task, Thread thread, CrawlerController controller) {
        if (blockingMillisMap.isEmpty()) {
            return true;
        }
        
        TaskDef taskDef = task.getTaskDef();
        Long bMillis = blockingMillisMap.get(taskDef);
        if (bMillis == null) {
            // 爬虫任务不需要阻塞执行
            return true;
        }
        if (task.getStatus() == CrawlerStatus.BLOCKING_TIME) {
            // 不拦截已阻塞任务
            throw new IllegalArgumentException("该任务已阻塞：" + task);
        }
        if (blockingQueue.remove(task)) {
            // 已执行过阻塞，跳过
            return true;
        }
        // 同步锁，粒度为爬虫定义对象，因此可能会多线程执行
        boolean isExecute = true;
        synchronized (taskDef) {
            // 判断是否需要阻塞
            Long next = nextMillisMap.get(taskDef);
            long now = System.currentTimeMillis();
            if (next == null) {
                // 该任务定义没有阻塞记录，新增一个；当前任务不需要阻塞
                log.debug("【阻塞同步过滤器】：初始化任务{}的阻塞记录完成", taskDef.getName());
                next = now;
            } else {
                if (next <= now) {
                    // 当前时间已超过可执行时间，不需要阻塞
                    log.debug("【阻塞同步过滤器】：任务{}不需要阻塞", taskDef.getName());
                } else {
                    // 尚未抵达下一次可执行时间，需要阻塞
                    log.debug("【阻塞同步过滤器】：任务{}需要阻塞，下一次唤醒时间为：{}", taskDef.getName(),
                            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(next)));
                    // 2020.07.23
                    // 将爬虫任务添加至爬虫池的延期队列
                    task.checkAndSetStatus(CrawlerStatus.BLOCKING_TIME);
                    if (!controller.deferral(task, next)) {
                        throw new RuntimeException("回收阻塞的爬虫任务失败：" + task.getMessage());
                    }
                    blockingQueue.add(task);
                    isExecute = false;
                }
            }
            // 添加/更新下一次可执行时间
            next += bMillis;
            nextMillisMap.put(taskDef, next);
        }
        return isExecute;// default true
    }
    
    public boolean isEmpty() {
        return blockingMillisMap.isEmpty();
    }
 
}
