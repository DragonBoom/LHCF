/**
 * 
 */
package indi.crawler.filter;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import indi.crawler.task.CrawlerController;
import indi.crawler.task.def.TaskDef;
import lombok.extern.slf4j.Slf4j;

/**
 * 实现任务的同步阻塞等待。对同一类任务，每隔一段时间后才会执行下一个任务
 * 
 * @author wzh
 * @since 2020.03.25
 */
@Slf4j
public class BlockingWaitFilter extends TaskFilter {
    /** 存放任务定义与所需阻塞时间的映射 */
    private Map<TaskDef, Long> blockingMillisMap = new HashMap<>();
    /** 存放任务定义与下一次可开始时间的映射 */
    private Map<TaskDef, Long> nextMillisMap = new ConcurrentHashMap<>();
    
    public BlockingWaitFilter() {
    }
    
    /**
     * 添加任务定义的阻塞策略
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
    public boolean isExecute(TaskDef taskDef, Thread thread, CrawlerController controller) {
        Long bMillis = blockingMillisMap.get(taskDef);
        if (bMillis == null) {
            // 不需要阻塞
            return true;
        }
        // 同步锁，粒度为爬虫定义对象
        boolean isExecute = true;
        synchronized (taskDef) {
            // 判断是否需要阻塞
            Long next = nextMillisMap.get(taskDef);
            long now = System.currentTimeMillis();
            if (next == null) {
                // 没有阻塞记录，新增一个，不需要阻塞
                next = now + bMillis;
                nextMillisMap.put(taskDef, next);
                log.debug("【阻塞同步过滤器】：初始化任务{}的阻塞记录完成", taskDef.getName());
            } else {
                if (next <= now) {
                    // 可执行时间小于当前时间，不需要阻塞
                    // 更新阻塞记录
                    next = now + bMillis;
                    nextMillisMap.put(taskDef, next);
                    log.debug("【阻塞同步过滤器】：任务{}已不需要阻塞", taskDef.getName());
                } else {
                    // 尚未抵达下一次可执行时间，需要阻塞
                    log.debug("【阻塞同步过滤器】：任务{}需要阻塞，下一次唤醒时间为：{}", taskDef.getName(),
                            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(next)));
                    isExecute = false;
                    
                }
            }
        }
        
        return isExecute;
    }
 
}
