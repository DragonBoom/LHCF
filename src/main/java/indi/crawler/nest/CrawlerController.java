package indi.crawler.nest;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import indi.crawler.bootstrap.CrawlerJob;
import indi.crawler.interceptor.InterceptorChain;
import indi.crawler.monitor.JVMMonitor;
import indi.crawler.processor.HTTPProcessor;
import indi.crawler.processor.Processor;
import indi.crawler.task.TaskType;
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
    private ContextPool contextPool;
    @Getter
    @Setter
    private CrawlerThreadPool threadPool;
    private Map<TaskType, Processor> processorMap;
    @Getter
    private CrawlerJob job;
    @Getter
    private InterceptorChain chain;
    @Getter
    @Setter
    private CrawlerContextFactory contextFactory;

    /**
     * 初始化
     */
    private void init() {
        contextPool = new ContextPool();
        processorMap = new HashMap<>();
        threadPool = new CrawlerThreadPool(this);

        // 注册处理器
        registerProcessor(TaskType.HTTP_TOPICAL, new HTTPProcessor());
        
        chain = new InterceptorChain();
        
        contextFactory = new CrawlerContextFactory(this);
        
        new JVMMonitor(this);
    }

    public CrawlerController(CrawlerJob job) {
        this.job = job;
        init();
    }

    public ContextPool getPool() {
        return contextPool;
    }

    private void registerProcessor(TaskType type, Processor processor) {
        if (!processorMap.containsKey(type)) {
            processorMap.put(type, processor);
        }
    }

    /**
     *  开始执行爬虫任务
     */
    public void process(CrawlerContext ctx) {
        chain.process(ctx);
    }
    
    public boolean offer(CrawlerContext ctx) {
        return contextPool.offer(ctx);
    }
    
    public CrawlerContext poll() {
        return contextPool.poll();
    }
}
