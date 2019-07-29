package indi.crawler.processor;

import java.util.List;

/**
 * 负责执行爬虫任务的处理器（Processor），是处理爬虫任务的入口。基本上，一种爬虫类型一种ProcessorExecutor
 * 
 * @author DragonBoom
 */
public abstract class ProcessorExecutor {

    /**
     * 执行处理器
     */
    public abstract ProcessorResult execute(ProcessorContext pCtx, List<Processor> processors) throws Throwable;

    /**
     * 获取用于处理连接的处理器，该处理器将被放到处理器链的末端
     */
    public abstract Processor getConnectionProcessor();
}
