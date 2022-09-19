package indi.crawler.exception;

import indi.crawler.processor.ProcessorContext;

/**
 * 异常处理器
 * 
 * @author DragonBoom
 *
 */
public interface ExceptionHandler {
    
    /**
     * 处理执行爬虫任务过程中发生的异常
     */
    void handleException(ProcessorContext hCtx, Throwable throwable);
}
