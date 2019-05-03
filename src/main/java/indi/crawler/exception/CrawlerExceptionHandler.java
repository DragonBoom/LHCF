package indi.crawler.exception;

import indi.crawler.interceptor.InterceptorContext;

/**
 * 异常处理器
 * 
 * @author DragonBoom
 *
 */
public interface CrawlerExceptionHandler {
    
    /**
     * 处理异常
     */
    void handleException(InterceptorContext hCtx, Throwable throwable);
}
