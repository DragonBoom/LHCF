package indi.crawler.processor.http;

import indi.crawler.processor.ProcessorContext;
import indi.crawler.processor.ProcessorResult;
import lombok.AllArgsConstructor;

/**
 * 缓存拦截器，将利用缓存加快对特定任务的处理
 * 
 * @author DragonBoom
 *
 */
@AllArgsConstructor
public abstract class CacheProcessor extends HttpProcessor {
    
    @Override
    public ProcessorResult executeRequest0(ProcessorContext iCtx) throws Throwable {
        return executeRequestByCache(iCtx);
    }

    protected ProcessorResult executeRequestByCache(ProcessorContext iCtx) throws Throwable {
        return ProcessorResult.KEEP_GOING;
    }

    @Override
    public ProcessorResult receiveResponse0(ProcessorContext iCtx) throws Throwable {
        return receiveResponseByCache(iCtx);
    }

    protected ProcessorResult receiveResponseByCache(ProcessorContext iCtx) throws Throwable {
        return ProcessorResult.KEEP_GOING;
    }

    @Override
    public ProcessorResult afterHandleResult0(ProcessorContext iCtx) throws Throwable {
        return afterHandleResultByCache(iCtx);
    }
    
    protected ProcessorResult afterHandleResultByCache(ProcessorContext iCtx) throws Throwable {
        return ProcessorResult.KEEP_GOING;
    }
}
