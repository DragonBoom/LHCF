package indi.crawler.interceptor.http;

import indi.crawler.interceptor.InterceptorContext;
import indi.crawler.nest.CrawlerContext;
import indi.crawler.task.Task;
import lombok.AllArgsConstructor;

/**
 * 缓存拦截器，将利用缓存加快对特定任务的处理
 * 
 * @author DragonBoom
 *
 */
@AllArgsConstructor
public abstract class CacheInterceptor extends HttpInterceptor {
    private Task task;

    /**
     * 执行普通的发起请求，供子类调用
     */
    protected void executeRequestPlain(InterceptorContext iCtx) {
        super.executeRequest(iCtx);
    }

    public void executeRequestByCache(InterceptorContext iCtx) {
        this.executeRequestPlain(iCtx);
    }

    @Override
    public void receiveResponse(InterceptorContext iCtx) {
        CrawlerContext ctx = iCtx.getCrawlerContext();
        if (!ctx.getTask().equals(task)) {
            super.receiveResponse(iCtx);
            return;
        }
        receiveResponseByCache(iCtx);
    }

    /**
     * 执行普通的接收响应，供子类调用
     */
    protected void receiveResponsePlain(InterceptorContext iCtx) {
        super.receiveResponse(iCtx);
    }

    public void receiveResponseByCache(InterceptorContext iCtx) {
        this.receiveResponsePlain(iCtx);
    }
    
    /**
     * 供子类调用
     */
    protected void afterHandleResultPlain(InterceptorContext iCtx) {
        super.afterHandleResult(iCtx);
    }
    
    public void afterHandleResultByCache(InterceptorContext iCtx) {
        this.afterHandleResultPlain(iCtx);
    }

    @Override
    protected void afterHandleResult(InterceptorContext iCtx) {
        CrawlerContext ctx = iCtx.getCrawlerContext();
        if (!ctx.getTask().equals(task)) {
            super.afterHandleResult(iCtx);
            return;
        }
        afterHandleResultByCache(iCtx);
    }

}
