
package indi.crawler.interceptor.http;

import indi.crawler.interceptor.InterceptorContext;
import indi.crawler.nest.CrawlerContext;
import indi.crawler.nest.CrawlerStatus;
import indi.crawler.nest.CrawlerThread;

/**
 * 默认处理器，位于处理器链的最前端，用于实现默认的重试、计时等功能
 * 
 * @author DragonBoom
 *
 */
public class BasicCrawlerInterceptor extends HttpInterceptor {
    
    @Override
    public HandlerResult process(InterceptorContext iCtx) {
        beforeAll(iCtx);
        afterExecuteRequest(iCtx);
        afterReceiveResponse(iCtx);
        afterHandleResult(iCtx);
        
        HandlerResult result = new HandlerResult();
        return result;
    }

    @Override
    public void beforeAll(InterceptorContext hCtx) {
        // 初始化/重置爬虫状态 TODO
        CrawlerContext ctx = hCtx.getCrawlerContext();
        ctx.setStatus(CrawlerStatus.RUNNING);
        ctx.setAttempts(ctx.getAttempts() + 1);
        ctx.getTask().addTotalCounts();
        ctx.setRegistration(System.currentTimeMillis());
        ctx.setThread((CrawlerThread) Thread.currentThread());

        super.beforeAll(hCtx);
    }

    @Override
    public void afterHandleResult(InterceptorContext hCtx) {
        CrawlerContext ctx = hCtx.getCrawlerContext();
        ctx.setStatus(CrawlerStatus.FINISHED);
        // 主动移除爬虫
        ctx.getController().getContextPool().removeLeased(ctx);
        
        super.afterHandleResult(hCtx);
    }
}
