package indi.crawler.interceptor.http;

import indi.crawler.interceptor.CrawlerInterceptor;
import indi.crawler.interceptor.InterceptorContext;
import lombok.Getter;
import lombok.Setter;

/**
 * HTTP爬虫拦截器
 * 
 * @author DragonBoom
 * @since 2019.05.02
 *
 */
public abstract class HttpInterceptor extends CrawlerInterceptor {

    @Getter
    @Setter
    protected InterceptorContext iCtx;

    @Override
    public HandlerResult process(InterceptorContext iCtx) {
        beforeAll(iCtx);
        afterExecuteRequest(iCtx);
        afterReceiveResponse(iCtx);
        afterHandleResult(iCtx);
        
        HandlerResult result = new HandlerResult();
        return result;
    }

    public void beforeAll(InterceptorContext iCtx) {
        if (super.next instanceof HttpInterceptor) {
            ((HttpInterceptor) next).beforeAll(iCtx);
        }
    }

    public void afterExecuteRequest(InterceptorContext iCtx) {
        if (super.next instanceof HttpInterceptor) {
            ((HttpInterceptor) next).afterExecuteRequest(iCtx);
        }
    };

    public void afterReceiveResponse(InterceptorContext iCtx) {
        if (super.next instanceof HttpInterceptor) {
            ((HttpInterceptor) next).afterReceiveResponse(iCtx);
        }
    }

    public void afterHandleResult(InterceptorContext iCtx) {
        if (super.next instanceof HttpInterceptor) {
            ((HttpInterceptor) next).afterHandleResult(iCtx);
        }
    }

}
