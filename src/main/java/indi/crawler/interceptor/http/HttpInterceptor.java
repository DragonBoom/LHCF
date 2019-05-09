package indi.crawler.interceptor.http;

import indi.crawler.interceptor.Interceptor;
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
public abstract class HttpInterceptor extends Interceptor {
    @Getter
    @Setter
    protected InterceptorContext iCtx;
    
    /**
     * 只有第一个拦截器会执行该方法。。。
     */
    @Override
    public HandlerResult process(InterceptorContext iCtx) {
        beforeAll(iCtx);
        executeRequest(iCtx);
        receiveResponse(iCtx);
        afterReceiveResponse(iCtx);
        afterHandleResult(iCtx);
        
        HandlerResult result = new HandlerResult();
        return result;
    }

    protected void beforeAll(InterceptorContext iCtx) {
        if (super.next instanceof HttpInterceptor) {
            ((HttpInterceptor) next).beforeAll(iCtx);
        }
    }

    protected void executeRequest(InterceptorContext iCtx) {
        if (super.next instanceof HttpInterceptor) {
            ((HttpInterceptor) next).executeRequest(iCtx);
        }
    };
    
    protected void receiveResponse(InterceptorContext iCtx) {
        if (super.next instanceof HttpInterceptor) {
            ((HttpInterceptor) next).receiveResponse(iCtx);
        }
    };

    protected void afterReceiveResponse(InterceptorContext iCtx) {
        if (super.next instanceof HttpInterceptor) {
            ((HttpInterceptor) next).afterReceiveResponse(iCtx);
        }
    }

    protected void afterHandleResult(InterceptorContext iCtx) {
        if (super.next instanceof HttpInterceptor) {
            ((HttpInterceptor) next).afterHandleResult(iCtx);
        }
    }

}
