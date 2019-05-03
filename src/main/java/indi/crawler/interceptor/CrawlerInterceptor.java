package indi.crawler.interceptor;

import lombok.Getter;
import lombok.Setter;

/**
 * 爬虫任务拦截器，在爬虫任务生命周期的各个阶段对其进行拦截
 * <p>
 * 拦截器这个概念很好，各个框架普遍有用到（Intercepting Filter拦截链模式？），但之前是直接把HTTP主题爬虫的生命周期作为拦截器的接口，
 * 这样是否不太合适呢？那到底要怎么定义爬虫的生命周期呢？<br>
 */
public abstract class CrawlerInterceptor {
    @Getter
    @Setter
    protected CrawlerInterceptor next;

    public HandlerResult process(InterceptorContext iCtx) {
        return next.process(iCtx);
    }

    @Getter
    @Setter
    public static class HandlerResult {

        public enum Result {
            KEEY_GOING, OVER;
        }

        private Object object;
    }
}
