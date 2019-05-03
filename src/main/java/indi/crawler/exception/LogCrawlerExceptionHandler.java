package indi.crawler.exception;

import indi.crawler.interceptor.InterceptorContext;
import indi.crawler.nest.CrawlerContext;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LogCrawlerExceptionHandler implements CrawlerExceptionHandler {
    
    @Override
    public void handleException(InterceptorContext hCtx, Throwable throwable) {
        CrawlerContext ctx = hCtx.getCrawlerContext();
        StringBuilder sb = new StringBuilder("!!! WARN: Task ");
        sb.append(ctx.getTask().getName()).append(" Happen Exceptions [ ")
                .append(ctx.getThrowables()).append(" ]");
        ctx.getThrowables().forEach(Throwable::printStackTrace);
        log.info(sb.toString());
    }
}
