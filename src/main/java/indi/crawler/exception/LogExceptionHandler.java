package indi.crawler.exception;

import indi.crawler.nest.CrawlerContext;
import indi.crawler.processor.ProcessorContext;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LogExceptionHandler implements ExceptionHandler {
    
    @Override
    public void handleException(ProcessorContext hCtx, Throwable throwable) {
        CrawlerContext ctx = hCtx.getCrawlerContext();
        StringBuilder sb = new StringBuilder("!!! WARN: Task ");
        sb.append(ctx.getTask().getName()).append(" Happen Exceptions [ ")
                .append(ctx.getThrowables()).append(" ]");
        ctx.getThrowables().forEach(Throwable::printStackTrace);
        log.info(sb.toString());
    }
}
