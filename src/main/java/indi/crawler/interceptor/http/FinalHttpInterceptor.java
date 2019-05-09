package indi.crawler.interceptor.http;

import indi.crawler.interceptor.InterceptorContext;
import indi.crawler.nest.CrawlerContext;
import indi.crawler.processor.HTTPProcessor;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@AllArgsConstructor
@ToString
public class FinalHttpInterceptor extends HttpInterceptor {
    private HTTPProcessor processor;

    @Override
    public void executeRequest(InterceptorContext iCtx) {
        CrawlerContext ctx = iCtx.getCrawlerContext();
        try {
            processor.executeRequest(iCtx.getCrawlerContext());
        } catch (Exception e) {
            ctx.addThrowables(e);
        }
    }

    @Override
    public void receiveResponse(InterceptorContext iCtx) {
        CrawlerContext ctx = iCtx.getCrawlerContext();
        try {
            processor.receiveResponse(iCtx.getCrawlerContext());
        } catch (Exception e) {
            ctx.addThrowables(e);
        }
    }

    @Override
    public void afterReceiveResponse(InterceptorContext iCtx) {
        CrawlerContext ctx = iCtx.getCrawlerContext();
        try {
            processor.handleResult(iCtx.getCrawlerContext());
        } catch (Exception e) {
            ctx.addThrowables(e);
        }
    }

    @Override
    public void afterHandleResult(InterceptorContext iCtx) {
        CrawlerContext ctx = iCtx.getCrawlerContext();
        try {
            processor.afterHandleResult(iCtx.getCrawlerContext());
        } catch (Exception e) {
            ctx.addThrowables(e);
        }
    }

}
