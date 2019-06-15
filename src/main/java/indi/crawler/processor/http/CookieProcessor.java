package indi.crawler.processor.http;

import java.net.URI;

import org.apache.http.Header;
import org.apache.http.client.methods.HttpRequestBase;

import indi.crawler.nest.CrawlerContext;
import indi.crawler.processor.ProcessorContext;
import indi.crawler.processor.ProcessorResult;
import indi.crawler.task.Task;

public class CookieProcessor extends HttpProcessor {

    // attach cookie
    @Override
    protected ProcessorResult executeRequest0(ProcessorContext pCtx) throws Throwable {
        CrawlerContext ctx = pCtx.getCrawlerContext();
        Task task = ctx.getTask();
        URI uri = ctx.getUri();
        HttpRequestBase request = ctx.getRequest();
        
        if (task.isKeepReceiveCookie()) {
            String cookies = task.getCookieStore().get(uri);
            if (cookies != null && cookies.length() > 1)
                request.addHeader("Cookie", cookies);
        }
        
        return ProcessorResult.KEEP_GOING;
    }
    
    @Override
    protected ProcessorResult handleResult0(ProcessorContext pCtx) throws Throwable {
        // save cookie
        CrawlerContext ctx = pCtx.getCrawlerContext();
        if (ctx.getTask().isKeepReceiveCookie()) {
            for (Header h : ctx.getResponse().getHeaders("Set-Cookie")) {
                ctx.getTask().getCookieStore().add(h.getValue(), ctx.getUri());
            }
        }
        
        return ProcessorResult.KEEP_GOING;
    }
}
