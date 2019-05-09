package indi.crawler.interceptor.http;

import java.net.URI;

import org.apache.http.Header;
import org.apache.http.client.methods.HttpRequestBase;

import indi.crawler.interceptor.InterceptorContext;
import indi.crawler.nest.CrawlerContext;
import indi.crawler.task.Task;

public class CookieInterceptor extends HttpInterceptor {

    // attach cookie
    @Override
    public void executeRequest(InterceptorContext hCtx) {
        CrawlerContext ctx = hCtx.getCrawlerContext();
        Task task = ctx.getTask();
        URI uri = ctx.getUri();
        HttpRequestBase request = ctx.getRequest();
        
        if (task.isKeepReceiveCookie()) {
            String cookies = task.getCookieStore().get(uri);
            if (cookies != null && cookies.length() > 1)
                request.addHeader("Cookie", cookies);
        }
        
        super.executeRequest(hCtx);
    }
    
    @Override
    public void afterReceiveResponse(InterceptorContext hCtx) {
        // save cookie
        CrawlerContext ctx = hCtx.getCrawlerContext();
        if (ctx.getTask().isKeepReceiveCookie()) {
            for (Header h : ctx.getResponse().getHeaders("Set-Cookie")) {
                ctx.getTask().getCookieStore().add(h.getValue(), ctx.getUri());
            }
        }
        
        super.afterReceiveResponse(hCtx);
    }
}
