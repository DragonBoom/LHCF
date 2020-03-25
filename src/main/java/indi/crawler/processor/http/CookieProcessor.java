package indi.crawler.processor.http;

import java.net.URI;

import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpRequestBase;

import indi.crawler.cookies.CookieStore;
import indi.crawler.processor.ProcessorContext;
import indi.crawler.processor.ProcessorResult;
import indi.crawler.task.Task;
import indi.crawler.task.def.TaskDef;

public class CookieProcessor extends HTTPProcessor {

    // attach cookie
    @Override
    protected ProcessorResult executeRequest0(ProcessorContext pCtx) throws Throwable {
        Task ctx = pCtx.getCrawlerContext();
        TaskDef task = ctx.getTaskDef();
        URI uri = ctx.getUri();
        HttpRequestBase request = ctx.getRequest();
        
        if (task.isKeepReceiveCookie()) {
            String cookies = task.getCookieStore().get(uri);
            if (cookies != null && cookies.length() > 0) {
                request.addHeader("Cookie", cookies);
            }
        }
        
        return ProcessorResult.KEEP_GOING;
    }
    
    @Override
    protected ProcessorResult afterHandleResult0(ProcessorContext pCtx) throws Throwable {
        // save cookie
        Task ctx = pCtx.getCrawlerContext();
        HttpResponse response = ctx.getResponse();
        // WARN: 这里如果不是通过HTTP请求拿数据（ctx.getResponse() == null），则不记录cookie，否则会与缓存之类操作冲突
        if (ctx.getTaskDef().isKeepReceiveCookie() && response != null) {
            Header[] setCookies = response.getHeaders("Set-Cookie");
            if (setCookies != null) {
                CookieStore cookieStore = ctx.getTaskDef().getCookieStore();
                for (Header h : setCookies) {
                    cookieStore.add(h.getValue(), ctx.getUri());
                }
                
            }
        }
        
        return ProcessorResult.KEEP_GOING;
    }
}
