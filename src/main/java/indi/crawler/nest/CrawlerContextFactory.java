package indi.crawler.nest;

import java.io.UnsupportedEncodingException;
import java.net.URI;

import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPatch;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;

import indi.crawler.task.Task;
import indi.exception.WrapperException;
import indi.util.BeanUtils;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * 生成爬虫上下文的工厂类
 * 
 * <p>将context的属性与初始化的逻辑进行解耦
 * 
 * @author DragonBoom
 *
 */
@AllArgsConstructor
@Getter
@Setter
@ToString
public class CrawlerContextFactory {
    private CrawlerController controller;

    public CrawlerContext createContext(Task task, final URI uri, String requestEntityString) {
        CrawlerContext ctx = new CrawlerContext();
        ctx.setController(controller);
        // 从Task中复制属性
        BeanUtils.copySelectedProperties(task, ctx)
                .copy("host")
                .copy("defaultMaxRetries", "maxRetries")// 最大重试次数
                .copy("defaultRetriesDeferrals", "retryDeferrals");// 重试延时时间（millis）
        // 初始化请求
        HttpRequestBase request = null;
        HttpEntity requestEntity = null;

        // full request, combine url & request...
        switch (task.getMethod()) {
        case GET:
            request = new HttpGet(uri);
            break;
        case POST:
            request = new HttpPost(uri);
            break;
        case DELETE:
            request = new HttpDelete(uri);
            break;
        case PATCH:
            request = new HttpPatch(uri);
            break;
        case PUT:
            request = new HttpPut(uri);
            break;
        }
        if (requestEntityString != null) {
            try {
                requestEntity = new StringEntity(requestEntityString);
            } catch (UnsupportedEncodingException e) {
                throw new WrapperException(e);
            }
        }
        // 若请求可携带请求实体（方法是POST/PUT），则设置请求实体
        if (request instanceof HttpEntityEnclosingRequestBase && requestEntity != null) {
            ((HttpEntityEnclosingRequestBase) request).setEntity(requestEntity);
        }
        request.setURI(uri);
        request.setHeaders(task.getRequestHeaders());
        // begin build header
        request.setHeader("Host", uri.getHost());
//        // attach cookie
//        if (task.isKeepReceiveCookie()) {
//            String cookies = task.getCookieStore().get(uri);
//            if (cookies != null && cookies.length() > 1)
//                request.addHeader("Cookie", cookies);
//        }
        RequestConfig config = task.getRequestConfigBuilder().build();
        if (config != null) {
            request.setConfig(config);
        }

        ctx.setRequest(request);
        ctx.setRequestEntity(requestEntity);
        // 设置其他属性
        ctx.setUri(uri);
        ctx.setTask(task);
        ctx.setResponseEntity(new ResponseEntity(ctx, task.getResultType()));
        return ctx;
    }
}
