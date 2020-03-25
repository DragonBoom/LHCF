package indi.crawler.task;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.util.Optional;

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

import indi.bean.BeanUtils;
import indi.crawler.task.def.TaskDef;
import indi.exception.WrapperException;
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
public class TaskFactory {
    private CrawlerController controller;
    
    /**
     * 创建新的爬虫任务。
     * 
     * @param taskName 爬虫任务定义的名称
     * @param uri 请求路径
     * @param requestEntityString 请求实体
     * @return
     */
    public Task build(String taskName, final URI uri, String requestEntityString) {
        TaskDef task = controller.getJob().getTaskDef(taskName);
        return build(task, uri, requestEntityString);
    }

    public Task build(TaskDef taskDef, final URI uri, String requestEntityStr) {
        Task task = new Task();
        task.setTaskDefName(taskDef.getName());
        task.setRequestEntityStr(requestEntityStr);
        task.setController(controller);
        // 从Task中复制属性
        BeanUtils.copySelectedProperties(taskDef, task)
                .copy("host")
                .copy("defaultMaxRetries", "maxRetries")// 最大重试次数
                .copy("defaultRetriesDeferrals", "retryDeferrals");// 重试延时时间（millis）
        // 初始化请求
        HttpRequestBase request = null;
        HttpEntity requestEntity = null;

        // full request, combine url & request...
        switch (taskDef.getMethod()) {
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
        if (requestEntityStr != null) {
            try {
                requestEntity = new StringEntity(requestEntityStr);
            } catch (UnsupportedEncodingException e) {
                throw new WrapperException(e);
            }
        }
        // 若请求可携带请求实体（方法是POST/PUT），则设置请求实体
        if (request instanceof HttpEntityEnclosingRequestBase && requestEntity != null) {
            ((HttpEntityEnclosingRequestBase) request).setEntity(requestEntity);
        }
        request.setURI(uri);
        request.setHeaders(taskDef.getRequestHeaders());
        // begin build header
        request.setHeader("Host", uri.getHost());
//        // attach cookie
//        if (task.isKeepReceiveCookie()) {
//            String cookies = task.getCookieStore().get(uri);
//            if (cookies != null && cookies.length() > 1)
//                request.addHeader("Cookie", cookies);
//        }
        RequestConfig config = taskDef.getRequestConfigBuilder().build();
        if (config != null) {
            request.setConfig(config);
        }

        task.setRequest(request);
        task.setRequestEntity(requestEntity);
        // 设置其他属性
        task.setUri(uri);
        task.setTaskDef(taskDef);
        task.setResponseEntity(new ResponseEntity(task, taskDef.getResultType()));
        
        // gen and set id key
        Optional.ofNullable(taskDef.getIdKeyGenerator())
                .map(fun -> fun.apply(task.getUri().toString(), task.getRequest()))
                .ifPresent(idKey -> task.setIdentityKey(idKey));
        return task;
    }
}
