package indi.crawler.interceptor;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import indi.crawler.exception.BasicExceptionHandler;
import indi.crawler.exception.CrawlerExceptionHandler;
import indi.crawler.interceptor.http.BasicHttpInterceptor;
import indi.crawler.interceptor.http.FinalHttpInterceptor;
import indi.crawler.interceptor.http.LogSpeedInterceptor;
import indi.crawler.nest.CrawlerContext;
import indi.crawler.processor.HTTPProcessor;
import indi.crawler.task.Task;
import indi.crawler.task.TaskType;
import lombok.Getter;
import lombok.Setter;

/**
 * 具体的线程安全应该由各爬虫控制器自己来确保
 * 
 * @author dragonboom
 *
 */
public class InterceptorChain {
    @Getter
    @Setter
    private LinkedList<Interceptor> defaultInterceptors;
    private CrawlerExceptionHandler exceptionHandler;
    
    private ConcurrentHashMap<TaskType, Interceptor> finalInterceptorMap = new ConcurrentHashMap<>();
    
    private void init() {
        defaultInterceptors = new LinkedList<>();
        /*
         * 越后面的拦截器越晚执行
         */
        defaultInterceptors.add(new BasicHttpInterceptor());// TODO
        defaultInterceptors.add(new LogSpeedInterceptor());// TODO sure ?
        
        exceptionHandler = new BasicExceptionHandler();// TODO
        
        finalInterceptorMap.put(TaskType.HTTP_TOPICAL, new FinalHttpInterceptor(new HTTPProcessor()));
    }

    public InterceptorChain() {
        init();
    }
    
    public void process(CrawlerContext ctx) {
        Task task = ctx.getTask();
        List<Interceptor> interceptors = task.getCrawlerInterceptors();
        if (interceptors == null) {
            interceptors = initInterceptors(task);
        }
        InterceptorContext hCtx = new InterceptorContext(ctx);
        try {
            interceptors.get(0).process(hCtx);
        } catch (Throwable e) {
            exceptionHandler.handleException(hCtx, e);
        }
    }
    
    /**
     * 初始化Task的拦截器并返回
     */
    private synchronized List<Interceptor> initInterceptors(Task task) {
        List<Interceptor> interceptors = task.getCrawlerInterceptors();

        if (interceptors != null) {
            throw new IllegalArgumentException("当前爬虫已有拦截器");
        }
        List<Interceptor> customInterceptors = task.getCustomInterceptors();
        interceptors = new ArrayList<>(customInterceptors.size() + defaultInterceptors.size() + 1);// 1 for final interceptor
        
        interceptors.addAll(defaultInterceptors);
        interceptors.addAll(customInterceptors);
        
        // get and add final interceptor
        Interceptor finalInterceptor = finalInterceptorMap.get(task.getType());
        
        if (finalInterceptor == null) {
            throw new IllegalArgumentException("暂不支持该格式： " + task.getType());
        }
        
        interceptors.add(finalInterceptor);
        
        // 构建拦截链
        Object[] array = interceptors.toArray();
        if (array.length > 1) {
            // set next for all
            for (int i = 1; i < array.length; i++) {
                Interceptor prev = (Interceptor) array[i - 1];
                Interceptor now = (Interceptor) array[i];
                prev.setNext(now);
            }
        }

        task.setCrawlerInterceptors(interceptors);
        return interceptors;
    }
}
