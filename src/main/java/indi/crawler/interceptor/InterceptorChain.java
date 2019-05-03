package indi.crawler.interceptor;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import indi.crawler.exception.BasicExceptionHandler;
import indi.crawler.exception.CrawlerExceptionHandler;
import indi.crawler.interceptor.http.BasicCrawlerInterceptor;
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
    private LinkedList<CrawlerInterceptor> defaultInterceptors;
    private CrawlerExceptionHandler exceptionHandler;
    
    private ConcurrentHashMap<TaskType, CrawlerInterceptor> finalInterceptorMap = new ConcurrentHashMap<>();
    
    private void init() {
        defaultInterceptors = new LinkedList<>();
        /*
         * 越后面的拦截器越晚执行
         */
        defaultInterceptors.add(new BasicCrawlerInterceptor());// TODO
        defaultInterceptors.add(new LogSpeedInterceptor());// TODO sure ?
        
        exceptionHandler = new BasicExceptionHandler();// TODO
        
        finalInterceptorMap.put(TaskType.HTTP_TOPICAL, new FinalHttpInterceptor(new HTTPProcessor()));
    }

    public InterceptorChain() {
        init();
    }
    
    public void process(CrawlerContext ctx) {
        Task task = ctx.getTask();
        List<CrawlerInterceptor> interceptors = task.getCrawlerInterceptors();
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
    private synchronized List<CrawlerInterceptor> initInterceptors(Task task) {
        List<CrawlerInterceptor> interceptors = task.getCrawlerInterceptors();

        if (interceptors != null) {
            throw new IllegalArgumentException("当前爬虫已有拦截器");
        }
        List<CrawlerInterceptor> customInterceptors = task.getCustomInterceptors();
        interceptors = new ArrayList<>(customInterceptors.size() + defaultInterceptors.size() + 1);// 1 for final interceptor
        
        interceptors.addAll(defaultInterceptors);
        interceptors.addAll(customInterceptors);
        
        CrawlerInterceptor finalInterceptor = finalInterceptorMap.get(task.getType());
        
        if (finalInterceptor == null) {
            throw new IllegalArgumentException("暂不支持该格式： " + task.getType());
        }
        
        interceptors.add(finalInterceptor);
        Object[] array = interceptors.toArray();
        if (array.length > 1) {
            // set next for all
            for (int i = 1; i < array.length; i++) {
                CrawlerInterceptor prev = (CrawlerInterceptor) array[i - 1];
                CrawlerInterceptor now = (CrawlerInterceptor) array[i];
                prev.setNext(now);
            }
        }

        task.setCrawlerInterceptors(interceptors);
        return interceptors;
    }
}
