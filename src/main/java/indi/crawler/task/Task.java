package indi.crawler.task;

import java.io.Serializable;
import java.net.URI;
import java.util.Comparator;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.impl.client.CloseableHttpClient;

import indi.crawler.task.def.TaskDef;
import indi.crawler.thread.CrawlerThread;
import indi.util.BeanUtils;
import indi.util.Cleanupable;
import indi.util.Log;
import indi.util.Message;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

/**
 * 描述一个爬虫任务
 * 
 * @author DragonBoom
 *
 */
@Getter
@Setter
@ToString
@Slf4j
@NoArgsConstructor(access = AccessLevel.PACKAGE)
public class Task implements Cleanupable, Comparable<Task>, Message, Log, Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * Context绑定的爬虫的任务
     */
    private transient TaskDef taskDef;
    /**
     * 反序列化时通过这个属性找到TaskDef
     */
    private String taskDefName;
    
    private String requestEntityStr;
    
    
    private transient CrawlerController controller;

    private URI uri;
    private transient CloseableHttpClient client;
    private CrawlerStatus status = CrawlerStatus.CREATED;
    private transient CrawlerThread thread;
    /**
     * 当前爬虫在爬虫池中的优先级，数值越小，优先级越大
     */
    private int priority = 0;
    /**
     * 爬虫尝试完成任务的次数
     */
    private int attempts = 0;
    /**
     * 爬虫执行任务时发生的异常，懒加载
     */
    private List<Throwable> throwables;
    // 最大重试次数
    private int maxRetries;
    // 重试超过次数限制后需要等待的时间，单位秒
    private long retryDeferrals;
    // 唤醒时间
    private long wakeUpTime;
    // 注册时间
    private long registration;
    private long deadLine;
    /**
     * 存储自定义变量，可用于在处理器间传递变量，采取懒加载
     */
    private Map<String, Object> data;
    // HTTP请求部分
    private transient HttpRequestBase request;
    private transient HttpEntity requestEntity;
    // HTTP proxy
    private transient HttpHost host;
    // HTTP响应部分
    private transient HttpResponse response;
    // HTTP响应实体，懒加载
    private transient ResponseEntity responseEntity = null;
    // 由本Context产生的子任务
    private transient List<Task> childs;    

    @Override
    public void cleanup() {
        BeanUtils.cleanup(this);
    }
    
    public void addThrowables(Throwable throwable) {
        throwables = Optional.ofNullable(throwables).orElse(new LinkedList<Throwable>());
        throwables.add(throwable);
    }

    @Override
    public int compareTo(Task o) {
        return o.getPriority() - this.priority; // TreeMap 取值时优先取小值！！
    }

    public static class DeferralContextComparator implements Comparator<Task> {

        @Override
        public int compare(Task o1, Task o2) {
            if (o1.wakeUpTime != o2.wakeUpTime) {
                return (int) (o1.wakeUpTime - o2.wakeUpTime); // 是否可能越界？
            } else {
                return o2.priority - o1.priority;
            }
        }
    }

    @Override
    public void log() {
        log.info("-------------------------------------------------");
        Task ctx = this;
        log.info("status: {}, taskName: {}, uri: {}, exceptions: {}, attemptCounts: {}, wakeUpTime: {}",
                ctx.getStatus(), ctx.getTaskDef().getName(), ctx.getUri(), ctx.getThrowables(),
                ctx.getAttempts(), new Date(ctx.getWakeUpTime()));
        log.info("response: {}", ctx.getResponse());
//        log.info("responseEntity: {}", ctx.getResponseEntity().getContent());
        
        CrawlerThread thread = ctx.getThread();
        log.info("关于该任务: {}", thread.getMessage());
        log.info("爬虫线程: {}", thread);
        log.info("爬虫任务是否仍在执行? {}",
                thread.getCurrentContext() != null && thread.getCurrentContext().equals(ctx) ? "是" : "否");
        log.info("-------------------------------------------------");
    }

    @Override
    public String getMessage() {
        return new StringBuilder(uri.toString())
                .append(", taskName:").append(getTaskDef().getName())
                .append(", status:").append(getStatus())
                .append(", exceptions:").append(getThrowables())
                .append(", attemptCounts:").append(getAttempts())
                .toString();
    }
}