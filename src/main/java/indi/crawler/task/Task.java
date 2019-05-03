package indi.crawler.task;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.HeaderGroup;

import indi.crawler.bootstrap.CrawlerJob;
import indi.crawler.cookies.CookieStore;
import indi.crawler.exception.CrawlerExceptionHandler;
import indi.crawler.exception.LogCrawlerExceptionHandler;
import indi.crawler.interceptor.CrawlerInterceptor;
import indi.crawler.interceptor.http.CookieInterceptor;
import indi.crawler.interceptor.http.LogInterceptor;
import indi.crawler.interceptor.http.SpecificTaskBlockingWaitInterceptor;
import indi.crawler.nest.ResponseEntity.TYPE;
import indi.crawler.result.ResultHandler;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * 该类描述一个独立的爬虫任务
 * 
 * @author DragonBoom
 *
 */
@Getter
//@Setter
@ToString
public class Task implements Comparable<Task> {
    private static final int DEFAULT_REQUEST_TIMEOUT = 4000;
    private static final int DEFAULT_TIMEOUT = 4000;
    private ResultHandler resultHandler;

    public enum HTTPMethodType {
        GET, POST, PUT, PATCH, DELETE;
    }

    private HTTPMethodType method = HTTPMethodType.GET;
    // 用于辨识Task身份
    private String name;
    /**
     * HTTP请求头部，线程安全？！
     */
    private HeaderGroup requestHeaders;
    private Lock headersLock;
    private RequestConfig.Builder requestConfigBuilder;
    // Specific HTTP HOST
    private HttpHost host;
    /**
     * 默认最大重试次数
     * 
     */
    private int defaultMaxRetries = 10; // 默认尝试10次
    /**
     * 默认重试超过限制次数后等待的时间
     * 
     */
    private long defaultRetriesDeferrals = 10000L; // 尝试时每次等待十秒
    /**
     * 历史执行任务数
     */
    private AtomicLong totalCounts = new AtomicLong();
    private List<CrawlerInterceptor> customInterceptors;// 配置的拦截器
    @Setter
    private List<CrawlerInterceptor> crawlerInterceptors;// 真正的拦截器
    private List<CrawlerExceptionHandler> crawlerExceptionHandler;

    private TaskType type = TaskType.HTTP_TOPICAL;
    private TYPE resultType = TYPE.String;
    @Setter
    private int priority = 0;// 优先级 数值越小优先级越高
    private boolean keepReceiveCookie = false;
    @Setter
    private CookieStore cookieStore;
    @Setter
    private HttpHost proxy;

    @Getter
    private boolean needCheckRocord;// 检查记录，

    private void init() {
        headersLock = new ReentrantLock();
        requestHeaders = new HeaderGroup();
        // init HTTP headers
        requestHeaders.addHeader(new BasicHeader("accept",
                "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8"));
        requestHeaders.addHeader(new BasicHeader("accept-encoding", "gzip, deflate"));// error when add br ? TODO
        requestHeaders.addHeader(new BasicHeader("accept-language", "zh-CN,zh;q=0.9"));
        requestHeaders.addHeader(new BasicHeader("cache-control", "max-age=0"));
        requestHeaders.addHeader(new BasicHeader("Connection", "keep-alive"));
        requestHeaders.addHeader(new BasicHeader("upgrade-insecure-requests", "1"));
        requestHeaders.addHeader(new BasicHeader("User-Agent",
                "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.87 Safari/537.36"));
    }

    private Task(String name) {
        this.name = name;
        init();
    }

    public void addTotalCounts() {
        totalCounts.incrementAndGet();
    }

    public TaskType getType() {
        return type;
    }

    public HTTPMethodType getMethod() {
        return method;
    }

    public Header[] getRequestHeaders() {
        headersLock.lock();
        Header[] headers = null;
        try {
            headers = requestHeaders.getAllHeaders();
        } finally {
            headersLock.unlock();
        }
        return headers;
    }

    public boolean removeHeader(String key) {
        boolean result = false;
        headersLock.lock();
        try {
            for (Header h : requestHeaders.getHeaders(key)) {
                requestHeaders.removeHeader(h);
                result = true;
            }
        } finally {
            headersLock.unlock();
        }
        return result;
    }

    public void addHeader(String key, String value) {
        headersLock.lock();
        try {
            requestHeaders.addHeader(new BasicHeader(key, value));
        } finally {
            headersLock.unlock();
        }
    }

    /*
     *  实现优先级越高，在有序列表中的排序越靠前(non-Javadoc)
     * 对Java中的有序列表，compareTo的值越低越靠前 
     *  
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    @Override
    public int compareTo(Task o) {
        return o.getPriority() - this.getPriority();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof Task) {
            Task tmp = (Task) o;
            if (this.getName().equals(tmp.getName())) {
                return true;
            }
        }
        return false;
    }

    public static class Builder {
        private Task task;
        private int requestTimeout = DEFAULT_REQUEST_TIMEOUT;
        private int timeout = DEFAULT_TIMEOUT;
        private CrawlerJob job;

        private Builder(String taskName, CrawlerJob job) {
            this.task = new Task(taskName);
            this.job = job;;
        }

        public static Builder begin(String taskName) {
            return new Builder(taskName, null);
        }
        
        public static Builder begin(String taskName, CrawlerJob job) {
            return new Builder(taskName, job);
        }

        public Builder withPriority(int priority) {
            task.priority = priority;
            return this;
        }

        public Builder withType(TaskType type) {
            task.type = type;
            return this;
        }

        public Builder withMethod(String method) {
            task.method = Optional.ofNullable(HTTPMethodType.valueOf(method)).orElseGet(() -> {
                throw new RuntimeException("Not Support This Method: " + method);
            });
            return this;
        }

        public Builder withResultType(TYPE type) {
            task.resultType = type;
            return this;
        }

        public Builder withResultHandler(ResultHandler h) {
            task.resultHandler = h;
            return this;
        }

        private void createIfNotExist() {
            task.customInterceptors = Optional.ofNullable(task.customInterceptors).orElse(new LinkedList<>());
            task.crawlerExceptionHandler = Optional.ofNullable(task.crawlerExceptionHandler).orElse(new LinkedList<>());
        }

        public Builder withRequestHeaders(Header header) {
            task.requestHeaders.addHeader(header);
            return this;
        }

        public Builder withCrawlerInterceptor(CrawlerInterceptor handler) {
            createIfNotExist();
            task.customInterceptors.add(handler);
            return this;
        }

        public Builder withLogDetail() {
            createIfNotExist();
            task.customInterceptors.add(new LogInterceptor());
            task.crawlerExceptionHandler.add(new LogCrawlerExceptionHandler());
            return this;
        }

        public Builder withBlockingWait(long millis) {
            createIfNotExist();
            task.customInterceptors.add(new SpecificTaskBlockingWaitInterceptor(task, millis));
            return this;
        }

        public Builder withHTTPProxy(String hostname, int port) {
            task.proxy = new HttpHost(hostname, port);
            return this;
        }

        public Builder withDefaultMaxRetires(int times, long deferrals) {
            task.defaultMaxRetries = times;
            task.defaultRetriesDeferrals = deferrals;
            return this;
        }

        /**
         * 设置从连接池获取连接的时限
         */
        public Builder withRequestTimeout(int timeout) {
            requestTimeout = timeout;
            return this;
        }

        /**
         * 设置连接的时限
         */
        public Builder withTimeout(int timeout) {
            this.timeout = timeout;
            return this;
        }

        public Builder withKeepReceiveCookie() {
            task.keepReceiveCookie = true;
            withCrawlerInterceptor(new CookieInterceptor());// 增加拦截器
            return this;
        }

        public Builder withCheckRecord() {
            task.needCheckRocord = true;
            return this;
        }

        private void buildDefaultIfNeed() {
            task.requestConfigBuilder = RequestConfig.custom()
                    .setProxy(task.proxy)
                    .setConnectionRequestTimeout(requestTimeout)
                    .setConnectTimeout(timeout);
        }

        public Task build() {
            createIfNotExist();
            buildDefaultIfNeed();
            return task;
        }
        
        public CrawlerJob and() {
            Objects.requireNonNull(job);
            build();
            job.register(task);
            return job;
        }
        
    }

}
