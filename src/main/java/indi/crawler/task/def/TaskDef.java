package indi.crawler.task.def;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.HeaderGroup;

import indi.crawler.bootstrap.CrawlerJob;
import indi.crawler.cookies.CookieStore;
import indi.crawler.exception.ExceptionHandler;
import indi.crawler.exception.LogExceptionHandler;
import indi.crawler.processor.Processor;
import indi.crawler.processor.http.CookieProcessor;
import indi.crawler.processor.http.LogProcessor;
import indi.crawler.processor.http.RedisCacheProcessor;
import indi.crawler.result.ResultHandler;
import indi.crawler.task.ResponseEntity.TYPE;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * 该类描述某一“类”处理url外其他因素相同的爬虫任务，相当于爬虫任务的模板
 * 
 * @author DragonBoom
 *
 */
@Getter
//@Setter
@ToString
public class TaskDef implements Comparable<TaskDef>, Serializable {
    private static final long serialVersionUID = 1L;
    private static final int DEFAULT_REQUEST_TIMEOUT = 4000;
    private static final int DEFAULT_TIMEOUT = 4000;
    private ResultHandler resultHandler;
    private HTTPMethodType method = HTTPMethodType.GET;
    /** 用于辨识Task身份 */
    private String name;
    /** HTTP请求头部，线程安全？！ */
    private HeaderGroup requestHeaders;
    private Lock headersLock;
    private RequestConfig.Builder requestConfigBuilder;
    // Specific HTTP HOST
    private HttpHost host;
    /** 默认最大重试次数 */
    private int defaultMaxRetries = 3; // 默认最多尝试3次
    /** 默认重试超过限制次数后等待的时间 */
    private long defaultRetriesDeferrals = 10000L; // 尝试时每次等待十秒
    /** 历史执行任务数 */
    private AtomicLong totalCounts = new AtomicLong();
    private List<Processor> customProcessors = new LinkedList<>();// 用户配置的任务级别拦截器
    @Setter private List<Processor> crawlerProcessors;// 真正的拦截器
    private List<ExceptionHandler> crawlerExceptionHandler = new LinkedList<>();
    private TaskType type = TaskType.HTTP_TOPICAL;
    private TYPE resultType = TYPE.String;
    private Charset resultStringCharset;
    @Setter private int priority = 0;// 优先级 数值越小优先级越高
    private boolean keepReceiveCookie = false;
    @Setter private CookieStore cookieStore;
    @Setter private HttpHost proxy;
    private boolean needCheckRecord;// 是否需要检查记录，以避免重复请求
    private BiFunction<String, HttpRequestBase, String> idKeyGenerator;// <url, result, key>
    private String tmpDir;// 临时文件的存储路径（最好和实际存储路径放在同一磁盘，以便于移动文件）
    private Function<String, Boolean> checkFun;// 检查是否需要执行任务的函数
    @Getter private String redisCacheUri;
    
    public enum HTTPMethodType {
        GET, POST, PUT, PATCH, DELETE;
    }

    private void init() {
        headersLock = new ReentrantLock();
        requestHeaders = new HeaderGroup();
        // init HTTP headers
        requestHeaders.addHeader(new BasicHeader("accept",
                "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3"));
        requestHeaders.addHeader(new BasicHeader("accept-encoding", "gzip, deflate"));// error when `gzip, deflate, br` ? TODO Brotli
        requestHeaders.addHeader(new BasicHeader("accept-language", "zh-CN,zh;q=0.9"));
        requestHeaders.addHeader(new BasicHeader("cache-control", "no-cache"));
        requestHeaders.addHeader(new BasicHeader("pragma", "no-cache"));
//        requestHeaders.addHeader(new BasicHeader("Connection", "keep-alive"));
        requestHeaders.addHeader(new BasicHeader("upgrade-insecure-requests", "1"));
        requestHeaders.addHeader(new BasicHeader("User-Agent",
                "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.87 Safari/537.36"));
    }

    private TaskDef(String name) {
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
    public int compareTo(TaskDef o) {
        return o.getPriority() - this.getPriority();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof TaskDef) {
            TaskDef tmp = (TaskDef) o;
            if (this.getName().equals(tmp.getName())) {
                return true;
            }
        }
        return false;
    }

    public static class Builder {
        private TaskDef taskDef;
        private int requestTimeout = DEFAULT_REQUEST_TIMEOUT;
        private int timeout = DEFAULT_TIMEOUT;
        private CrawlerJob job;

        private Builder(String taskName, CrawlerJob job) {
            this.taskDef = new TaskDef(taskName);
            this.job = job;;
        }

        public static Builder begin(String taskName) {
            return new Builder(taskName, null);
        }
        
        public static Builder begin(String taskName, CrawlerJob job) {
            return new Builder(taskName, job);
        }

        /**
         * 设置优先级。数值越大，优先级越高
         * 
         * @param priority
         * @return
         */
        public Builder withPriority(int priority) {
            taskDef.priority = priority;
            return this;
        }

        public Builder withType(TaskType type) {
            taskDef.type = type;
            return this;
        }

        public Builder withMethod(String method) {
            taskDef.method = Optional.ofNullable(HTTPMethodType.valueOf(method)).orElseGet(() -> {
                throw new RuntimeException("Not Support This Method: " + method);
            });
            return this;
        }

        /**
         * 默认为String
         * 
         * @param type
         * @return
         */
        public Builder withResultType(TYPE type) {
            taskDef.resultType = type;
            return this;
        }
        
        public Builder withResultCharset(Charset charset) {
            taskDef.resultStringCharset = charset;
            return this;
        }

        public Builder withResultHandler(ResultHandler h) {
            taskDef.resultHandler = h;
            return this;
        }

        public Builder withRequestHeaders(Header header) {
            taskDef.requestHeaders.addHeader(header);
            return this;
        }

        public Builder withCrawlerInterceptor(Processor handler) {
            taskDef.customProcessors.add(handler);
            return this;
        }

        public Builder withLogDetail() {
            taskDef.customProcessors.add(new LogProcessor());
            taskDef.crawlerExceptionHandler.add(new LogExceptionHandler());
            return this;
        }
        
        /**
         * @param redisURI "redis://password@localhost:6379/0"
         * @return
         */
        public Builder withRedisCache(String redisURI) {
            if (redisURI != null) {
                taskDef.redisCacheUri = redisURI;
                taskDef.customProcessors.add(new RedisCacheProcessor(redisURI));
            }
            return this;
        }

        public Builder withHTTPProxy(String hostname, int port) {
            taskDef.proxy = new HttpHost(hostname, port);
            return this;
        }

        public Builder withDefaultMaxRetires(int times, long deferrals) {
            taskDef.defaultMaxRetries = times;
            taskDef.defaultRetriesDeferrals = deferrals;
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
            taskDef.keepReceiveCookie = true;
            withCrawlerInterceptor(new CookieProcessor());// 增加拦截器
            return this;
        }

        public Builder withCheckRecord() {
            taskDef.needCheckRecord = true;
            return this;
        }
        
        /**
         * 为当前任务配置爬虫的id计算函数，将根据该函数计算缓存时的key。使用该方法以规避资源地址变动导致的缓存不命中。
         */
        public Builder withKeyGenerator(BiFunction<String, HttpRequestBase, String> idKeyGenerator) {
            taskDef.idKeyGenerator = idKeyGenerator;
            return this;
        }
        
        /**
         * 设置使用的临时文件夹（仅当ResultType为File时才有效）
         * 
         * @param tmpDir
         * @return
         */
        public Builder withTmpDir(String tmpDir) {
            taskDef.tmpDir = tmpDir;
            return this;
        }
        
        /**
         * 
         * @param checkFun 返回false时跳过该爬虫任务
         * @return
         */
        public Builder withURIChecker(Function<String, Boolean> checkFun) {
            taskDef.checkFun = checkFun;
            return this;
        }
        
        /**
         * 启用并设置当前任务的阻塞执行时间；即设置多久执行一次该任务，可通过该配置避免因访问频率过高而被ban。
         * 
         * @author DragonBoom
         * @since 2020.03.25
         * @param millis
         * @return
         */
        public Builder withBlockingMillis(long millis) {
            job.getBlockingWaitFilter().addBlock(taskDef, millis);
            
            return this;
        }
        
        volatile boolean builded = false;
        
        /**
         * 构建爬虫任务定义
         * 
         * @return
         */
        public TaskDef build() {
            if (builded) {
                return taskDef;
            }
            builded = true;
            // 构建HttpClient的请求配置
            taskDef.requestConfigBuilder = RequestConfig.custom()
                    .setProxy(taskDef.proxy)// nullable
                    .setConnectionRequestTimeout(requestTimeout)
                    .setConnectTimeout(timeout);
            return taskDef;
        }
        
        /**
         * 向CrawlerJob注册该任务，并返回CrawlerJob以继续链式调用
         * @return
         */
        public CrawlerJob and() {
            Objects.requireNonNull(job);
            job.register(build());
            return job;
        }
        
    }

}
