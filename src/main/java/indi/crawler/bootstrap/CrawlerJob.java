package indi.crawler.bootstrap;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;

import indi.crawler.cookies.CookieStore;
import indi.crawler.cookies.MemoryCookieStore;
import indi.crawler.filter.BlockingWaitFilter;
import indi.crawler.monitor.CloseableMonitor;
import indi.crawler.processor.Processor;
import indi.crawler.processor.http.LogSpeedProcessor;
import indi.crawler.task.CrawlerController;
import indi.crawler.task.def.SpecificTask;
import indi.crawler.task.def.TaskDef;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * 爬虫项目启动类；该类的方法基本上都是线程不安全的
 * 
 * @author dragonboom
 *
 */
@Slf4j
public class CrawlerJob {
    private static final int DEFAULT_THREAD_POOL_SIZE = 3;
    private Map<String, TaskDef> taskDefs;
    @Getter
    private List<SpecificTask> seeds;// 启动时执行的任务
    @Getter
    private CrawlerController controller;// 懒加载，以确保其他配置好后再初始化控制器
    @Getter
    private CookieStore cookieStore;
    private HttpHost proxy;
    private boolean descPriority;
    @Getter
    private String redisURI;
    @Getter
    private Path tmpFolder;// 临时文件夹路径
    @Getter
    private Runnable closeCallbackFun;// 结束时的回调（为空则啥也不做）
    @Getter
    private BlockingWaitFilter blockingWaitFilter;
    @Getter
    private List<Processor> customProcessors;// 用户配置的工程级别拦截器
    @Getter
    private int threadCount = DEFAULT_THREAD_POOL_SIZE;
    
    /**
     * 初始化值域
     */
    private void init() {
        taskDefs = new HashMap<>();
        seeds = new LinkedList<>();
        customProcessors = new LinkedList<>();
        blockingWaitFilter = new BlockingWaitFilter();
        cookieStore = new MemoryCookieStore();
    }

    private CrawlerJob() {
        init();
    }
    
    public static CrawlerJob build() {
        return new CrawlerJob();
    }
    
    private List<Integer> priorityCache = new ArrayList<>();

    public synchronized void register(TaskDef taskDef) {
        String name = taskDef.getName();
        if (taskDefs.containsKey(name)) {
            System.exit(-1);// warn ! ! !
            throw new IllegalArgumentException("Duplicate task name " + name);
        }
        int pri = taskDef.getPriority();
        // 为了能够让 TreeMap 准确一类一键，必须使每个Task的都有不一样的比较值
        
        while (priorityCache.contains(pri)) {
            if (!descPriority) {
                pri--;
            } else {
                pri++;
            }
        }
        taskDef.setPriority(pri);
        priorityCache.add(pri);
        
        if (taskDef.isKeepReceiveCookie()) {
            taskDef.setCookieStore(cookieStore);
        }
        
        // set proxy if not exist
        if (proxy != null && taskDef.getProxy() == null) {
            taskDef.setProxy(proxy);
            taskDef.getRequestConfigBuilder().setProxy(proxy);
        }
        //
        log.info("已注册爬虫任务定义 {}", taskDef.toString());
        
        // final
        taskDefs.put(name, taskDef);
    }

    public synchronized TaskDef getTaskDef(String name) {
        return taskDefs.get(name);
    }

    /**
     * 开始配置任务；{@link CrawlerJob#withASCPriorityOrder()}
     * 
     * 
     */
    public TaskDef.Builder withTask(String taskName) {
        return TaskDef.Builder.begin(taskName, this);
    }

    /**
     * 从种子URL开始执行本项任务；直接通过锁对象来保证线程安全；不能重复执行，会抛异常
     * 
     * <p>这里的各实例都存在解耦的空间，后续考虑进行优化 FIXME:
     * 
     * @return
     */
    public synchronized boolean start(@Nullable String taskName, String url, HttpEntity requestEntity) {
        if (controller == null) {// 这里实际上是CrawlerController唯一的初始逻辑
            controller = new CrawlerController(this);// 这个其实不太合理。。。
            new CloseableMonitor(this);// 启用结束监视器
            
            // TODO: 这里并不适合作为注册过滤器的入口，考虑设计专门的注册方式
            if (!blockingWaitFilter.isEmpty()) {
                controller.addFilter(blockingWaitFilter);// 启动阻塞过滤器
            }
        } else {
            throw new RuntimeException("Job Already Started !");
        }
        if (taskName != null) {
            SpecificTask seed = new SpecificTask(taskName, url, requestEntity);
            seeds.add(seed);
        }
        // 添加种子任务
        for (SpecificTask seed : seeds) {
            controller.offer(seed.toTask(controller));
        }
        // clear cache
        priorityCache = null;
        return true;
    }
    
    /**
     * 从特定URL开始执行爬虫工程
     * 
     * @param taskName
     * @param url
     * @param entity
     * @return
     */
    public boolean start(String taskName, String url) {
        return start(taskName, url, (HttpEntity) null);
    }
    
    /**
     * 启动工程，适用于手动添加任务的场景
     * 
     * @return
     * @author DragonBoom
     * @since 2020.09.14
     */
    public boolean start() {
        return start(null, null, null);
    }
    
    /**
     * 添加作用于指定URL的任务
     * 
     * @param taskName
     * @param url
     * @param entity
     * @return
     * @since 2020.03.25
     */
    public boolean addSpecificTask(String taskName, String url, HttpEntity requestEntity) {
        SpecificTask task = new SpecificTask(taskName, url, requestEntity);
        // 应该实现为，若Job尚未启动，先保存任务，而不是直接添加到爬虫池
        if (controller == null) {
            seeds.add(task);
            return true;
        } else {
            return controller.offer(task.toTask(controller));
        }
    }
    
    /**
     * 启用HTTP代理
     * @author DragonBoom
     * @since 2020.03.25
     * @param host
     * @param port
     * @return
     */
    public CrawlerJob withHTTPProxy(String host, int port) {
        this.proxy = new HttpHost(host, port);
        return this;
    }
    
    /**
     * 启用HTTP代理
     * @author DragonBoom
     * @since 2020.03.25
     * @param host
     * @param port
     * @param sure 用于在链式调用外判断
     * @return
     */
    public CrawlerJob withHTTPProxy(String host, int port, boolean sure) {
        if (sure) {
            this.proxy = new HttpHost(host, port);
        }
        return this;
    }
    
    /**
     * 若注册的爬虫任务没有优先级，则自动为其设置根据注册时间，设置越来越小的优先级；默认为越来越大
     * 
     * @return
     */
    public CrawlerJob withDescPriorityOrder() {
        this.descPriority = true;
        return this;
    }
    
    /**
     * 使用redis消息队列作为爬虫任务池。该方法仅设置redisURI，不会直接启用redis爬虫池
     * 
     * @param redisURI
     * @return
     */
    public CrawlerJob withRedisMQTaskPool(String redisURI) {
        this.redisURI = redisURI;
        return this;
    }
    
    /**
     * 指定临时文件夹
     * 
     * @param pathStr
     * @return
     */
    public CrawlerJob withTmpFolder(String pathStr) {
        this.tmpFolder = Paths.get(pathStr);
        return this;
    }
    
    /**
     * 指定临时文件夹
     * 
     * @param pathStr
     * @return
     */
    public CrawlerJob withTmpFolder(Path tmpFolder) {
        this.tmpFolder = tmpFolder;
        return this;
    }
    
    /**
     * 定义结束时的回调
     * 
     * @param fun
     * @return
     */
    public CrawlerJob withCloseCallback(Runnable fun) {
        this.closeCallbackFun = fun;
        return this;
    }
    
    public CrawlerJob withSpeedLog() {
        this.customProcessors.add(new LogSpeedProcessor());
        return this;
    }
    
    /**
     * 设置线程池的线程数量
     * 
     * @param count
     * @return
     * @author DragonBoom
     * @since 2020.09.15
     */
    public CrawlerJob withThreadCount(int threadCount) {
        this.threadCount = threadCount;
        return this;
    }

}
