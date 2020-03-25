package indi.crawler.bootstrap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.http.HttpHost;

import indi.crawler.cookies.CookieStore;
import indi.crawler.cookies.MemoryCookieStore;
import indi.crawler.filter.BlockingWaitFilter;
import indi.crawler.monitor.CloseableMonitor;
import indi.crawler.monitor.ContextPoolMonitor;
import indi.crawler.task.CrawlerController;
import indi.crawler.task.Task;
import indi.crawler.task.def.SpecificTask;
import indi.crawler.task.def.TaskDef;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * 爬虫项目启动类
 * 
 * @author dragonboom
 *
 */
@Slf4j
public class CrawlerJob {
    private Map<String, TaskDef> tasks;
    private Task seed;
    private CrawlerController controller;// 懒加载，以确保其他配置好后再初始化控制器
    private CookieStore cookieStore;
    private HttpHost proxy;
    private boolean descPriority;
    @Getter
    private String redisURI;
    @Getter
    private String tmpFolderPath;// 临时文件夹路径
    @Getter
    private Runnable closeCallbackFun;// 结束时的回调（为空则啥也不做）
    @Getter
    private BlockingWaitFilter blockingWaitFilter;
    @Setter
    @Getter
    private boolean enableBlkckingWait = false;
    
    /** 函数：结束整个系统 */
    public static final Runnable CLOSE_SYSTEM = () -> System.exit(0);

    /**
     * 初始化爬虫项目
     */
    private void init() {
        cookieStore = new MemoryCookieStore();
        tasks = new ConcurrentHashMap<>();
        blockingWaitFilter = new BlockingWaitFilter();
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
        if (tasks.containsKey(name)) {
            System.exit(-1);// warn ! ! !
            throw new RuntimeException("Duplicate task name " + name);
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
        tasks.put(name, taskDef);
    }

    public synchronized TaskDef getTaskDef(String name) {
        return tasks.get(name);
    }

    public CrawlerController getController() {
        return controller;
    }

    public CookieStore getCookieStore() {
        return cookieStore;
    }

    public Task getSeed() {
        return seed;
    }
    private static final Long CONTEXT_POOL_MONITOR_SLEEP_MILLIS = 5000L;

    /**
     * 从种子URI开始执行本项任务
     * 
     * @return
     */
    public synchronized boolean start(String taskName, String seedUri, String entity) {
        if (controller == null) {// 这里实际上是CrawlerController唯一的初始逻辑
            controller = new CrawlerController(this);// 这个其实不太合理。。。
            new CloseableMonitor(this);// 启用关闭监视器
            
            if (enableBlkckingWait) {
                controller.addFilter(blockingWaitFilter);
            }
        }
        seed = new SpecificTask(taskName, seedUri, entity).toCrawlerContext(controller);
        controller.offer(seed);
        // clear cache
        priorityCache = null;
        return true;
    }
    
    public boolean start(String taskName, String seedUri) {
        return start(taskName, seedUri, null);
    }
    
    /**
     * 添加任务的种子
     * 
     * @author DragonBoom
     * @since 2020.03.25
     * @param taskName
     * @param seedUri
     * @param entity
     * @return
     */
    public boolean addSpecificTask(String taskName, String seedUri, String entity) {
        taskName = Optional.ofNullable(taskName).orElse(tasks.entrySet().iterator().next().getKey());
        Task ctx = new SpecificTask(taskName, seedUri, entity).toCrawlerContext(controller);
        return controller.offer(ctx);
    }
    
    public boolean addSpecificTask(String seedUri) {
        return addSpecificTask(null, seedUri, null);
    }
    
    public boolean addSpecificTask(String seedUri, String entity) {
        return addSpecificTask(null, seedUri, entity);
    }
    
    /**
     * 启用爬虫池监视器
     * 
     * @author DragonBoom
     * @since 2020.03.25
     * @return
     */
    public CrawlerJob withContextPoolMonitor() {
        new ContextPoolMonitor(this, CONTEXT_POOL_MONITOR_SLEEP_MILLIS);// getter ?
        return this;
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
     * 启用关闭爬虫的监视器（现在无论是否调用本方法都会启动该监视器）
     * 
     * @return
     */
    public CrawlerJob withCloseableMonitor() {
        return this;
    }
    
    /**
     * 若注册的爬虫任务没有优先级，则自动为其设置根据注册时间，设置越来越小的优先级，默认为越来越大
     * 
     * @return
     */
    public CrawlerJob withDescPriorityOrder() {
        this.descPriority = true;
        return this;
    }
    
    /**
     * {@link CrawlerJob#withASCPriorityOrder()}
     */
    public TaskDef.Builder withTask(String taskName) {
        return TaskDef.Builder.begin(taskName, this);
    }
    
    /**
     * 使用redis消息队列作为爬虫任务池
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
        this.tmpFolderPath = pathStr;
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

}
