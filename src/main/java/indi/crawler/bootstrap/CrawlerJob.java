package indi.crawler.bootstrap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.http.HttpHost;

import indi.crawler.cookies.CookieStore;
import indi.crawler.cookies.MemoryCookieStore;
import indi.crawler.monitor.CloseableMonitor;
import indi.crawler.monitor.ContextPoolMonitor;
import indi.crawler.task.CrawlerController;
import indi.crawler.task.Task;
import indi.crawler.task.def.SpecificTask;
import indi.crawler.task.def.TaskDef;
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
    private CrawlerController controller;
    private CookieStore cookieStore;
    private HttpHost proxy;
    private boolean descPriority;

    /**
     * 初始化爬虫项目
     */
    private void init() {
        cookieStore = new MemoryCookieStore();
        tasks = new ConcurrentHashMap<>();
        controller = new CrawlerController(this);
        new CloseableMonitor(this);
    }

    public CrawlerJob() {
        init();
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
     * 开始本项任务
     * 
     * @return
     */
    public boolean start(String taskName, String seedUri, String entity) {
        seed = new SpecificTask(taskName, seedUri, entity).toCrawlerContext(controller);
        controller.offer(seed);
        // clear cache
        priorityCache = null;
        return true;
    }
    
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
    
    public CrawlerJob withContextPoolMonitor() {
        new ContextPoolMonitor(this, CONTEXT_POOL_MONITOR_SLEEP_MILLIS);// getter ?
        return this;
    }
    
    public CrawlerJob withHTTPProxy(String host, int port) {
        this.proxy = new HttpHost(host, port);
        return this;
    }

    public CrawlerJob withCloseableMonitor() {
                ; // TODO WARN
        return this;
    }
    
    /**
     * 若注册的爬虫任务没有优先级，则自动为其设置根据注册时间，设置越来越小的优先级，默认为越来越大
     * 
     * @return
     */
    public CrawlerJob withASCPriorityOrder() {
        this.descPriority = true;
        return this;
    }
    
    /**
     * {@link CrawlerJob#withASCPriorityOrder()}
     */
    public TaskDef.Builder withTask(String taskName) {
        return TaskDef.Builder.begin(taskName, this);
    }
    

}
