/**
 * 
 */
package indi.crawler.filter;

import indi.crawler.task.CrawlerController;
import indi.crawler.task.Task;
import indi.crawler.task.def.TaskDef;

/**
 * @author wzh
 * @since 2020.03.25
 */
public abstract class TaskFilter {
    
    /**
     * 根据爬虫定义，判断是否要执行爬虫任务
     * 
     * @author DragonBoom
     * @since 2020.03.25
     */
    public abstract boolean isExecute(TaskDef taskDef, Thread thread, CrawlerController controller);
    
    /**
     * 在执行爬虫任务前的预处理，此时已获取了爬虫任务。默认不做任何事情
     * 
     * @author DragonBoom
     * @since 2020.03.25
     * @return 将执行返回的Task；若返回null则不执行
     */
    public Task preHandle(Task task, Thread thread) {return task;}
}
