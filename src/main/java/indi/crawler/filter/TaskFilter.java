/**
 * 
 */
package indi.crawler.filter;

import indi.crawler.task.CrawlerController;
import indi.crawler.task.Task;

/**
 * 任务过滤器，作用于爬虫线程获取爬虫任务的这个环节
 * 
 * @author wzh
 * @since 2020.03.25
 */
public abstract class TaskFilter {
    
    /**
     * 判断是否要执行爬虫任务。注意，执行该方法时，爬虫任务已从爬虫池的可用队列中取出，但尚未加入出租队列，拦截器需要负责处理该
     * 爬虫任务；
     * 
     * 若拦截器不对爬虫任务进行回收，爬虫任务将被垃圾回收，即任务消失
     * 
     * @author DragonBoom
     * @since 2020.03.25
     */
    public abstract boolean isExecute(Task task, Thread thread, CrawlerController controller);
}
