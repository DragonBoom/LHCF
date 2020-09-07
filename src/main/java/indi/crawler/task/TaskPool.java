package indi.crawler.task;

import indi.crawler.filter.TaskFilter;
import indi.obj.Message;

public interface TaskPool extends Message {

    /**
     * 新增爬虫任务，不能用于回收爬虫
     * 
     * @author DragonBoom
     * @param task
     * @return
     */
    boolean offer(Task task);
    
    /**
     * 延期执行爬虫任务，只允许添加状态为延期的任务 
     * 
     * @author DragonBoom
     * @param task
     * @param wakeUpTime TODO
     * @return 是否回收成功
     */
    boolean deferral(Task task, Long wakeUpTime);
    
    Task poll();
    
    /**
     * 返回当前等待队列数组，对该数组的操作是安全的，但需要注意内存开销！！
     * 
     */
    Task[] cloneLeased();
    
    /**
     * 移除出租的爬虫
     */
    boolean removeLeased(Task ctx);
    
    /**
     * 获取已出租的爬虫（正在工作的爬虫）数
     */
    int getLeasedSize();
    
    /**
     * 获取可以工作，但没有在工作的爬虫数
     */
    int workableSize();
    
    /**
     * 可以马上执行的爬虫任务数
     * @return
     */
    int availableSize();
    
    /**
     * 判断爬虫池是否为空
     * 
     * @return
     */
    boolean isEmpty();
    
    /**
     * 添加过滤器
     * 
     * @author DragonBoom
     * @since 2020.03.25
     * @param filter
     * @return
     */
    boolean addFilter(TaskFilter filter);
}
