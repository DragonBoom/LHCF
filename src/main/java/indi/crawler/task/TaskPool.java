package indi.crawler.task;

import indi.util.Message;

public interface TaskPool extends Message {

    boolean offer(Task task);
    
    Task poll();
    
    Task[] cloneLeased();
    
    /**
     * 移除出租的爬虫
     */
    boolean removeLeased(Task ctx);
    
    int getLeasedSize();
    
    /**
     * 可以执行的爬虫任务数
     */
    int workableSize();
    
    /**
     * 可以马上执行的爬虫任务数
     * @return
     */
    int availableSize();
    
    boolean isEmpty();
}
