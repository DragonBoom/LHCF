package indi.crawler.result;

import java.util.List;

import indi.crawler.task.Task;

/**
 * 处理结果时的工具
 * 
 * @author DragonBoom
 *
 */
public interface ResultHelper {

    void addNewTask(String taskName, String uri);
    
    /**
     * 添加新任务
     * 
     * @param taskName
     * @param uri
     */
    void addNewTask(String taskName, String uri, String requestEntity);
    
    List<Task> getNewTasks();
}
