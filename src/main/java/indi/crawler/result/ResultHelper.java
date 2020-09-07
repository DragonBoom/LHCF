package indi.crawler.result;

import java.util.List;

import org.apache.http.HttpEntity;

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
    
    /**
     * 添加新任务
     * 
     * @param taskName
     * @param uri
     * @param requestEntity TODO
     * @param arg 可自行定义的传递用参数
     * @author DragonBoom
     * @since 2020.09.04
     */
    void addNewTask(String taskName, String uri, HttpEntity requestEntity, Object arg);
    
    List<Task> getNewTasks();
}
