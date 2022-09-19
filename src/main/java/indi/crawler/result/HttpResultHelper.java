package indi.crawler.result;

import java.io.Serializable;
import java.net.URI;
import java.util.LinkedList;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.http.HttpEntity;

import indi.crawler.task.Task;
import indi.crawler.task.TaskFactory;

/**
 * 用于在ResultHandler中添加新任务的中间类
 * 
 * @author DragonBoom
 * @since 2019.10.26
 */
public class HttpResultHelper implements ResultHelper {
    private TaskFactory taskFactory;
    private List<Task> newTasks;
    
    private void init() {
        this.newTasks = new LinkedList<>();
    }

    public HttpResultHelper(TaskFactory taskFactory) {
        this.taskFactory = taskFactory;
        init();
    }

    @Override
    public void addNewTask(String taskName, String uri) {
        addNewTask(taskName, uri, null);
    }

    @Override
    @Deprecated
    public void addNewTask(String taskName, String uri, String stringContent) {
        Task newTask = taskFactory.build(taskName, URI.create(uri), stringContent);
        newTasks.add(newTask);
    }
    
    @Override
    public void addNewTask(String taskName, String uri, @Nullable HttpEntity requestEntity, Serializable arg) {
        Task newTask = taskFactory.build(taskName, URI.create(uri), requestEntity);
        newTask.setArg(arg);
        newTasks.add(newTask);
    }

    @Override
    public List<Task> getNewTasks() {
        return newTasks;
    }
}
