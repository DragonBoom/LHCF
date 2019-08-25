package indi.crawler.result;

import java.net.URI;
import java.util.LinkedList;
import java.util.List;

import indi.crawler.task.Task;
import indi.crawler.task.TaskFactory;
import indi.crawler.task.def.TaskDef;

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
    public void addNewTask(String taskName, String uri, String requestEntity) {
        Task newTask = taskFactory.build(taskName, URI.create(uri), requestEntity);
        newTasks.add(newTask);
    }

    @Override
    public List<Task> getNewTasks() {
        return newTasks;
    }

}
