package indi.crawler.processor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import indi.crawler.exception.BasicExceptionHandler;
import indi.crawler.exception.ExceptionHandler;
import indi.crawler.processor.http.HTTPProcessorExecutor;
import indi.crawler.task.CrawlerController;
import indi.crawler.task.Task;
import indi.crawler.task.def.TaskDef;
import indi.crawler.task.def.TaskType;
import lombok.extern.slf4j.Slf4j;

/**
 * 具体的线程安全应该由各爬虫控制器自己来确保
 *
 * @author dragonboom
 *
 */
@Slf4j
public class ProcessorChain {
    private ExceptionHandler exceptionHandler;

    private ConcurrentHashMap<TaskType, ProcessorExecutor> executorMap = new ConcurrentHashMap<>();
    private CrawlerController controller;

    private void init(CrawlerController controller) {
        exceptionHandler = new BasicExceptionHandler();// TODO

        executorMap.put(TaskType.HTTP_TOPICAL, new HTTPProcessorExecutor(controller));
    }

    public ProcessorChain(CrawlerController controller) {
        this.controller = controller;
        init(controller);
    }

    /**
     * 执行爬虫任务
     *
     * @param ctx
     */
    public void process(Task ctx) {
        TaskDef task = ctx.getTaskDef();
        List<Processor> processors = task.getCrawlerProcessors();
        if (processors == null) {
            processors = initInterceptors(task);
        }
        ProcessorContext pCtx = new ProcessorContext(ctx);
        ProcessorExecutor executor = getExecutor(task);
        try {
            executor.execute(pCtx, processors);
        } catch (Throwable e) {
            exceptionHandler.handleException(pCtx, e);
        }
    }

    /**
     * 初始化Task的处理器链并返回。简单地在方法上加synchronized以应对并发问题。
     */
    private synchronized List<Processor> initInterceptors(TaskDef task) {
        List<Processor> processors = task.getCrawlerProcessors();

        if (processors != null) {
            return processors;
        }

        // 1. 添加自定义处理器
        List<Processor> taskProcessor = task.getCustomProcessors();
        List<Processor> jobProcessor = controller.getJob().getCustomProcessors();

        processors = new ArrayList<>(taskProcessor.size() + jobProcessor.size() + 1);// + 1 for connection processor
        // 先添加任务级别，再添加工程级别的处理器
        processors.addAll(taskProcessor);
        processors.addAll(jobProcessor);

        // 2. 添加连接处理器到List末端
        // 获取处理器的执行者
        ProcessorExecutor executor = getExecutor(task);
        // 获取处理连接的处理器
        Processor connectionProcessor = executor.getConnectionProcessor();
        // 将其放到执行链的末端
        processors.add(connectionProcessor);

        // 构建处理器链
        Object[] array = processors.toArray();
        if (array.length > 1) {
            // set next for all
            for (int i = 1; i < array.length; i++) {
                Processor prev = (Processor) array[i - 1];
                Processor now = (Processor) array[i];
                prev.setNext(now);
            }
        }

        log.info("构建 {} 处理器链：{}", task.getName(), processors);
        task.setCrawlerProcessors(processors);
        return processors;
    }

    /**
     * 获取爬虫任务的执行器
     *
     * @param task
     * @return
     */
    private ProcessorExecutor getExecutor(TaskDef task) {
        return executorMap.get(task.getType());
    }
}
