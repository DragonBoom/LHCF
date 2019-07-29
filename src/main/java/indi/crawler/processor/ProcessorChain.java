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

    private void init(CrawlerController controller) {
        exceptionHandler = new BasicExceptionHandler();// TODO

        executorMap.put(TaskType.HTTP_TOPICAL, new HTTPProcessorExecutor(controller));
    }

    public ProcessorChain(CrawlerController controller) {
        init(controller);
    }

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
     * 初始化Task的处理器链并返回
     */
    private synchronized List<Processor> initInterceptors(TaskDef task) {
        List<Processor> processors = task.getCrawlerProcessors();

        if (processors != null) {
            return processors;
        }
        List<Processor> customInterceptors = task.getCustomProcessors();
        processors = new ArrayList<>(customInterceptors.size() + 1);// 1 for connection processor

        ProcessorExecutor executor = getExecutor(task);
        Processor connectionProcessor = executor.getConnectionProcessor();
        // 添加自定义处理器
        processors.addAll(customInterceptors);
        // 添加连接处理器到List末端
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

    private ProcessorExecutor getExecutor(TaskDef task) {
        return executorMap.get(task.getType());
    }
}
