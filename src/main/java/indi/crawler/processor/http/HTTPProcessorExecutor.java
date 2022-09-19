package indi.crawler.processor.http;

import java.util.List;

import indi.crawler.processor.Processor;
import indi.crawler.processor.ProcessorContext;
import indi.crawler.processor.ProcessorExecutor;
import indi.crawler.processor.ProcessorResult;
import indi.crawler.processor.ProcessorResult.Result;
import indi.crawler.task.CrawlerController;
import indi.crawler.task.CrawlerStatus;
import indi.crawler.task.Task;
import lombok.Getter;
import lombok.Setter;

/**
 * HTTPProcessor链的启动器
 * 
 * @author wzh
 * @since 2020.01.18
 */
public class HTTPProcessorExecutor extends ProcessorExecutor {
    @Getter
    @Setter
    private HTTPProcessor connectionProcessor;

    public HTTPProcessorExecutor(CrawlerController controller) {
        this.connectionProcessor = new HTTPConnectionProcessor(controller);
    }

    @Override
    public ProcessorResult execute(ProcessorContext pCtx, List<Processor> processors) throws Throwable {
        Processor firstProcessor = processors.get(0);
        Task ctx = pCtx.getCrawlerContext();
        
        ProcessorResult result = null;
        if (firstProcessor instanceof HTTPProcessor) {
            HTTPProcessor firstHttpProcessor = (HTTPProcessor) firstProcessor;
            result = firstHttpProcessor.beforeAll(pCtx);
            if (isOver(result)) {
                return result;
            }
            
            result = firstHttpProcessor.executeRequest(pCtx);
            if (isOver(result)) {
                return result;
            }
            
            result = firstHttpProcessor.receiveResponse(pCtx);
            if (isOver(result)) {
                return result;
            }
            
            result = firstHttpProcessor.handleResult(pCtx);
            if (isOver(result)) {
                return result;
            }
            
            result = firstHttpProcessor.afterHandleResult(pCtx);
            if (isOver(result)) {
                return result;
            }
        } else {
            throw new IllegalArgumentException("该处理器不是HTTP处理器: " + firstProcessor);
        }
        finishCrawlerContext(ctx);
        return result;
    }

    
    /**
     * 结束爬虫上下文
     * 
     * @param ctx
     */
    private void finishCrawlerContext(Task ctx) {
        ctx.getController().getTaskPool().remove(ctx);
        // 置为FINISHED状态后，将由JVMMonitor做进一步处理
        ctx.checkAndSetStatus(CrawlerStatus.FINISHED);
    }
    
    private boolean isOver(ProcessorResult result) {
        return result.getResult() == Result.OVER;
    }

}
