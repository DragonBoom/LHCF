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
import indi.crawler.thread.CrawlerThread;
import lombok.Getter;
import lombok.Setter;

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
        initCrawlerContext(ctx);
        
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
     * 初始化爬虫上下文
     * 
     * @param ctx
     */
    private void initCrawlerContext(Task ctx) {
        ctx.setStatus(CrawlerStatus.RUNNING);
        ctx.setAttempts(ctx.getAttempts() + 1);
        ctx.getTaskDef().addTotalCounts();
        ctx.setRegistration(System.currentTimeMillis());
        ctx.setThread((CrawlerThread) Thread.currentThread());
    }
    
    /**
     * 结束爬虫上下文
     * 
     * @param ctx
     */
    private void finishCrawlerContext(Task ctx) {
        ctx.setStatus(CrawlerStatus.FINISHED);
        // 主动移除爬虫
        ctx.getController().getTaskPool().removeLeased(ctx);
    }
    
    private boolean isOver(ProcessorResult result) {
        return result.getResult() == Result.OVER;
    }

}
