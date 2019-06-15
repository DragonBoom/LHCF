package indi.crawler.processor.http;

import java.util.List;

import indi.crawler.nest.CrawlerContext;
import indi.crawler.nest.CrawlerStatus;
import indi.crawler.nest.CrawlerThread;
import indi.crawler.processor.Processor;
import indi.crawler.processor.ProcessorContext;
import indi.crawler.processor.ProcessorExecutor;
import indi.crawler.processor.ProcessorResult;
import indi.crawler.processor.ProcessorResult.Result;
import lombok.Getter;
import lombok.Setter;

public class HTTPProcessorExecutor extends ProcessorExecutor {
    @Getter
    @Setter
    private HttpProcessor connectionProcessor;

    public HTTPProcessorExecutor() {
        this.connectionProcessor = new HTTPConnectionProcessor();
    }

    @Override
    public ProcessorResult execute(ProcessorContext pCtx, List<Processor> processors) throws Throwable {
        Processor firstProcessor = processors.get(0);
        CrawlerContext ctx = pCtx.getCrawlerContext();
        initCrawlerContext(ctx);
        
        ProcessorResult result = null;
        if (firstProcessor instanceof HttpProcessor) {
            HttpProcessor firstHttpProcessor = (HttpProcessor) firstProcessor;
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
    private void initCrawlerContext(CrawlerContext ctx) {
        ctx.setStatus(CrawlerStatus.RUNNING);
        ctx.setAttempts(ctx.getAttempts() + 1);
        ctx.getTask().addTotalCounts();
        ctx.setRegistration(System.currentTimeMillis());
        ctx.setThread((CrawlerThread) Thread.currentThread());
    }
    
    /**
     * 结束爬虫上下文
     * 
     * @param ctx
     */
    private void finishCrawlerContext(CrawlerContext ctx) {
        ctx.setStatus(CrawlerStatus.FINISHED);
        // 主动移除爬虫
        ctx.getController().getContextPool().removeLeased(ctx);
    }
    
    private boolean isOver(ProcessorResult result) {
        return result.getResult() == Result.OVER;
    }

}
