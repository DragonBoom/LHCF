package indi.crawler.processor.http;

import indi.crawler.processor.Processor;
import indi.crawler.processor.ProcessorContext;
import indi.crawler.processor.ProcessorResult;
import indi.data.ThrowableBiFunction;

/**
 * 
 * @author DragonBoom
 * @since 2019.05.02
 *
 */
public abstract class HttpProcessor extends Processor {

    public ProcessorResult beforeAll(ProcessorContext pCtx) throws Throwable {
        ProcessorResult result = beforeAll0(pCtx);
        return process0(pCtx, result, (nextProcessor, ctx) -> nextProcessor.beforeAll(ctx));
    }

    /**
     * 
     * @param pCtx
     * @return
     * @throws Throwable
     */
    protected ProcessorResult beforeAll0(ProcessorContext pCtx) throws Throwable {
        return ProcessorResult.KEEP_GOING;
    }

    public ProcessorResult executeRequest(ProcessorContext pCtx) throws Throwable {
        ProcessorResult result = executeRequest0(pCtx);
        return process0(pCtx, result, (nextProcessor, ctx) -> nextProcessor.executeRequest(ctx));
    }

    protected ProcessorResult executeRequest0(ProcessorContext pCtx) throws Throwable {
        return ProcessorResult.KEEP_GOING;
    }

    public ProcessorResult receiveResponse(ProcessorContext pCtx) throws Throwable {
        ProcessorResult result = receiveResponse0(pCtx);
        return process0(pCtx, result, (nextProcessor, ctx) -> nextProcessor.receiveResponse(ctx));
    }

    protected ProcessorResult receiveResponse0(ProcessorContext pCtx) throws Throwable {
        return ProcessorResult.KEEP_GOING;
    }

    public ProcessorResult handleResult(ProcessorContext pCtx) throws Throwable {
        ProcessorResult result = handleResult0(pCtx);
        return process0(pCtx, result, (nextProcessor, ctx) -> nextProcessor.handleResult(ctx));
    }

    protected ProcessorResult handleResult0(ProcessorContext pCtx) throws Throwable {
        return ProcessorResult.KEEP_GOING;
    }

    public ProcessorResult afterHandleResult(ProcessorContext pCtx) throws Throwable {
        ProcessorResult result = afterHandleResult0(pCtx);
        return process0(pCtx, result, (nextProcessor, ctx) -> nextProcessor.afterHandleResult(ctx));
    }

    protected ProcessorResult afterHandleResult0(ProcessorContext pCtx) throws Throwable {
        return ProcessorResult.KEEP_GOING;
    }

    private ProcessorResult process0(ProcessorContext pCtx, ProcessorResult result,
            ThrowableBiFunction<HttpProcessor, ProcessorContext, ProcessorResult> keepGoingFunForNext)
            throws Throwable {
        if (result.getResult() == ProcessorResult.Result.KEEP_GOING && next != null) {
            Processor next = getNext();
            if (next instanceof HttpProcessor) {
                // 执行通过参数传入的函数
                return keepGoingFunForNext.apply((HttpProcessor) next, pCtx);
            } else {
                throw new IllegalArgumentException("该处理器不是HTTP处理器: " + next);
            }
        } else {
            return result;
        }
    }

}
