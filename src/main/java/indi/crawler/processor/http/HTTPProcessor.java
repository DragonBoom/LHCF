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
public abstract class HTTPProcessor extends Processor {

    public ProcessorResult beforeAll(ProcessorContext pCtx) throws Throwable {
        ProcessorResult result = beforeAll0(pCtx);
        return continueNext(pCtx, result, (nextProcessor, ctx) -> nextProcessor.beforeAll(ctx));
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
        return continueNext(pCtx, result, (nextProcessor, ctx) -> nextProcessor.executeRequest(ctx));
    }

    protected ProcessorResult executeRequest0(ProcessorContext pCtx) throws Throwable {
        return ProcessorResult.KEEP_GOING;
    }

    public ProcessorResult receiveResponse(ProcessorContext pCtx) throws Throwable {
        ProcessorResult result = receiveResponse0(pCtx);
        return continueNext(pCtx, result, (nextProcessor, ctx) -> nextProcessor.receiveResponse(ctx));
    }

    protected ProcessorResult receiveResponse0(ProcessorContext pCtx) throws Throwable {
        return ProcessorResult.KEEP_GOING;
    }

    public ProcessorResult handleResult(ProcessorContext pCtx) throws Throwable {
        ProcessorResult result = handleResult0(pCtx);
        return continueNext(pCtx, result, (nextProcessor, ctx) -> nextProcessor.handleResult(ctx));
    }

    protected ProcessorResult handleResult0(ProcessorContext pCtx) throws Throwable {
        return ProcessorResult.KEEP_GOING;
    }

    public ProcessorResult afterHandleResult(ProcessorContext pCtx) throws Throwable {
        ProcessorResult result = afterHandleResult0(pCtx);
        return continueNext(pCtx, result, (nextProcessor, ctx) -> nextProcessor.afterHandleResult(ctx));
    }

    protected ProcessorResult afterHandleResult0(ProcessorContext pCtx) throws Throwable {
        return ProcessorResult.KEEP_GOING;
    }

    /**
     * 若本处理返回结果是继续处理，则继续执行下一处理器，否则返回处理结果
     * 
     * @param pCtx
     * @param result
     * @param keepGoingFunForNext 当本处理器的处理结果是继续执行时，对下一处理器执行的函数
     * @return
     * @throws Throwable
     */
    private ProcessorResult continueNext(ProcessorContext pCtx, ProcessorResult result,
            ThrowableBiFunction<HTTPProcessor, ProcessorContext, ProcessorResult> keepGoingFunForNext)
            throws Throwable {
        if (result.getResult() == ProcessorResult.Result.KEEP_GOING && next != null) {
            Processor next = getNext();
            if (next instanceof HTTPProcessor) {
                return keepGoingFunForNext.apply((HTTPProcessor) next, pCtx);
            } else {
                throw new IllegalArgumentException("该处理器不是HTTP处理器: " + next);
            }
        } else {
            return result;
        }
    }

}
