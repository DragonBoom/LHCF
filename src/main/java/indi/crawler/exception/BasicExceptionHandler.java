package indi.crawler.exception;

import java.util.Arrays;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;

import org.apache.http.ConnectionClosedException;
import org.apache.http.NoHttpResponseException;
import org.apache.http.TruncatedChunkException;
import org.apache.http.client.ClientProtocolException;

import com.google.common.collect.ImmutableMap;

import indi.crawler.processor.ProcessorContext;
import indi.crawler.task.CrawlerStatus;
import indi.crawler.task.Task;
import indi.crawler.task.def.TaskDef;
import indi.exception.WrapperException;
import lombok.extern.slf4j.Slf4j;

/**
 * 基础的异常处理器
 * 
 * @author DragonBoom
 *
 */
@Slf4j
public class BasicExceptionHandler implements ExceptionHandler {
    /** 5s */
    private static final long DEFAULT_ADDITIONAL_WAIT_MAX_MILLIS = 5000;
    
    private ImmutableMap<Class<? extends Throwable>, BiFunction<Task, Throwable, HandleResult>> handlers; 
    
    private static final BiFunction<Task, Throwable, HandleResult> RECOVER_HANDLER = (ctx, e) -> {
        log.error("发生可回收异常，将回收爬虫并重试：{}", e.getMessage());
        return HandleResult.RECOVER;
    };
    
    protected void init() {
        // list -> 1
        // build Fun
        // build exception -> handler map
        handlers = ImmutableMap
                .<Class<? extends Throwable>, BiFunction<Task, Throwable, HandleResult>>builder()
                .put(ConnectionClosedException.class, RECOVER_HANDLER)
                .put(SSLHandshakeException.class, RECOVER_HANDLER)
                .put(SSLException.class, RECOVER_HANDLER)
                .put(NoHttpResponseException.class, RECOVER_HANDLER)
                .put(TruncatedChunkException.class, RECOVER_HANDLER)
                .put(ClientProtocolException.class, (ctx, e) -> {
                    // 报该异常表示URI格式有问题？
                    log.warn("ClientProtocolException: {}", ctx.getUri());
                    ctx.checkAndSetStatus(CrawlerStatus.INTERRUPTED);// 标记任务无法完成，等待被回收
                    return HandleResult.NOT_RECOVER;
                })
                .put(RuntimeException.class, (ctx, e) -> {
                    log.error("该异常无法处理，即将终止爬虫任务 {} \n {}", e, Arrays.stream(e.getStackTrace()).collect(Collectors.toList()));
                    e.printStackTrace();
                    ctx.checkAndSetStatus(CrawlerStatus.INTERRUPTED);// 标记任务无法完成，等待被回收
                    return HandleResult.NOT_RECOVER;
                })
                .build();
    }
    
    public BasicExceptionHandler() {
        init();
    }

    @Override
    public void handleException(ProcessorContext hCtx, Throwable throwable) throws AbortTaskException {
        Task ctx = hCtx.getCrawlerContext();
        
        // 还原被 WrapperException 封装的异常
        if (throwable instanceof WrapperException) {
            throwable = Optional.ofNullable(throwable.getCause()).orElse(throwable);
        }
        // 2021.12.11
        Class<? extends Throwable> eClass = throwable.getClass();
        if (eClass.equals(AbortTaskException.class)) {
            throw (AbortTaskException) throwable;
        }
        
        ctx.checkAndSetStatus(CrawlerStatus.PENDING);
        ctx.addThrowables(throwable);
        log.warn("处理异常({}) message={}", eClass, throwable.getMessage());
        TaskDef task = ctx.getTaskDef();
        // 尝试用已有的处理器去处理异常，若找不到对应的处理器，再用默认的处理器处理
        BiFunction<Task, Throwable, HandleResult> handler = handlers.getOrDefault(eClass, (ctx0, e) -> {
            // 以集合的形式输出异常栈到日志中
            log.error("该异常目前尚无法处理，将尝试再次处理-{} {} \n {}", task.getName(), e,
                    Optional.ofNullable(e.getStackTrace())
                        .map(s -> Arrays.stream(s).collect(Collectors.toList()))
                        .orElse(null));
            // 若捕获的异常无法处理，则标记任务无法完成，等待被回收
            e.printStackTrace();
            ctx0.checkAndSetStatus(CrawlerStatus.INTERRUPTED);
            return HandleResult.RECOVER;
        });
        
        HandleResult handleResult = handler.apply(ctx, throwable);
        // 若处理器处理的结果是回收，则开始走回收流程，尝试再次执行任务
        if (handleResult == HandleResult.RECOVER) {
            int attempts = ctx.getAttempts();
            attempts++;
            // 计算额外等待时间
            long totalCounts = ctx.getTaskDef().getTotalCounts().get();
            
            if (attempts <= ctx.getMaxRetries()) {
                // 没有超过最大重试次数，将任务放到爬虫池的延时队列中
                ctx.checkAndSetStatus(CrawlerStatus.DEFERRED);
                // 直接线程等待很浪费资源，故将其放进等待队列，需要取值时才去判断一次是否过了等待时间
                // 等待时间 = context的等待时间 + 额外等待时间
                long additionalWaitMillis =  attempts / totalCounts
                        * DEFAULT_ADDITIONAL_WAIT_MAX_MILLIS;// 额外等待时间:根据任务重试次数计算，尝试越多次，等待越久
                long wakeUpTime = System.currentTimeMillis() + ctx.getRetryDeferrals() + additionalWaitMillis;
                ctx.setPriority(ctx.getTaskDef().getPriority() + ctx.getPriority());
                log.info("回收{}爬虫(第 {} / {} 次)：{}", ctx.getTaskDef().getName(), ctx.getAttempts(), ctx.getMaxRetries(), ctx.getMessage());
                ctx.getController().deferral(ctx, wakeUpTime);
            } else {
                // 超过了最大重试次数，则标记任务无法完成，等待被回收
                log.error("任务超过最大重试次数：{} 次，将停止工作!! 最后一次尝试抛出的异常为：{}", ctx.getMaxRetries(), throwable);
                ctx.checkAndSetStatus(CrawlerStatus.INTERRUPTED);
                // 若此前曾经被回收，此时爬虫可能已经不存在于出租队列中（要看爬虫池的具体实现）
            }
        }
    }
    
    public enum HandleResult {
        RECOVER, NOT_RECOVER
    }
}
