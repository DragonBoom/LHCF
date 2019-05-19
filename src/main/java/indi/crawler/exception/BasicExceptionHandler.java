package indi.crawler.exception;

import java.util.Arrays;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;

import org.apache.http.ConnectionClosedException;
import org.apache.http.NoHttpResponseException;
import org.apache.http.TruncatedChunkException;
import org.apache.http.client.ClientProtocolException;

import com.google.common.collect.ImmutableMap;

import indi.crawler.interceptor.InterceptorContext;
import indi.crawler.nest.CrawlerContext;
import indi.crawler.nest.CrawlerStatus;
import lombok.extern.slf4j.Slf4j;

/**
 * 基础的异常处理器
 * 
 * @author DragonBoom
 *
 */
@Slf4j
public class BasicExceptionHandler implements CrawlerExceptionHandler {
    /** 5s */
    private static final long DEFAULT_ADDITIONAL_WAIT_MAX_MILLIS = 5000;
    
    private ImmutableMap<Class<? extends Throwable>, BiFunction<CrawlerContext, Throwable, HandleResult>> handlers; 
    
    protected void init() {
        // list -> 1
        // build Fun
        BiFunction<CrawlerContext, Throwable, HandleResult> recoverFun = (ctx, e) -> HandleResult.RECOVER;
        // build map
        handlers = ImmutableMap
                .<Class<? extends Throwable>, BiFunction<CrawlerContext, Throwable, HandleResult>>builder()
                .put(ConnectionClosedException.class, recoverFun)
                .put(SSLHandshakeException.class, recoverFun)
                .put(SSLException.class, recoverFun)
                .put(NoHttpResponseException.class, recoverFun)
                .put(TruncatedChunkException.class, recoverFun)
                .put(ClientProtocolException.class, (ctx, e) -> {
                    // 报该异常表示URI格式有问题？
                    log.warn("ClientProtocolException: {}", ctx.getUri());
                    ctx.setStatus(CrawlerStatus.INTERRUPTED);// 标记任务无法完成，等待被回收
                    return HandleResult.NOT_RECOVER;
                })
                .put(RuntimeException.class, (ctx, e) -> {
                    log.error("该异常无法处理，即将终止爬虫任务 {} \n {}", e, Arrays.stream(e.getStackTrace()).collect(Collectors.toList()));
                    e.printStackTrace();
                    return HandleResult.NOT_RECOVER;
                })
                .build();
                ;
    }
    
    public BasicExceptionHandler() {
        init();
    }

    @Override
    public void handleException(InterceptorContext hCtx, Throwable throwable) {
        CrawlerContext ctx = hCtx.getCrawlerContext();
        ctx.setStatus(CrawlerStatus.PENDING);
        ctx.addThrowables(throwable);

        BiFunction<CrawlerContext, Throwable, HandleResult> handler = handlers.getOrDefault(throwable.getClass(), (ctx0, e) -> {
            log.error("该异常无法处理，即将终止爬虫任务 {} \n {}", e, Arrays.stream(e.getStackTrace()).collect(Collectors.toList()));
            // 若捕获的异常无法处理，则标记任务无法完成，等待被回收
            e.printStackTrace();
            ctx0.setStatus(CrawlerStatus.INTERRUPTED);
            return HandleResult.NOT_RECOVER;
        });
        
        HandleResult handleResult = handler.apply(ctx, throwable);
        
        if (handleResult == HandleResult.RECOVER) {
            int attempts = ctx.getAttempts();
            attempts++;
            // 计算额外等待时间
            long totalCounts = ctx.getTask().getTotalCounts().get();
            
            if (attempts <= ctx.getMaxRetries()) {
                // 若没有超过最大重试次数，则将任务放到延时队列中
                ctx.setStatus(CrawlerStatus.DEFERRED);
                // 直接线程等待很浪费资源，故将其放进等待队列，需要取值时才去判断一次是否过了等待时间
                // 等待时间 = context的等待时间 + 额外等待时间
                long additionalWaitMillis =  attempts / totalCounts
                        * DEFAULT_ADDITIONAL_WAIT_MAX_MILLIS;// 额外等待时间:根据任务重试次数计算，尝试越多次，等待越久
                ctx.setWakeUpTime(System.currentTimeMillis() + ctx.getRetryDeferrals()
                        + additionalWaitMillis);
                ctx.setPriority(ctx.getTask().getPriority() + ctx.getPriority());
                ctx.getController().offer(ctx);
                return;
            } else {
                // 超过了最大重试次数，则标记任务无法完成，等待被回收
                log.error("任务超过最大重试次数：{} 次，将停止工作!! 最后一次尝试抛出的异常为：{}", ctx.getMaxRetries(), throwable);
                ctx.setStatus(CrawlerStatus.INTERRUPTED);
                return;
            }
        }
    }
    
    public enum HandleResult {
        RECOVER, NOT_RECOVER
    }
}
