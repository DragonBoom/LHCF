package indi.crawler.processor.http;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpRequestBase;

import com.google.common.io.ByteStreams;

import indi.crawler.processor.ProcessorContext;
import indi.crawler.processor.ProcessorResult;
import indi.crawler.task.CrawlerStatus;
import indi.crawler.task.ResponseEntity;
import indi.crawler.task.Task;
import indi.crawler.util.RedisUtils;
import indi.data.Pair;
import indi.exception.WrapperException;
import lombok.extern.slf4j.Slf4j;

/**
 * 基于Redis的请求缓存处理器，确保不出现阻塞的情况
 * 
 * <p>2019.10.26 目前存在一个问题，缓存难以处理临时文件的存储方式。redis能否以流的形式操作数据呢？http://blog.huangz.me/2017/redis-streams.html
 * 
 * @author DragonBoom
 *
 */
@Slf4j
public class RedisCacheProcessor extends CacheProcessor {
    protected String redisURI;
//    protected RedisClient client;
//    protected RedisCommands<String, Object> commands;
    
    /**用该锁确保只有一个线程在调同一个Redis API，确保不会有线程因一直阻塞而超时报错*/
//    private Lock redisLock;// must static!!
    
    private static final Map<String, Lock> URI_LOCK_MAP = new HashMap<>();
    
    private static final Lock STATIC_LOCK = new ReentrantLock();

    protected static final String HKEY = "LHCF-REQUEST";

    protected void init(String redisURI) {
        this.redisURI = redisURI;
//        // 初始化redisLock，确保使用同一个Redis的对象共用同一个锁
//        STATIC_LOCK.lock();
//        try {
//            redisLock = URI_LOCK_MAP.get(redisURI);
//            if (redisLock == null) {
//                redisLock = new ReentrantLock(true);// fair lock
//                URI_LOCK_MAP.put(redisURI, redisLock);
//            }
//        } finally {
//            STATIC_LOCK.unlock();
//        }
//        
//        client = RedisClient.create(redisURI);
//        client.setDefaultTimeout(Duration.ofMinutes(2));// TODO: sure?z
//        StatefulRedisConnection<String, Object> connect = client.connect(new StringObjectRedisCodec());
//        commands = connect.sync();
    }

    public RedisCacheProcessor(String redisURI) {
        
        init(redisURI);
    }

    /**
     * 检测是否有缓存当前请求，有则不执行其他拦截器，不发送请求
     */
    @Override
    public ProcessorResult executeRequestByCache(ProcessorContext iCtx) throws Throwable {
        Task ctx = iCtx.getCrawlerContext();
        String field = generateField(ctx);
        if (isCached(field)) {
            log.info("该任务已缓存，不再发送请求 {} {}", ctx.getTaskDef().getName(), ctx.getUri());
            return ProcessorResult.CONTINUE_STAGE;// 跳过其他拦截器（不需要发送请求）
        } else {
            log.info("该任务尚未缓存，将缓存请求 {} {}", ctx.getTaskDef().getName(), ctx.getUri());
            return ProcessorResult.KEEP_GOING;
        }
    }
    
    /**本线程上次使用的哈希键*/
    private static final ThreadLocal<Pair<HttpRequestBase, String>> lastRequestFieldThreadLocal = new ThreadLocal<>();

    /**
     * 根据请求，生成Redis的哈希键
     * 
     * <p>该方法的执行非常频繁，需要特别注意效率
     */
    protected String generateField(Task task) throws Throwable {
        
        if (task.getIdentityKey() != null) {
            return task.getIdentityKey();
        }
        
        HttpRequestBase request = task.getRequest();
        // 1. 尝试通过ThreadLocal缓存直接获取哈希键
        Pair<HttpRequestBase, String> pair = lastRequestFieldThreadLocal.get();
        if (pair != null && request.equals(pair.getFirst())) {
            String lastField = pair.getSecond();
            if (lastField != null) {
                return lastField;
            }
        }
        // 2. gen field
        // with plain url
        URI uri = request.getURI();
        StringBuilder sb = new StringBuilder().append(request.getMethod()).append(uri);
        // with request body
        if (request instanceof HttpEntityEnclosingRequestBase) {
            // 若请求携带请求实体（方法是POST/PUT），则生成的Field将包含该实体的信息
            HttpEntityEnclosingRequestBase entityRequest = (HttpEntityEnclosingRequestBase) request;
            Optional.ofNullable(entityRequest.getEntity())
                    .map(entity -> {
                        try {
                            return entity.getContent();
                        } catch (UnsupportedOperationException | IOException e) {
                            throw new WrapperException(e);
                        }
                    })
                    .map(inStream -> {
                        try {
                            return ByteStreams.toByteArray(inStream);
                        } catch (Exception e) {
                            throw new WrapperException(e);
                        } finally {
                            try {
                                inStream.close();
                            } catch (IOException e) {
                                throw new WrapperException(e);
                            }
                        }
                    })
                    .map(bytes -> new String(bytes))// TODO: sure ?
                    .ifPresent(sb::append);
        }
        String field = sb.toString();
        // 3. set thread local cache
        lastRequestFieldThreadLocal.set(Pair.of(request, field));
        return field;
    }
    
    @Override
    public ProcessorResult receiveResponseByCache(ProcessorContext iCtx) throws Throwable {
        Task ctx = iCtx.getCrawlerContext();
        String field = generateField(ctx);
        if (isCached(field)) {
            // 若有缓存该请求，则直接从缓存获取响应
            ResponseEntity responseEntity;
            responseEntity = (ResponseEntity) RedisUtils.getAsyncCommands(redisURI).hget(HKEY, field).get();
            
            if (responseEntity != null) {
                log.info("  从Redis缓存中获取数据：{} {}", ctx.getTaskDef().getName(), ctx.getUri());
                ctx.setResponseEntity(responseEntity);
                return ProcessorResult.CONTINUE_STAGE;// stop receive response
            } else {
                log.error("从Redis中检测到缓存，但无法访问：{} {}", ctx.getTaskDef().getName(), ctx.getUri());
                throw new WrapperException("从Redis中检测到缓存，但无法访问");
            }
        }
        // 若没有缓存，则继续后续步骤
        return ProcessorResult.KEEP_GOING;
    }

    @Override
    public ProcessorResult afterHandleResultByCache(ProcessorContext iCtx) throws Throwable {
        Task ctx = iCtx.getCrawlerContext();
        String field = generateField(ctx);
        if (!isCached(field)) {
            List<Throwable> throwables = ctx.getThrowables();
            // 若没有缓存过该请求、响应不为空且没有发生过异常，则缓存该请求实体
            CrawlerStatus status = ctx.getStatus();
            if (ctx.getResponse() != null && (throwables == null || throwables.isEmpty())
                    && (status.equals(CrawlerStatus.FINISHED) || status.equals(CrawlerStatus.RUNNING))) {
                ResponseEntity responseEntity = ctx.getResponseEntity();
                
                log.info("添加新缓存：{} {}", ctx.getTaskDef().getName(), ctx.getUri());
                RedisUtils.getAsyncCommands(redisURI).hset(HKEY, field, responseEntity).get();
                
//                redisLock.lock();
//                try {
//                } finally {
//                    redisLock.unlock();
//                }
            }
        }

        // 进行后续步骤
        return ProcessorResult.KEEP_GOING;
    }
    
    ThreadLocal<Pair<String, Boolean>>  isFieldCachedThreadLocal = new ThreadLocal<>();
    
    protected boolean isCached(String field) {
        Pair<String, Boolean> pair = isFieldCachedThreadLocal.get();
        if (pair != null && field.equals(pair.getFirst())) {
            return pair.getSecond();
        } else {
            boolean isCached = false;
            try {
                isCached = RedisUtils.getAsyncCommands(redisURI).hexists(HKEY, field).get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
            isFieldCachedThreadLocal.set(Pair.of(field, isCached));
            return isCached;
        }
    }

}
