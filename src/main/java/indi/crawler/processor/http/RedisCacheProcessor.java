package indi.crawler.processor.http;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Optional;

import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpRequestBase;

import com.google.common.io.ByteStreams;

import indi.crawler.nest.CrawlerContext;
import indi.crawler.nest.CrawlerStatus;
import indi.crawler.nest.ResponseEntity;
import indi.crawler.processor.ProcessorContext;
import indi.crawler.processor.ProcessorResult;
import indi.data.Pair;
import indi.data.StringObjectRedisCodec;
import indi.exception.WrapperException;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RedisCacheProcessor extends CacheProcessor {
    protected static RedisClient client;
    protected static RedisCommands<String, Object> commands;

    protected static final String HKEY = "LHCF-REQUEST";

    protected void init(String redisURI) {
        if (client == null) {
            // thread safe
            client = RedisClient.create(redisURI);
            client.setDefaultTimeout(Duration.ofMinutes(2));// TODO: sure?z
            StatefulRedisConnection<String, Object> connect = client.connect(new StringObjectRedisCodec());
            commands = connect.sync();
        }
    }

    public RedisCacheProcessor(String redisURI) {
        init(redisURI);
    }

    /**
     * 检测是否有缓存当前请求，有则不执行其他拦截器
     */
    @Override
    public ProcessorResult executeRequestByCache(ProcessorContext iCtx) throws Throwable {
        CrawlerContext ctx = iCtx.getCrawlerContext();
        String field = generateField(ctx.getRequest());
        if (isCached(field)) {
            log.info("该任务已缓存，不再发送请求 {} {}", ctx.getTask().getName(), ctx.getUri());
            return ProcessorResult.CONTINUE_STAGE;
        } else {
            log.info("该任务尚未缓存，将缓存请求 {} {}", ctx.getTask().getName(), ctx.getUri());
            return ProcessorResult.KEEP_GOING;
        }
    }
    
    private static final ThreadLocal<Pair<HttpRequestBase, String>> lastRequestFieldThreadLocal = new ThreadLocal<>();

    /**
     * 根据请求，生成Redis的哈希键
     * 
     * <p>该方法的执行非常频繁，需要特别注意效率
     */
    protected String generateField(HttpRequestBase request) throws Throwable {
        // 1. try get thread local cache
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
        // TODO Auto-generated method stub
        CrawlerContext ctx = iCtx.getCrawlerContext();
        String field = generateField(ctx.getRequest());
        if (isCached(field)) {
            // 若有缓存该请求，则直接从缓存获取响应
            ResponseEntity responseEntity = (ResponseEntity) commands.hget(HKEY, field);
            
            if (responseEntity != null) {
                log.info("从Redis缓存中获取数据：{} {}", ctx.getTask().getName(), ctx.getUri());
                ctx.setResponseEntity(responseEntity);
                return ProcessorResult.CONTINUE_STAGE;// stop receive response
            } else {
                log.error("从Redis中检测到缓存，但无法访问：{} {}", ctx.getTask().getName(), ctx.getUri());
                throw new WrapperException("从Redis中检测到缓存，但无法访问");
            }
        }
        // 若没有缓存，则继续后续步骤
        return ProcessorResult.KEEP_GOING;
    }

    @Override
    public ProcessorResult afterHandleResultByCache(ProcessorContext iCtx) throws Throwable {
        CrawlerContext ctx = iCtx.getCrawlerContext();
        String field = generateField(ctx.getRequest());
        if (!isCached(field)) {
            List<Throwable> throwables = ctx.getThrowables();
            // 若响应不为空且没有发生过异常，则缓存该请求
            CrawlerStatus status = ctx.getStatus();
            if (ctx.getResponse() != null && (throwables == null || throwables.size() == 0)
                    && (status.equals(CrawlerStatus.FINISHED) || status.equals(CrawlerStatus.RUNNING))) {
                ResponseEntity responseEntity = ctx.getResponseEntity();
                
                log.info("添加新缓存：{} {}", ctx.getTask().getName(), ctx.getUri());
                commands.hset(HKEY, field, responseEntity);
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
            boolean isCached = commands.hexists(HKEY, field);
            isFieldCachedThreadLocal.set(Pair.of(field, isCached));
            return isCached;
        }
    }

}
