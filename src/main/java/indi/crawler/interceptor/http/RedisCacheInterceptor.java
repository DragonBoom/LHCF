package indi.crawler.interceptor.http;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;

import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpRequestBase;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.ByteStreams;

import indi.crawler.interceptor.InterceptorContext;
import indi.crawler.nest.CrawlerContext;
import indi.crawler.nest.CrawlerStatus;
import indi.crawler.nest.ResponseEntity;
import indi.crawler.task.Task;
import indi.exception.WrapperException;
import indi.util.ObjectMapperUtils;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.RedisCodec;

public class RedisCacheInterceptor extends CacheInterceptor {
    protected static RedisClient client;
    protected static RedisCommands<String, ResponseEntity> commands;

    protected static final String HKEY = "LHCF-REQUEST";

    protected void init(String redisURI) {
        if (client == null) {
            // thread safe
            client = RedisClient.create(redisURI);
            
            StatefulRedisConnection<String, ResponseEntity> connect = client.connect(new RedisCodec<String, ResponseEntity>() {

                @Override
                public String decodeKey(ByteBuffer bytes) {
                    try {
                        return new String(bytes.array(), "utf-8");
                    } catch (UnsupportedEncodingException e) {
                        throw new WrapperException(e);
                    }
                }

                @Override
                public ResponseEntity decodeValue(ByteBuffer byteBuffer) {
                    // bytes是DirectByteBuffer，无法直接拿到字节数组数组
                    // direct byte buffer，指直接进行I/O访问，而不是先建立缓存再进行访问
                    byte[] bytes = null;
                    if (byteBuffer.hasArray()) {
                        bytes = byteBuffer.array();
                    } else {
                        bytes = new byte[byteBuffer.remaining()];
                    }
                    byteBuffer.get(bytes);
                    ObjectMapper mapper = ObjectMapperUtils.getMapper();
                    try {
                        return mapper.readValue(bytes, ResponseEntity.class);
                    } catch (IOException e) {
                        throw new WrapperException(e);
                    }
                }

                @Override
                public ByteBuffer encodeKey(String key) {
                    return ByteBuffer.wrap(key.getBytes());
                }

                @Override
                public ByteBuffer encodeValue(ResponseEntity value) {
                    String jsonStr;
                    try {
                        jsonStr = ObjectMapperUtils.getMapper().writeValueAsString(value);
                    } catch (JsonProcessingException e) {
                        throw new WrapperException(e);
                    }
                    return ByteBuffer.wrap(jsonStr.getBytes());
                }
                
            });
            commands = connect.sync();
        }
    }

    public RedisCacheInterceptor(Task task, String redisURI) {
        super(task);
        init(redisURI);
    }

    @Override
    public void executeRequestByCache(InterceptorContext iCtx) {
        CrawlerContext ctx = iCtx.getCrawlerContext();
        String field = generateField(ctx.getRequest());
        if (commands.hexists(HKEY, field)) {
        } else { 
            super.executeRequestPlain(iCtx);
        };
    }

    protected String generateField(HttpRequestBase request) {
        StringBuilder sb = new StringBuilder();
        if (request instanceof HttpEntityEnclosingRequestBase) {
            // 若请求可携带请求实体（方法是POST/PUT），则生成的Field将包含该实体的信息
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
                                e.printStackTrace();
                            }
                        }
                    })
                    .map(bytes -> new String(bytes))// TODO: sure ?
                    .ifPresent(sb::append);
        }
        URI uri = request.getURI();
        return sb.append(uri).toString();
        
    }

    @Override
    public void receiveResponseByCache(InterceptorContext iCtx) {
        // TODO Auto-generated method stub
        CrawlerContext ctx = iCtx.getCrawlerContext();
        String field = generateField(ctx.getRequest());
        if (commands.hexists(HKEY, field)) {
            // 若有缓存该请求，则直接从缓存获取响应
            ResponseEntity responseEntity = commands.hget(HKEY, field);
            if (responseEntity != null) {
                ctx.setResponseEntity(responseEntity);
            }
            // TODO: what about response ??
            return;
        } else {
            // 若没有缓存，则继续后续步骤
            super.receiveResponsePlain(iCtx);
        }
        
    }

    @Override
    public void afterHandleResultByCache(InterceptorContext iCtx) {
        CrawlerContext ctx = iCtx.getCrawlerContext();
        String field = generateField(ctx.getRequest());
        if (!commands.hexists(HKEY, field)) {
            List<Throwable> throwables = ctx.getThrowables();
            // 若响应不为空且没有发生过异常，则缓存该请求
            if (ctx.getResponse() != null && (throwables == null || throwables.size() == 0)
                    && (ctx.getStatus().equals(CrawlerStatus.FINISHED) || ctx.getStatus().equals(CrawlerStatus.RUNNING))) {
                ResponseEntity responseEntity = ctx.getResponseEntity();
                commands.hset(HKEY, field, responseEntity);
            }
        }

        // 进行后续步骤
        super.afterHandleResultByCache(iCtx);
    }
    
   

}
