package indi.crawler.util;

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import indi.data.StringObjectRedisCodec;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RedisUtils {
//    private static Map<RedisClient, ThreadLocal<RedisCommands<String, Object>>> map = new ConcurrentHashMap<>();
//    private static ThreadLocal<RedisClient> clientThreadLocal = new ThreadLocal<>();
//    private static ThreadLocal<RedisAsyncCommands<String, Object>> commandsThreadLocal = new ThreadLocal<>();
    private static ConcurrentHashMap<String, RedisClient> map = new ConcurrentHashMap<>();
    private static ConcurrentHashMap<String, RedisAsyncCommands<String, Object>> commandsMap = new ConcurrentHashMap<>();
    private static Lock lock = new ReentrantLock();
    
    public static RedisAsyncCommands<String, Object> getAsyncCommands(String redisURI) {
        RedisAsyncCommands<String, Object> asyncCommands = commandsMap.get(redisURI);
        
        if (asyncCommands == null) {
            lock.lock();
            try {
                RedisClient client = getClient(redisURI);
                StatefulRedisConnection<String, Object> connect = client.connect(new StringObjectRedisCodec());
                asyncCommands = connect.async();
                commandsMap.put(redisURI, asyncCommands);
            } finally {
                lock.unlock();
            }
        }
        return asyncCommands;
    }
    
    private static RedisClient getClient(String redisURI) {
        RedisClient redisClient = map.get(redisURI);
        if (redisClient == null) {
            lock.lock();
            try {
                if (redisClient == null) {
                    redisClient = RedisClient.create(redisURI);
                    redisClient.setDefaultTimeout(Duration.ofMinutes(2));
                    map.put(redisURI, redisClient);
                }
            } finally {
                lock.unlock();
            }
        }
        log.info("新建Redis连接：{}", redisURI);
        return redisClient;
    }
}
