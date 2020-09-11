package indi.crawler.util;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import indi.data.StringObjectRedisCodec;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.codec.RedisCodec;
import lombok.extern.slf4j.Slf4j;

/**
 * 基于lettuce操作Redis的工具类
 * 
 * @author DragonBoom
 */
@Slf4j
public class RedisUtils {
    private static final RedisCodec<String, Object> STRING_OBJECT_CODEC = new StringObjectRedisCodec();
    private static Map<String, ThreadLocal<RedisAsyncCommands<String, Object>>> commandsMap = new ConcurrentHashMap<>();
    private static ConcurrentHashMap<String, RedisClient> redisClientMap = new ConcurrentHashMap<>();
    private static Lock lock = new ReentrantLock();
    
    /**
     * 获取异步命令。为每个线程（而不是每次调用）的每个uri创建一个连接（以确保brpop不会导致与其他线程的lpush阻塞）。
     * 
     * @param redisURI redis地址（含密码）；目前一个redisURI对应一个单例RedisClient，
     * 而且redisURI字符串必须完全相同才会获得同一个Clinet。后续有空再考虑优化这点
     */
    public static RedisAsyncCommands<String, Object> getAsyncCommands(String redisURI) {
        // 获取指定连接的ThreadLocal<RedisAsyncCommands>
        ThreadLocal<RedisAsyncCommands<String, Object>> commandsThreadLocal = commandsMap.get(redisURI);
        
        if (commandsThreadLocal == null) {
            lock.lock();
            // 建立连接并获取异步命令
            try {
                if (commandsMap.get(redisURI) == null) {
                    // 在这里而不是值域创建匿名内部类，以传入redisURI
                    commandsThreadLocal = new ThreadLocal<RedisAsyncCommands<String, Object>>() {
                        
                        @Override
                        protected RedisAsyncCommands<String, Object> initialValue() {
                            // 获取client
                            RedisClient client = getClient(redisURI);
                            log.info("为线程 {} 新建Redis连接：{}", Thread.currentThread(), redisURI);
                            // 建立连接
                            StatefulRedisConnection<String, Object> connect = client.connect(STRING_OBJECT_CODEC);
                            // 获取异步命令
                            return connect.async();
                        }
                        
                    };
                    commandsMap.put(redisURI, commandsThreadLocal);
                }
            } finally {
                lock.unlock();
            }
        }
        
        return commandsThreadLocal.get();
    }

    /**
     * 获取单例RedisClient，用内存缓存实现。
     * 
     * @param redisURI redis地址（含密码）；目前一个redisURI对应一个单例RedisClient，
     * 而且redisURI字符串必须完全相同才会获得同一个Clinet。后续有空再考虑优化这点
     * @return
     */
    public static RedisClient getClient(String redisURI) {
        RedisClient redisClient = redisClientMap.get(redisURI);
        if (redisClient == null) {
            lock.lock();
            try {
                if (redisClient == null) {
                    redisClient = RedisClient.create(redisURI);
                    redisClient.setDefaultTimeout(Duration.ofMinutes(2));
                    redisClientMap.put(redisURI, redisClient);
                }
            } finally {
                lock.unlock();
            }
        }
        return redisClient;
    }
}
