package indi.crawler.cookies;

import java.net.HttpCookie;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Table;

public class MemoryCookieStore extends BasicCookieStore {
    // domain -> cookies
//    private ListMultimap<String, HttpCookie> store;
    
    /**
     * Table<\Domain(String), CookieKey(String), HttpCookie>。
     * 
     * 用Table而不是Multimap是为了解决cookie的Key重复的问题
     */
    private Table<String, String, HttpCookie> store;// for add
    private HashMultimap<String, HttpCookie> multimap;// for get
    
    private Lock lock;
    /** 0 idle, > 0 adding( = adding count), -1 getting */
    

    private void init() {
        store = HashBasedTable.create();// not thread safe...
        multimap = HashMultimap.create();// not thread safe
        lock = new ReentrantLock();
    }

    public MemoryCookieStore() {
        init();
    }

    @Override
    public Collection<HttpCookie> get0(String domain) {
        Set<HttpCookie> cookies = multimap.get(domain);
        return cookies;
    }

    /**
     * 简单用synchronized处理并发问题
     */
    @Override
    public synchronized void add0(HttpCookie cookie) {
        String domain = cookie.getDomain();
        // 1. 
        HttpCookie oldCookie = store.put(domain, cookie.getName(), cookie);
        // 利用HttpCookie实现的equals方法，若cookie实际没有变化，则不需要执行后续步骤
        if (cookie.equals(oldCookie)) {
            return;
        }

        // update multimap
        if (oldCookie == null) {
            // 此次添加cookie为 新增 操作
            // 直接添加到multimap中
        } else {
            // 此次添加cookie为 更新 操作
            // 移除旧cookie，添加新cookie
            multimap.remove(domain, oldCookie);
        }
        multimap.put(domain, cookie);
    }

    @Override
    public synchronized boolean clear0(String domain) {
        store.clear();
        return true;
    }

}
