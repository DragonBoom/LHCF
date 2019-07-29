package indi.crawler.cookies;

import java.net.HttpCookie;
import java.util.Collection;
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
     * Table<Domain, CookieKey, HttpCookie>
     * 
     * 用Table而不是Multimap是为了解决cookieKey重复的问题
     */
    private Table<String, String, HttpCookie> store;// for add
    private HashMultimap<String, HttpCookie> multimap;// for get
    
    private Lock lock;
    // -1 idle, 0 getting, 1 adding
    private AtomicInteger status;
    

    private void init() {
        store = HashBasedTable.create();// not thread safe
        multimap = HashMultimap.create();// not thread safe
        lock = new ReentrantLock();
        status = new AtomicInteger();
    }

    public MemoryCookieStore() {
        init();
    }

    @Override
    public Collection<HttpCookie> get0(String domain) {
        // TODO Optimise 没必要每次都创建新的LinkedList对象
        boolean canGet = false;// be true when status = -1 / status = 0
        while (!canGet) {
            if (!(canGet = status.compareAndSet(-1, 0))) {
                canGet = status.compareAndSet(0, 0);
            }
        }
        
        return multimap.get(domain);
    }

    @Override
    public synchronized void add0(HttpCookie cookie) {
        String domain = cookie.getDomain();
        // 1. 
        HttpCookie oldCookie = store.put(domain, cookie.getName(), cookie);
        // 利用HttpCookie实现的equals方法，若cookie实际没有变化，则不需要执行后续步骤
        if (cookie.equals(oldCookie)) {
            return;
        }
        while (status.compareAndSet(-1, 1)) {
            // update multimap
            if (oldCookie == null) {
                // 此次添加cookie为 新增 操作
                // 直接添加到multimap中
                multimap.put(domain, cookie);
            } else {
                // 此次添加cookie为 更新 操作
                // 移除旧cookie，添加新cookie
                multimap.remove(domain, oldCookie);
                multimap.put(domain, cookie);
            }
            status.decrementAndGet();// 1 -> -1
        }
    }

    @Override
    public synchronized boolean clear0(String domain) {
        store.clear();
        return true;
    }

}
