package indi.crawler.proxy;

import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public abstract class ProxyPool {
    private PriorityQueue<PriorityProxy> availables;
    private HashSet<PriorityProxy> leaseds;

    private Lock availablesLock;
    private Lock leasedsLock;

    public ProxyPool() {
        availables = new PriorityQueue<>();
        leaseds = new HashSet<>();

        availablesLock = new ReentrantLock();
    }

    /**
     * 请求代理
     * 
     * @return 代理
     */
    public PriorityProxy lease() {
        PriorityProxy proxy = null;
        // 1
        if (availables.size() > 0) {
            availablesLock.lock();
            try {
                proxy = availables.poll();
            } finally {
                availablesLock.unlock();
            }
        }
        // 2 填充可用队列
        if (proxy == null) {
            fillAvailables();
        }
        // finally
        if (proxy == null) {
            // TODO
            return null;
        }
        leasedsLock.lock();
        try {
            leaseds.add(proxy);
        } finally {
            leasedsLock.unlock();
        }
        return proxy;
    }

    /**
     * 归还代理
     * 
     * @return 表示归还是否成功
     */
    public boolean release(PriorityProxy proxy) {
        if (!check(proxy)) {
            return false;
        }
        availablesLock.lock();
        try {
            availables.add(proxy);
        } finally {
            availablesLock.unlock();
        }
        return false;
    }

    /**
     * 判断代理是否有效
     */
    protected abstract boolean check(PriorityProxy proxy);

    /**
     * 填充可用队列
     * 
     * @return 返回填充结果
     */
    protected abstract boolean fillAvailables();

}
