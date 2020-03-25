package indi.crawler.monitor;

import java.util.NoSuchElementException;

import indi.crawler.task.CrawlerController;
import indi.thread.BasicThread;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Monitor {
    
    /**
     * 监听器的线程。为BasicThread封装一层，便于统一管理
     * 
     * @author wzh
     * @since 2019.12.08
     */
    public static class MonitorThread extends BasicThread {
        @Getter
        protected volatile boolean retire = false;
        protected CrawlerController controller;
        
        public void startDeamon(CrawlerController controller) {
            this.controller = controller;
            // 注册
            controller.getMonitorThreads().add(this);
            
            super.startDeamon();
        }
        
        public void startNotDeamon(CrawlerController controller) {
            this.controller = controller;
            // 注册
            controller.getMonitorThreads().add(this);
            
            super.startNotDeamon();
        }
        
        /**
         * 结束线程
         */
        public void retire() {
            log.info("结束监视器线程：{}", this);
            retire = true;
        }
        
        /**
         * 禁用该方法
         * 
         * @deprecated 要启动线程必须用startDeamon或startNotDeamon方法启动
         */
        @Override
        @Deprecated
        public synchronized void start() {
            throw new NoSuchElementException();
        }
        
        

        /*
         * 下列构造函数直接继承自Thread
         */
        public MonitorThread(Runnable target, String name) {
            super(target, name);
            // TODO Auto-generated constructor stub
        }

        public MonitorThread(Runnable target) {
            super(target);
            // TODO Auto-generated constructor stub
        }

        public MonitorThread(String name) {
            super(name);
            // TODO Auto-generated constructor stub
        }

        public MonitorThread(ThreadGroup group, Runnable target, String name, long stackSize) {
            super(group, target, name, stackSize);
            // TODO Auto-generated constructor stub
        }

        public MonitorThread(ThreadGroup group, Runnable target, String name) {
            super(group, target, name);
            // TODO Auto-generated constructor stub
        }

        public MonitorThread(ThreadGroup group, Runnable target) {
            super(group, target);
            // TODO Auto-generated constructor stub
        }

        public MonitorThread(ThreadGroup group, String name) {
            super(group, name);
            // TODO Auto-generated constructor stub
        }

        public MonitorThread() {
            super();
            // TODO Auto-generated constructor stub
        }

        /**
         * 
         * @deprecated 请使用startDeamon(CralwerController)方法
         */
        @Override
        @Deprecated
        public void startDeamon() {
            // TODO Auto-generated method stub
            super.startDeamon();
        }

        /**
         * 
         * @deprecated 请使用startNotDeamon(CralwerController)方法
         */
        @Override
        @Deprecated
        public void startNotDeamon() {
            // TODO Auto-generated method stub
            super.startNotDeamon();
        }
        
        
    }
    
}
