package indi.crawler.monitor;

import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

import indi.crawler.task.CrawlerController;
import indi.exception.WrapperException;
import indi.thread.BasicThread;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Monitor {
    
    /**
     * 监听器的线程。为BasicThread封装一层，便于统一管理（统一结束等）
     * 
     * @author wzh
     * @since 2019.12.08
     */
    public static class MonitorThread extends BasicThread {
        @Getter
        protected volatile boolean retire = false;
        protected CrawlerController controller;
        @Getter
        protected long sleepMillis;
        
        public void startDeamon(CrawlerController controller) {
            this.controller = controller;
            // 向控制器注册
            controller.getMonitorThreads().add(this);
            
            super.startDeamon();
        }
        
        public void startNotDeamon(CrawlerController controller) {
            this.controller = controller;
            // 向控制器注册
            controller.getMonitorThreads().add(this);
            
            super.startNotDeamon();
        }
        
        @Override
        public void run() {
            while (!retire) {
                try {
                    TimeUnit.MILLISECONDS.sleep(sleepMillis);
                } catch (InterruptedException e) {
                    // 睡眠中被中断
                    if (this.isRetire()) {
                        // 已退休，属于正常行为
                        log.debug("从睡眠中结束线程:{}", this.getClass().getSimpleName());
                    } else {
                        throw new WrapperException(e);
                    }
                }
                run0();
            }
        }
        
        /**
         * 子类可重写该方法，以沿用本类提供的循环睡眠与中断校验功能
         * 
         * @author DragonBoom
         * @since 2020.09.04
         */
        public void run0() {};
        
        /**
         * 结束线程
         */
        public void retire() {
            log.info("结束监视器线程：{}", this.getClass().getSimpleName());
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
        
        /**
         * 
         * @param name 线程名称
         * @param sleepMillis 每次循环的间隔
         */
        public MonitorThread(String name, Long sleepMillis) {
            super(name);
            this.sleepMillis = sleepMillis;
        }
        
        /**
         * 
         * @param sleepMillis 每次循环的间隔
         */
        public MonitorThread(Long sleepMillis) {
            this.sleepMillis = sleepMillis;
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
