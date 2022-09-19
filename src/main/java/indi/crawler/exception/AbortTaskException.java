package indi.crawler.exception;

/**
 * 用于强制停止爬虫线程执行任务的异常。爬虫线程抛出该异常，将被视为主动中止所执行的任务。
 * 当尝试修改处于该状态的爬虫时也会抛该异常。
 * 
 * @author DragonBoom
 * @since 2019.10.26
 */
public class AbortTaskException extends RuntimeException {
    private static final long serialVersionUID = 1L;

}
