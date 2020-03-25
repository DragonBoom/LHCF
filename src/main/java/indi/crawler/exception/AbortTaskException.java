package indi.crawler.exception;

/**
 * 用于强制停止爬虫线程执行任务的异常。爬虫线程抛出该异常，将被视为主动中止爬虫任务。
 * 
 * @author DragonBoom
 * @since 2019.10.26
 */
public class AbortTaskException extends RuntimeException {
    private static final long serialVersionUID = 1L;

}
