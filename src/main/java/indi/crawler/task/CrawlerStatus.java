package indi.crawler.task;

/**
 * 爬虫状态
 * 
 * @author DragonBoom
 *
 */
public enum CrawlerStatus {
    CREATED("Created"), RUNNING("Running"),
    /**
     * 正在判断状态将切换为什么值
     */
    PENDING("Pending"),
    /**
     * 延迟处理，延迟时间的字段为WakeUpTime
     */
    DEFERRED("Deferred"),
    /**
     * 被用户中断
     */
    ABORTED("Aborted by user"),
    /**
     * 任务完成
     */
    FINISHED("Finished"),
    /**
     * 任务无法完成
     */
    INTERRUPTED("Interrupted"),
    /**
     * 阻塞中（需等待其他任务完成）
     */
    BLOCKING("Blocking"),
    /**
     * 阻塞一定时间（等待到达WakeUpTime时才能执行）；当爬虫处于该状态，且已达到WakeUpTime，就不会再被阻塞
     */
    BLOCKING_TIME("BlockingTime")
    ;

    private String desc;

    CrawlerStatus(String desc) {
        this.desc = desc;
    }

    public String getDescription() {
        return desc;
    }
}
