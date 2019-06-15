package indi.crawler.nest;

/**
 * 爬虫状态
 * 
 * @author DragonBoom
 *
 */
public enum CrawlerStatus {
    CREATED("Created"), RUNNING("Running"),
    /**
     * 等待中
     */
    PENDING("Pending"),
    /**
     * 延迟处理
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
    BLOCKING("Blocking")
    ;

    private String desc;

    CrawlerStatus(String desc) {
        this.desc = desc;
    }

    public String getDescription() {
        return desc;
    }
}
