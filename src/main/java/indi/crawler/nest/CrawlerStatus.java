package indi.crawler.nest;

/**
 * 爬虫状态
 * 
 * @author DragonBoom
 *
 */
public enum CrawlerStatus {
    CREATED("Created"), RUNNING("Running"), PENDING("Pending"), DEFERRED("Deferred"),
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
    INTERRUPTED("Interrupted");

    private String desc;

    CrawlerStatus(String desc) {
        this.desc = desc;
    }

    public String getDescription() {
        return desc;
    }
}
