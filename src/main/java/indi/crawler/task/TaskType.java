package indi.crawler.task;

public enum TaskType {
    /** 主题爬虫*/
    HTTP_TOPICAL("HTTP Topical Crawler Task")
    ;

    private String type;

    TaskType(String type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return type;
    }
}
