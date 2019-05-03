package indi.crawler.interceptor;

import indi.crawler.nest.CrawlerContext;
import lombok.Getter;
import lombok.Setter;

public class InterceptorContext {
    @Getter
    @Setter
    private CrawlerContext crawlerContext;

    public InterceptorContext(CrawlerContext crawlerContext) {
        this.crawlerContext = crawlerContext;
    }
}
