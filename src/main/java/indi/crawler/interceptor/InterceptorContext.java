package indi.crawler.interceptor;

import indi.crawler.nest.CrawlerContext;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@AllArgsConstructor
public class InterceptorContext {
    @Getter
    @Setter
    private CrawlerContext crawlerContext;
}
