package indi.crawler.processor;

import indi.crawler.nest.CrawlerContext;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@AllArgsConstructor
public class ProcessorContext {
    @Getter
    @Setter
    private CrawlerContext crawlerContext;
}
