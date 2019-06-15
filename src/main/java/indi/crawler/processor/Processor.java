package indi.crawler.processor;

import lombok.Getter;
import lombok.Setter;

/**
 * 爬虫任务执行器，由ProcessorExcutor执行
 */
public abstract class Processor {
    @Getter
    @Setter
    protected Processor next;
}
