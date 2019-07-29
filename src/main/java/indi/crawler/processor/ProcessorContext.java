package indi.crawler.processor;

import indi.crawler.task.Task;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@AllArgsConstructor
public class ProcessorContext {
    @Getter
    @Setter
    private Task crawlerContext;
}
