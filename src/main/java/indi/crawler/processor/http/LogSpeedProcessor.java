package indi.crawler.processor.http;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import indi.crawler.monitor.CloseableMonitor;
import indi.crawler.processor.ProcessorContext;
import indi.crawler.processor.ProcessorResult;
import lombok.extern.slf4j.Slf4j;

/**
 * 用于记录下载速度
 * 
 * @author wzh
 * @since 2021.01.28
 */
@Slf4j
public class LogSpeedProcessor extends HTTPProcessor {
    /** 总下载字节大小 */
    private AtomicLong downloadByteCount = new AtomicLong();
    /** 请求次数 */
    private AtomicLong responseCount = new AtomicLong();
    private static Long DEFAULT_MILLIS = -1L;
    /** 第一次下载结束时间 */
    private Long firstMillis = DEFAULT_MILLIS;
    
    public LogSpeedProcessor() {
        log.info("启动速度记录处理器");
        CloseableMonitor.registLog(() -> {
            return new StringBuilder()
                    .append("共下载 ").append(downloadByteCount.get() / 1024 / 1024).append(" mb")
                    .append("， 处理响应").append(responseCount.get()).append("个，")
                    .append("耗时" + (System.currentTimeMillis() - firstMillis) / 1000 / 60 + "min")
                    .toString();
        });
    }

    @Override
    protected ProcessorResult handleResult0(ProcessorContext ctx) throws Throwable {
        Optional.of(ctx.getCrawlerContext().getResponseEntity()).ifPresent(e -> {
            responseCount.addAndGet(1);
            downloadByteCount.addAndGet(e.size());
            if (firstMillis == DEFAULT_MILLIS) {
                firstMillis = System.currentTimeMillis();
            }
        });
        return ProcessorResult.KEEP_GOING;
    }

}
