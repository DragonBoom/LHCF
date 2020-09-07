package indi.crawler.processor.http;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import indi.crawler.monitor.CloseableMonitor;
import indi.crawler.processor.ProcessorContext;
import indi.crawler.processor.ProcessorResult;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LogSpeedProcessor extends HTTPProcessor {
    /** 总下载字节大小 */
    private AtomicLong downloadByteCount = new AtomicLong();
    /** 请求次数 */
    private AtomicLong responseCount = new AtomicLong();
    
    public LogSpeedProcessor() {
        log.info("启动速度记录处理器");
    }

    @Override
    protected ProcessorResult handleResult0(ProcessorContext ctx) throws Throwable {
        Optional.of(ctx.getCrawlerContext().getResponseEntity()).ifPresent(e -> {
            responseCount.addAndGet(1);
            switch(e.getType()) {
            case ByteArray:
                if (e.getContent() instanceof byte[]) {
                    Optional.ofNullable((byte[]) e.getContent())
                            .ifPresent(bytes -> downloadByteCount.addAndGet(bytes.length));
                }
                break;
            case String:
                if (e.getContent() instanceof String) {
                    Optional.ofNullable((String) e.getContent())
                            .map(str -> str.getBytes())
                            .ifPresent(bytes -> downloadByteCount.addAndGet(bytes.length));
                }
                break;
            default:
                throw new IllegalArgumentException();
            }
        });
        CloseableMonitor.addLogAtEnd("LogSpeed", new StringBuilder()
                .append("共下载 ").append(downloadByteCount.get() / 1024 / 1024).append(" mb")
                .append("， 共处理响应").append(responseCount.get()).append("个")
                .toString());
        return ProcessorResult.KEEP_GOING;
    }

}
