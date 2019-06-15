package indi.crawler.processor.http;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import indi.crawler.monitor.CloseableMonitor;
import indi.crawler.processor.ProcessorContext;
import indi.crawler.processor.ProcessorResult;

public class LogSpeedProcessor extends HttpProcessor {
    private AtomicLong downloadByteCount = new AtomicLong();

    @Override
    protected ProcessorResult handleResult0(ProcessorContext ctx) throws Throwable {
        Optional.of(ctx.getCrawlerContext().getResponseEntity()).ifPresent(e -> {
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
        Map<String, String> toLogAtEndMap = CloseableMonitor.getToLogAtEndMap();
        toLogAtEndMap.put("LogSpeed", new StringBuilder()
                .append("共下载 ").append(downloadByteCount.get() / 1024 / 1024).append(" mb")
                .toString());
        return ProcessorResult.KEEP_GOING;
    }

}
