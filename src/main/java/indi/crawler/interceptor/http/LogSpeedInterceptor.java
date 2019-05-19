package indi.crawler.interceptor.http;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import indi.crawler.interceptor.InterceptorContext;
import indi.crawler.monitor.CloseableMonitor;

public class LogSpeedInterceptor extends HttpInterceptor {
    private AtomicLong downloadByteCount = new AtomicLong();

    @Override
    public void afterReceiveResponse(InterceptorContext ctx) {
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
        CloseableMonitor.setToLogAtEnd(new StringBuilder()
                .append("共下载 ").append(downloadByteCount.get() / 1024 / 1024).append(" mb")
                .toString());
        
        super.afterReceiveResponse(ctx);
    }

}
