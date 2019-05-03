package indi.crawler.proxy;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class PriorityProxy implements Comparable<PriorityProxy> {
    public static final int DEFAULT_PRIORITY = 0;
    public static final int DEFAULT_AGE = 0;
    public static final int DEFAULT_STAGE = 0;

    private String host;
    private int port;
    private int priority;
    /**
     * 用于记录使用次数
     */
    private int age;
    /**
     * 阶段，用于控制使用频率
     */
    private int stage;
    /**
     * 当前阶段开始时间，用于限制使用频率
     */
    private long stageBeginMillis;

    public PriorityProxy(String host, int port) {
        this.host = host;
        this.port = port;
        priority = DEFAULT_PRIORITY;
    }

    @Override
    public int compareTo(PriorityProxy o) {
        return this.getPriority() - o.getPriority();
    }

}
