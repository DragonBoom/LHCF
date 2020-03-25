package indi.crawler.processor;

import java.util.function.Supplier;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
public class ProcessorResult {
    public Result result;
    private Object object;

    public enum Result {
        /** 继续执行其他拦截器 */
        KEEP_GOING,
        /** 跳过当前步骤 */
        CONTINUE_STAGE,
        /** 结束拦截 */
        OVER;
    }
    /**继续执行*/
    public static final ProcessorResult KEEP_GOING = new ProcessorResult(Result.KEEP_GOING, null);
    /**跳过之后的环节*/
    public static final ProcessorResult CONTINUE_STAGE = new ProcessorResult(Result.CONTINUE_STAGE, null);
    /**结束处理*/
    public static final ProcessorResult OVER = new ProcessorResult(Result.OVER, null);
}
