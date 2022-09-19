package indi.crawler.processor.http;

import org.apache.http.Header;
import org.apache.http.HttpResponse;

import indi.crawler.processor.ProcessorContext;
import indi.crawler.processor.ProcessorResult;
import indi.crawler.task.Task;
import indi.crawler.task.def.TaskDef;
import indi.crawler.task.def.TaskType;
import lombok.extern.slf4j.Slf4j;

/**
 * 用于在处理HTTP连接的各阶段输出日志
 * 
 * @author DragonBoom
 *
 */
@Slf4j
public class HttpLogProcessor extends HTTPProcessor {

    /**
     * 将请求头数组将转化为字符串
     * 
     * @param headers
     * @return
     */
	private String stringifyHeaders(Header[] headers) {
		StringBuilder sb = new StringBuilder();
		for (Header h : headers) {
			sb.append("[").append(h.getName()).append("=").append(h.getValue()).append("] ");
		}
		return sb.toString();
	}
	
	/**
	 * 将HTTP响应转化为字符串
	 * 
	 * @param ctx
	 * @return
	 */
	private String stringifyHTTPResponse(Task ctx) {
	    HttpResponse response = ctx.getResponse();
	    if (response != null) {
	        StringBuilder sb = new StringBuilder("-<<<  Receive Response-> ")
	                .append(response.getStatusLine()).append(" ")
	                .append(ctx.getTaskDef().getName()).append(" ")
	                .append(ctx.getUri()).append("  ")
	                .append(" [").append(stringifyHeaders(response.getAllHeaders())).append("]");
	        return sb.toString();
	    } else {
	        return "";
	    }
	}

	/**
	 * 将HTTP请求转化为字符串
	 * 
	 * @param ctx
	 * @return
	 */
	private String stringifyHTTPRequest(Task ctx) {
		StringBuilder sb = new StringBuilder("->>> Send HTTP Request-> ");
		String method = ctx.getTaskDef().getMethod().name();
		String uri = ctx.getUri().toString();
		String headers = stringifyHeaders(ctx.getRequest().getAllHeaders());
		sb.append(method).append(" ")
		    .append(ctx.getTaskDef().getName()).append(" ")
		    .append(uri).append(" [").append(headers).append("]");
		return sb.toString();
	}

	@Override
	protected ProcessorResult executeRequest0(ProcessorContext iCtx) throws Throwable {
		Task ctx = iCtx.getCrawlerContext();
		if (ctx.getTaskDef().getType() == TaskType.HTTP_TOPICAL) {
		    log.info(stringifyHTTPRequest(ctx));
		}
		
		return ProcessorResult.KEEP_GOING;
	}

	@Override
	protected ProcessorResult handleResult0(ProcessorContext hCtx) throws Throwable {
		Task ctx = hCtx.getCrawlerContext();
		if (ctx.getTaskDef().getType() == TaskType.HTTP_TOPICAL) {
		    log.info(stringifyHTTPResponse(ctx));
		}
		
		return ProcessorResult.KEEP_GOING;
	}

	/**
	 * 1. 记录任务已完成，且又创建了多少个新的任务
	 */
	@Override
	protected ProcessorResult afterHandleResult0(ProcessorContext iCtx) throws Throwable {
		Task ctx = iCtx.getCrawlerContext();
		TaskDef taskDef = ctx.getTaskDef();
		if (ctx.getChilds() != null && ctx.getChilds().size() > 0) {
			StringBuilder sb = new StringBuilder("-||| After handle result, task ")
			        .append(taskDef.getName())
			        .append(" complete and add [").append(ctx.getChilds().size()).append("] new Task to the Pool");
			log.info(sb.toString());
		}
		
		return ProcessorResult.KEEP_GOING;
	}
}
