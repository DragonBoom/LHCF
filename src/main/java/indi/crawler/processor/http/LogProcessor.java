package indi.crawler.processor.http;

import org.apache.http.Header;

import indi.crawler.processor.ProcessorContext;
import indi.crawler.processor.ProcessorResult;
import indi.crawler.task.Task;
import indi.crawler.task.def.TaskType;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LogProcessor extends HttpProcessor {

	private String stringifyHeaders(Header[] headers) {
		StringBuilder sb = new StringBuilder();
		for (Header h : headers) {
			sb.append("[").append(h.getName()).append("=").append(h.getValue()).append("] ");
		}
		return sb.toString();
	}

	private String stringifyHTTPRequest(Task ctx) {
		StringBuilder sb = new StringBuilder("-> Send HTTP Request-> ");
		String method = ctx.getTaskDef().getMethod().name();
		String uri = ctx.getUri().toString();
		String headers = stringifyHeaders(ctx.getRequest().getAllHeaders());
		sb.append(method).append(" ").append(uri).append(" [").append(headers).append("]");
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

	private String stringifyHTTPResponse(Task ctx) {
	    if (ctx.getResponse() != null) {
	        StringBuilder sb = new StringBuilder("-<  Receive Response-> ");
	        String uri = ctx.getUri().toString();
	        String headers = stringifyHeaders(ctx.getResponse().getAllHeaders());
	        sb.append(uri).append("  ").append(" [").append(headers).append("]");
	        return sb.toString();
	    } else {
	        return "";
	    }
	}

	@Override
	protected ProcessorResult handleResult0(ProcessorContext hCtx) throws Throwable {
		Task ctx = hCtx.getCrawlerContext();
		if (ctx.getTaskDef().getType() == TaskType.HTTP_TOPICAL) {
		    log.info(stringifyHTTPResponse(ctx));
		}
		
		return ProcessorResult.KEEP_GOING;
	}

	@Override
	protected ProcessorResult afterHandleResult0(ProcessorContext iCtx) throws Throwable {
		Task ctx = iCtx.getCrawlerContext();
		if (ctx.getChilds() != null && ctx.getChilds().size() > 0) {
			StringBuilder sb = new StringBuilder("> After handle result, Add [")
					.append(ctx.getChilds().size()).append("] new Context to the Pool");
			log.info(sb.toString());
		}
		
		return ProcessorResult.KEEP_GOING;
	}
}
