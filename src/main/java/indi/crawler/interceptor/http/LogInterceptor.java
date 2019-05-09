package indi.crawler.interceptor.http;

import org.apache.http.Header;

import indi.crawler.interceptor.InterceptorContext;
import indi.crawler.nest.CrawlerContext;
import indi.crawler.task.TaskType;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LogInterceptor extends HttpInterceptor {

	private String stringifyHeaders(Header[] headers) {
		StringBuilder sb = new StringBuilder();
		for (Header h : headers) {
			sb.append("[").append(h.getName()).append("=").append(h.getValue()).append("] ");
		}
		return sb.toString();
	}

	private String stringifyHTTPRequest(CrawlerContext ctx) {
		StringBuilder sb = new StringBuilder("-> Send HTTP Request-> ");
		String method = ctx.getTask().getMethod().name();
		String uri = ctx.getUri().toString();
		String headers = stringifyHeaders(ctx.getRequest().getAllHeaders());
		sb.append(method).append(" ").append(uri).append(" [").append(headers).append("]");
		return sb.toString();
	}

	@Override
	public void executeRequest(InterceptorContext iCtx) {
		CrawlerContext ctx = iCtx.getCrawlerContext();
		if (ctx.getTask().getType() == TaskType.HTTP_TOPICAL) {
		    log.info(stringifyHTTPRequest(ctx));
		}
		
		super.executeRequest(iCtx);
	}

	private String stringifyHTTPResponse(CrawlerContext ctx) {
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
	public void afterReceiveResponse(InterceptorContext hCtx) {
		CrawlerContext ctx = hCtx.getCrawlerContext();
		if (ctx.getTask().getType() == TaskType.HTTP_TOPICAL) {
		    log.info(stringifyHTTPResponse(ctx));
		}
		
		super.afterReceiveResponse(hCtx);
	}

	@Override
	public void afterHandleResult(InterceptorContext iCtx) {
		CrawlerContext ctx = iCtx.getCrawlerContext();
		if (ctx.getChilds() != null && ctx.getChilds().size() > 0) {
			StringBuilder sb = new StringBuilder("> After handle result, Add [")
					.append(ctx.getChilds().size()).append("] new Context to the Pool");
			log.info(sb.toString());
		}
		
		super.afterHandleResult(iCtx);
	}
}
