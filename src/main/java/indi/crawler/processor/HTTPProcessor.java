package indi.crawler.processor;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;

import indi.crawler.nest.CrawlerContext;
import indi.crawler.nest.CrawlerController;
import indi.crawler.nest.ResponseEntity;
import indi.crawler.nest.ResponseEntity.TYPE;
import indi.crawler.result.ResultHandler;
import indi.crawler.task.SpecificTask;

/**
 * 包含HTTP爬虫的处理逻辑
 * 
 * @author DragonBoom
 *
 */
public class HTTPProcessor extends Processor {
    private static final String DEFAULT_CHARSET = "utf-8";
    private static final int DEFAULT_MAX_ROUTE = Integer.MAX_VALUE;
    private static final int DEFAULT_MAX_PER_ROUTE = Integer.MAX_VALUE;
    private HttpClient client;
    
    public HTTPProcessor() {
        init();
    }

    private void init() {
        PoolingHttpClientConnectionManager manager = new PoolingHttpClientConnectionManager();
        manager.setMaxTotal(DEFAULT_MAX_ROUTE); // TODO
        manager.setDefaultMaxPerRoute(DEFAULT_MAX_PER_ROUTE); // TODO
        client = HttpClientBuilder.create().setConnectionManager(manager).build();
    }

    public HttpResponse executeRequest(CrawlerContext ctx) throws Exception {
        HttpRequestBase request = ctx.getRequest();
        HttpResponse response = null;
        HttpHost host = ctx.getHost();// TODO
        if (host == null)
            response = client.execute(request);
        else
            response = client.execute(host, request);
        ctx.setResponse(response);
        return response;
    }

    public ResponseEntity receiveResponse(CrawlerContext ctx) throws Exception {
        String charset = null;
        // TODO
        charset = Optional.ofNullable(charset).orElse(DEFAULT_CHARSET);
        HttpResponse response = ctx.getResponse();
        HttpEntity entity = response.getEntity();
        // when gzip
//        GzipDecompressingEntity gzipDecompressingEntity = new GzipDecompressingEntity(entity);
//        gzipDecompressingEntity.getContent();
        ResponseEntity responseEntity = ctx.getResponseEntity();
        
        Object resultV = null;
        resultV = EntityUtils.toByteArray(entity);

        TYPE type = responseEntity.getType();
        Objects.requireNonNull(type);
        switch (responseEntity.getType()) {
        case String:
            resultV = new String((byte[]) resultV, charset);
            break;
        case ByteArray:
            break;
        default:
            throw new RuntimeException("Not Support response type " + responseEntity.getType());
        }
        EntityUtils.consume(entity);// 结束响应
        responseEntity.setContent(resultV);
        return responseEntity;
    }

    public void handleResult(CrawlerContext ctx) throws Exception {
        ResultHandler handler = ctx.getTask().getResultHandler();
        if (handler == null) {
            return;
        }
        List<SpecificTask> tasks = handler.process(ctx);
        if (tasks == null) {
            return;
        }
        ctx.setChilds(tasks);
    }
    
    /**
     * 添加新任务
     */
    public void afterHandleResult(CrawlerContext ctx) throws Exception {
        List<SpecificTask> tasks = ctx.getChilds();
        Iterator<SpecificTask> i = tasks.iterator();
        CrawlerController controller = ctx.getController();
        while (i.hasNext()) {
            if (!controller.offer(i.next().toCrawlerContext(controller))) {
                i.remove(); // 若无法存入上下文池（重复任务），则将其移除
            }
        }
        ctx.setChilds(tasks);
    }
}
