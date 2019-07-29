package indi.crawler.processor.http;

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

import indi.crawler.processor.ProcessorContext;
import indi.crawler.processor.ProcessorResult;
import indi.crawler.result.HttpResultHelper;
import indi.crawler.result.ResultHandler;
import indi.crawler.task.CrawlerController;
import indi.crawler.task.ResponseEntity;
import indi.crawler.task.ResponseEntity.TYPE;
import indi.crawler.task.Task;

/**
 * 负责处理Http连接。包含HTTP爬虫的处理逻辑
 * 
 * @author DragonBoom
 *
 */
public class HTTPConnectionProcessor extends HttpProcessor {
    private static final String DEFAULT_CHARSET = "utf-8";
    private static final int DEFAULT_MAX_ROUTE = Integer.MAX_VALUE;
    private static final int DEFAULT_MAX_PER_ROUTE = Integer.MAX_VALUE;
    private HttpClient client;
    
    private CrawlerController controller;
    
    public HTTPConnectionProcessor(CrawlerController controller) {
        init();
        this.controller = controller;
    }

    private void init() {
        PoolingHttpClientConnectionManager manager = new PoolingHttpClientConnectionManager();
        manager.setMaxTotal(DEFAULT_MAX_ROUTE); // TODO
        manager.setDefaultMaxPerRoute(DEFAULT_MAX_PER_ROUTE); // TODO
        client = HttpClientBuilder.create().setConnectionManager(manager).build();
    }

    @Override
    protected ProcessorResult executeRequest0(ProcessorContext pCtx) throws Exception {
        Task ctx = pCtx.getCrawlerContext();
        
        HttpRequestBase request = ctx.getRequest();
        HttpResponse response = null;
        HttpHost host = ctx.getHost();// TODO
        if (host == null)
            response = client.execute(request);
        else
            response = client.execute(host, request);
        ctx.setResponse(response);
        return ProcessorResult.CONTINUE_STAGE;
    }

    @Override
    protected ProcessorResult receiveResponse0(ProcessorContext pCtx) throws Exception {
        Task ctx = pCtx.getCrawlerContext();

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

        return ProcessorResult.CONTINUE_STAGE;
    }

    @Override
    protected ProcessorResult handleResult0(ProcessorContext pCtx) throws Exception {
        Task ctx = pCtx.getCrawlerContext();
        
        ResultHandler handler = ctx.getTaskDef().getResultHandler();
        HttpResultHelper resultHelper = new HttpResultHelper(controller.getTaskFactory());
        
        handler.process(ctx, resultHelper);
        List<Task> newTasks = resultHelper.getNewTask();
        ctx.setChilds(newTasks);
        return ProcessorResult.CONTINUE_STAGE;
    }
    
    /**
     * 添加新任务
     */
    @Override
    protected ProcessorResult afterHandleResult0(ProcessorContext pCtx) throws Exception {
        Task ctx = pCtx.getCrawlerContext();
        List<Task> tasks = ctx.getChilds();
        if (tasks != null) {
            Iterator<Task> i = tasks.iterator();
            CrawlerController controller = ctx.getController();
            while (i.hasNext()) {
                if (!controller.offer(i.next())) {
                    i.remove(); // 若无法存入上下文池（重复任务），则将其移除
                }
            }
            ctx.setChilds(tasks);
        }
        return ProcessorResult.CONTINUE_STAGE;
    }
}
