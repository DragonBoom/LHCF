package indi.crawler.processor.http;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

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
import indi.crawler.task.def.TaskDef;
import indi.crawler.thread.CrawlerThread;
import indi.io.FileUtils;
import lombok.extern.slf4j.Slf4j;

/**
 * 负责处理Http连接
 * 
 * @author DragonBoom
 *
 */
@Slf4j
public class HTTPConnectionProcessor extends HTTPProcessor {
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
        log.info("register HTTPConnectionProcessor...");
        PoolingHttpClientConnectionManager manager = new PoolingHttpClientConnectionManager();
        manager.setMaxTotal(DEFAULT_MAX_ROUTE); // TODO
        manager.setDefaultMaxPerRoute(DEFAULT_MAX_PER_ROUTE); // TODO
        client = HttpClientBuilder.create().setConnectionManager(manager).build();
    }

    @Override
    protected ProcessorResult executeRequest0(ProcessorContext pCtx) throws Exception {
        Task ctx = pCtx.getCrawlerContext();
        TaskDef taskDef = ctx.getTaskDef();
        Function<String, Boolean> checkFun = taskDef.getCheckFun();
        if (checkFun != null) {
            // 检测是否要执行当前任务，若不需要，则通过线程类的方法使线程直接跳过该任务，继续处理其他任务
            Boolean checkAccept = checkFun.apply(ctx.getUri().toString());
            if (!checkAccept) {
                CrawlerThread currentThread = (CrawlerThread) Thread.currentThread();
                currentThread.completeCurrentTask();
            }
        }
        
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
        TaskDef taskDef = ctx.getTaskDef();

        String charset = null;
        // TODO
        charset = Optional.ofNullable(charset).orElse(DEFAULT_CHARSET);
        HttpResponse response = ctx.getResponse();
        HttpEntity httpEntity = response.getEntity();
        // when gzip
//        GzipDecompressingEntity gzipDecompressingEntity = new GzipDecompressingEntity(entity);
//        gzipDecompressingEntity.getContent();
        ResponseEntity responseEntity = ctx.getResponseEntity();
        
        Object resultV = null;
        TYPE type = responseEntity.getType();
        Objects.requireNonNull(type);
        switch (responseEntity.getType()) {
        case String:
            byte[] bytes = EntityUtils.toByteArray(httpEntity);
            resultV = new String(bytes, charset);
            break;
        case ByteArray:
            resultV = EntityUtils.toByteArray(httpEntity);
            break;
        case File:// 将响应流写入临时文件中，可显著减少内存占用
            // generate tmp file
            // 临时文件的存放目录，优先取任务定义的路径，若没有，再取整个爬虫项目的路径，若也没有，则取默认路径
            File tmpFile = FileUtils.createTmpFile(Optional.ofNullable(taskDef.getTmpDir())
                    .orElse(Optional.ofNullable(controller.getJob().getTmpFolderPath()).orElse(null)));
            // 将响应流写入临时文件
            try (FileOutputStream outStream = new FileOutputStream(tmpFile);
                    InputStream inStream = httpEntity.getContent();) {
//                httpEntity.writeTo(outStream);// 为了能监控详细的下载进度，需要自己实现流的处理逻辑
                FileUtils.copyChannel(Channels.newChannel(inStream), Channels.newChannel(outStream), null);
            }
            resultV = tmpFile;
            break;
        default:
            throw new RuntimeException("Not Support response type " + responseEntity.getType());
        }
        EntityUtils.consume(httpEntity);// 结束响应
        responseEntity.setContent(resultV);

        return ProcessorResult.CONTINUE_STAGE;
    }

    @Override
    protected ProcessorResult handleResult0(ProcessorContext pCtx) throws Exception {
        Task ctx = pCtx.getCrawlerContext();
        
        ResultHandler handler = ctx.getTaskDef().getResultHandler();
        HttpResultHelper resultHelper = new HttpResultHelper(controller.getTaskFactory());
        
        handler.process(ctx, resultHelper);
        List<Task> newTasks = resultHelper.getNewTasks();
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
