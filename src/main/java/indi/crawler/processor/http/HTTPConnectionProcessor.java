package indi.crawler.processor.http;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.charset.Charset;
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
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;
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
        client = HttpClientBuilder.create()
                // 默认会根据状态码(301)，对GET/HEAD请求进行重定向，可见：DefaultRedirectStrategy
//                .setRedirectStrategy(redirectStrategy)
                .setConnectionManager(manager).build();
    }

    @Override
    protected ProcessorResult executeRequest0(ProcessorContext pCtx) throws Exception {
        Task task = pCtx.getCrawlerContext();
        TaskDef taskDef = task.getTaskDef();
        Function<String, Boolean> checkFun = taskDef.getCheckFun();
        if (checkFun != null) {
            // 检测是否要执行当前任务，若不需要，则通过线程类的方法使线程直接跳过该任务，继续处理其他任务
            Boolean checkAccept = checkFun.apply(task.getUri().toString());
            if (!checkAccept) {
                CrawlerThread currentThread = (CrawlerThread) Thread.currentThread();
                currentThread.completeCurrentTask();
            }
        }
        
        HttpRequestBase request = task.getRequest();
        HttpResponse response = null;
        
        HttpContext context = new BasicHttpContext();
        
        response = client.execute(request, context);
        
        task.setResponse(response);
        task.setHttpContext(context);
        return ProcessorResult.CONTINUE_STAGE;
    }
    
    @Override
    protected ProcessorResult receiveResponse0(ProcessorContext pCtx) throws Exception {
        Task task = pCtx.getCrawlerContext();
        TaskDef taskDef = task.getTaskDef();

        Charset charset = taskDef.getResultStringCharset();
        // TODO 可配
        charset = Optional.ofNullable(charset).orElse(Charset.defaultCharset());
        HttpResponse response = task.getResponse();
        HttpEntity httpEntity = response.getEntity();
        // when gzip
//        GzipDecompressingEntity gzipDecompressingEntity = new GzipDecompressingEntity(entity);
//        gzipDecompressingEntity.getContent();
        ResponseEntity responseEntity = task.getResponseEntity();
        
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
            String tempDir = Optional.ofNullable(taskDef.getTmpDir())
                    .orElse(Optional.ofNullable(controller.getJob().getTmpFolderPath()).orElse(null));
            Objects.requireNonNull(tempDir, "下载类型为文件时，必须指定缓存地址！");
            File tmpFile = FileUtils.createTmpFile(tempDir);
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
        Task task = pCtx.getCrawlerContext();
        
        ResultHandler handler = task.getTaskDef().getResultHandler();
        HttpResultHelper resultHelper = new HttpResultHelper(controller.getTaskFactory());
        
        handler.process(task, resultHelper);
        List<Task> newTasks = resultHelper.getNewTasks();
        task.setChilds(newTasks);
        return ProcessorResult.CONTINUE_STAGE;
    }
    
    /**
     * 添加新任务
     */
    @Override
    protected ProcessorResult afterHandleResult0(ProcessorContext pCtx) throws Exception {
        Task task = pCtx.getCrawlerContext();
        List<Task> tasks = task.getChilds();
        if (tasks != null) {
            Iterator<Task> i = tasks.iterator();
            CrawlerController controller = task.getController();
            while (i.hasNext()) {
                if (!controller.offer(i.next())) {
                    i.remove(); // 若无法存入上下文池（重复任务），则将其移除
                }
            }
            task.setChilds(tasks);
        }
        return ProcessorResult.CONTINUE_STAGE;
    }
}
