package indi.crawler.bootstrap;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.http.HttpResponse;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import indi.crawler.result.ResultHandler;
import indi.crawler.result.ResultHelper;
import indi.crawler.task.ResponseEntity.TYPE;
import indi.crawler.task.Task;
import indi.crawler.util.JobUtils;
import indi.crawler.util.RedisUtils;
import indi.exception.WrapperException;
import indi.io.ClassPathProperties;
import indi.io.FileUtils;
import indi.util.CompressionUtils;
import indi.util.SystemUtils;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import net.lingala.zip4j.util.Zip4jConstants;

/**
 * 爬取在Konachan收藏的图片。尽量详细地输出日志
 * 
 * <p>
 * 
 * <li>2019.12.06 目前的代码为linux vps版本。需要考虑到，
 * <pre>
 *      1. 与vps传输文件，上传慢但下载快
 *      2. vps本身执行速度（爬虫/压缩）特别快，与vps传输才是耗时最久的一环
 * </pre>
 * 
 * <p>目前版本兼容win与linx，但路径只能写死。
 * 
 * <p>2020.01.18：发现执行完最后一个请求会卡死。。。是因为关闭爬虫的监视器还在睡眠吗？Y 调低睡眠时间、提高监视频率后就没有这个问题了
 * 
 * @author wzh
 */
@Slf4j
public class KonachanFavBootstrap {
    /** 下载存储路径 for linux */
    private static final String LINUX_DOWNLOAD_PATH = "/root/konachan";
    /** 下载存储路径 for win */
    private static final String WIN_DOWNLOAD_PATH = "F:\\byCrawler\\konachan";
    /** 临时文件目录 for linux */
    private static final String LINUX_TMP_DIR_PATH = "/root/tmp/crawler";
    /** 临时文件目录 for win */
    private static final String WIN_TMP_DIR_PATH = "F:\\tmp\\crawler";
    
    /** 下载路径对象 */
    @Getter
    private Path downloadPath;
    /** 临时文件夹对象 */
    @Getter
    private Path tmpDirPath;
    
    /** 是否在下载完成后压缩图片 */
    @Getter
    @Setter
    private Boolean isCompressWhenComplete;
    /** 压缩包路径 */
    @Getter
    private Path zipPath;
    /** 压缩包密码 */
    @Getter
    private String zipPwd = ClassPathProperties.getProperty("/account.properties", "zip-pwd");

    /** 本地图片序号集合（缓存） */
    @Getter
    private static final Set<String> LOCAL_CODES = new HashSet<>();
    /** 已下载的图片序号集合（缓存） */
    @Getter
    private static final Set<String> DOWNLOADED_CODES = new HashSet<>();
    /** 线上存在的图片序号集合 */
    @Getter
    private static final Set<String> ONLINE_CODES = new HashSet<>();
    
    public KonachanFavBootstrap() {
        // 执行初始化逻辑
        init();
    }

    /**
     * 自定义压缩密码版本
     * 
     * @param zipPwd
     */
    public KonachanFavBootstrap(String zipPwd) {
        this.zipPwd = zipPwd;
        init();
    }
    
    /**
     * 初始化各项参数，目前有：
     * 
     * <li>图片下载地址
     * <li>临时文件地址
     * <li>初始化已下载图片的缓存
     */
    private void init() {
        log.info("开始初始化{}。。。", this.getClass().getSimpleName());
        if (SystemUtils.isWin()) {
            // windows
            downloadPath = Paths.get(WIN_DOWNLOAD_PATH);
            tmpDirPath = Paths.get(WIN_TMP_DIR_PATH);
            log.info("当前系统为Window系统，Konachan目录为：{}", downloadPath);
        } else {
            // linux
            downloadPath = Paths.get(LINUX_DOWNLOAD_PATH);
            tmpDirPath = Paths.get(LINUX_TMP_DIR_PATH);
            log.info("当前系统为Linux系统，Konachan目录为：{}", downloadPath);
        }
        zipPath = downloadPath.resolve("konachan.zip");
        
        // 若不存在，创建目录
        FileUtils.createDirectoryIfNotExist(downloadPath);
        FileUtils.createDirectoryIfNotExist(tmpDirPath);
        
        // 清理已下载的空文件
        clearEmptyDownloadedPic();
        
        // 刷新已下载记录缓存
        JobUtils.refreshLocalFileCaches(downloadPath, DOWNLOADED_CODES);;
        
        log.info("初始化 {} 完成", this.getClass().getSimpleName());
    }
    
    /**
     * 清理已下载的空文件
     */
    private void clearEmptyDownloadedPic() {
        log.info("正在清理空文件...");
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(downloadPath)) {
            stream.forEach(p -> {
                try {
                    if (Files.size(p) == 0) {
                        Files.delete(p);
                    }
                } catch (IOException e) {
                    throw new WrapperException(e);
                }
            });
        } catch (Exception e) {
            throw new WrapperException(e);
        }
        log.info("清理空文件完成");
    }
    
    /**
     * 用于解析解析Konachan图片文件的url，可用于多种格式的url。
     */
    private String parseKeyFromDownloadUrl(String url) {
        // v1
        // for https://konachan.com/post/show/292694/animal-bird-clouds-grass-m-b-original-scenic-schoo
        Matcher matcher = Pattern.compile("(?<=/)\\d+?(?=/)").matcher(url);
        if (matcher.find()) {
            return matcher.group();
        }
        // v2 2019.10.26
        // for https://konachan.com/image/dfc49bea21da57ae0a526efc076017dd/Konachan.com%20-%20290554%20dress%20elsword%20eve...
        matcher = Pattern.compile("(?<=/Konachan.com%20-%20)\\d+(?=%20)").matcher(url);
        if (matcher.find()) {
            return matcher.group();
        }
        throw new IllegalArgumentException("无法从uri中解析处图片id！！" + url);
    }
    
    private String getFullFileName(String url) {
        return parseKeyFromDownloadUrl(url) + "." + FileUtils.getSuffix(url);
    }
    
    /**
     * 获取图片文件的存储路径
     * 
     * @param url
     * @return
     */
    private Path getFilePath(String url) {
        return downloadPath.resolve(getFullFileName(url));
    }

    /**
     * 保存已下载的图片
     */
    ResultHandler downloadResultHandler = (ctx, helper) -> {
        if (ctx.getResponseEntity().getContent() instanceof String) {
            System.out.println("response content type error");
            System.exit(-1);
        }
        File tmpFile = (File) ctx.getResponseEntity().getContent();
        String uri = ctx.getUri().toString();
        
        Path p = getFilePath(uri);
        // 移动临时文件（将删除临时文件）
        Files.move(tmpFile.toPath(), p, StandardCopyOption.REPLACE_EXISTING);
    };

    private boolean errorCheck(Task ctx, Document doc, ResultHelper helper) {
        // 500 error check
        Element title = null;
        if ((title = doc.getElementsByTag("head").get(0)).getElementsByTag("title").html()
                .contains("Error")) {
            if (title.html().contains("Privoxy Error")) {
                System.out.println("!!! 代理网络错误... " + ctx.getUri());
                helper.addNewTask(ctx.getTaskDef().getName(), ctx.getUri().toString(), null);
                
            } else
                System.out.println(title.html());
            return false;

        } else {
            return true;
        }
    }

    /**
     * 在详情页获取原图地址
     */
    ResultHandler detailPageResultHandler = (ctx, helper) -> {
        HttpResponse response = ctx.getResponse();
        String html = (String) ctx.getResponseEntity().getContent();
        Document doc = Jsoup.parse(html);
        if (!errorCheck(ctx, doc, helper)) {
            return;
        }
        Elements es = doc.getElementsByClass("original-file-unchanged");
        if (es.size() == 0) {
            es = doc.getElementsByClass("original-file-changed");
        }
        if (response != null && es.size() == 0) {// response == null when read response from cache
            // 先判断是否为响应错误
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode != 200) {
                // 若响应错误，则不处理
                log.error("解析详情页时响应错误，状态码为 {}", statusCode);
                return;
            } else {
                // 若响应没问题，则标签确实不存在，需要中止爬虫，手动处理
                log.error("链接解析错误 标签不存在!!! " + ctx.getUri());
                System.exit(0);// warn ！！！
            }
        }
        String link = null;
        for (Element e : es) {
            if (e.html().contains("View larger")) {
                link = e.attr("href");
            }
            if (e.html().contains("PNG")) {
                link = e.attr("href");
                break;
            }
        }
        if (link == null) {
            log.error("详情页链接解析错误 url为空 !!! " + ctx.getUri() + "\n" + es);
            // remove cache
            String redisCacheUri = ctx.getTaskDef().getRedisCacheUri();
            // 若使用了缓存，清除缓存的数据
            if (redisCacheUri != null) {
                RedisUtils.getAsyncCommands(redisCacheUri).hdel("LHCF-REQUEST", ctx.getIdentityKey()).get();
            }
            return;
        }
        // 判断文件是否存在
        Path p = getFilePath(link);
        if (!Files.exists(p, LinkOption.NOFOLLOW_LINKS)) {
            helper.addNewTask("Download", link);
        }
        else {
            log.info("文件已存在");
        }
    };

    /**
     * 在列表页中获取下一页与详情页，并添加到相应的爬虫任务
     */
    ResultHandler listPageResultHandler = (ctx, helper) -> {
        log.debug("{}", ctx.getUri());
        String html = (String) ctx.getResponseEntity().getContent();
        Objects.requireNonNull(html);
//        System.out.println(html);
        Document doc = Jsoup.parse(html);
        String prefix = "https://konachan.com";
        if (!errorCheck(ctx, doc, helper)) {
            return;
        }
        // 获取下一页
        String postfix = null;
        try {
            postfix = doc.getElementsByClass("next_page").get(0).attr("href");
        } catch (Exception e) {
//            e.printStackTrace();
//            System.out.println(doc);
//            System.out.println(doc.getElementsByTag("body"));
            log.error("找不到下一页按钮 {}", ctx.getUri());
            return;
        }
        String nextPageUrl = null;
        if (postfix != null && postfix.length() > 0) {
            nextPageUrl = prefix + doc.getElementsByClass("next_page").get(0).attr("href");
            helper.addNewTask("GetFav", nextPageUrl);
        }
        // 获取本页图片预览url
        for (Element e : doc.getElementsByClass("thumb")) {
            String url = prefix + e.attr("href");
            helper.addNewTask("PreDownload", url);
        }
    };
    
    private CrawlerJob job;
    
    /**
     * 启动任务
     * 
     * @param args 爬虫参数数组，第0个为开始下载的收藏夹地址
     * @throws IOException
     */
    public void run(String... args) throws IOException {
        String favUrl = args[0];
        // 刷新本地图片序号的缓存
        JobUtils.refreshLocalFileCaches(downloadPath, DOWNLOADED_CODES);
        
//        FileUtils.clearDirectory(path);
//        log.info("清空目录");
        
        String redisURI = null;// 2019.10.26 设为null以测试速度会不会更快。。。好像确实快了很多。。。

        System.setProperty("jdk.http.auth.tunneling.disabledSchemes", "");// 启用HTTPS代理
        job = CrawlerJob.build()
                // 输出下载速度等信息
                .withSpeedLog()
                // 定义结束回调
                .withCloseCallback(() -> {
                    // 删除线上没有记录（即取消收藏）的文件
                    try {
                        deleteNoRecord();
                    } catch (IOException e) {
                        throw new WrapperException(e);
                    }
                    
                    // 打包下载的所有图片
                    if (isCompressWhenComplete) {
                        String dest = downloadPath.toString();
                        log.info("打包下载文件到{}中，密码为：{}。。。", dest, zipPwd);
                        CompressionUtils.packZip(dest, zipPath.toString(), zipPwd, Zip4jConstants.DEFLATE_LEVEL_ULTRA, true);
                        log.info("打包完成！");
                    } else {
                        log.info("未启用自动打包功能");
                    }
                    
                    // 接下来closeableMonitor 将结束整个爬虫体系，包括线程池的线程，还有相关的守护线程
                })
                .withTmpFolder(tmpDirPath.toString())// 指定整个爬虫业务的临时文件夹，可被单个爬虫任务定义的配置所覆盖
                .withRedisMQTaskPool(redisURI)
//                .withContextPoolMonitor()// 有bug，会抛空指针，待升级
                .withCloseableMonitor()// 开启关闭监视器，一旦没有待处理的爬虫任务就结束线程
                .withHTTPProxy("127.0.0.1", 10809)
                // 下载原图
                .withTask("Download")
                    .withResultHandler(downloadResultHandler)
                    .withTmpDir(tmpDirPath.toString())
                    .withResultType(TYPE.File)// FIXME: file 类型的redis存储不好实现。。。
                    .withPriority(3)
                    .withLogDetail()
//                    .withRedisCache("redis://@localhost:6379/0")
                    .withKeepReceiveCookie()
                    .and()
                // 访问详情页
                .withTask("PreDownload")
//                    .withPriority(2)
                    .withResultHandler(detailPageResultHandler)
                    .withRedisCache(redisURI)
                    .withKeepReceiveCookie()
                    // 配置请求校验器，返回false时不执行该爬虫任务
                    .withURIChecker(uri -> {
                        // 获取图片序号
                        String key = parseKeyFromDownloadUrl(uri);
                        
                        // 新增已下载记录
                        addDownloadedRecord(key);
                        // 判断是否已下载至本地，是则跳过下载
                        boolean isNeedDownload = !DOWNLOADED_CODES.contains(key);
                        if (!isNeedDownload) {
                            log.debug("跳过已下载图片{}", key);
                        }
                        return isNeedDownload;
                    })
                    // 配置爬虫的key的计算函数
                    .withKeyGenerator((url, request) -> {
                        // get 286704 from https://konachan.com/post/show/292694/animal-bird-clouds-grass-m-b-original-scenic-schoo
                        return "PreDownload" + parseKeyFromDownloadUrl(url);
                    })
//                    .withLogDetail()
                    .and()
                // 获取分页列表页
                .withTask("GetFav")
                    .withResultHandler(listPageResultHandler)
                    .withLogDetail()
                    .withKeepReceiveCookie()
                    .withBlockingMillis(10000L)// 每10s处理一个收藏夹页面
                    .and();
                /*
                 * 启动项目。由于启动时建立了线程池，该方法执行完成后不会导致main方法结束
                 */
        boolean start = job.start("GetFav", favUrl);
        if (start) {
            System.out.println("---LHCF 爬虫任务启动完成---");
        } else {
            System.out.println("---LHCF 爬虫任务启动失败---");
        }
    }
    
    /**
     * 从默认收藏夹开始下载
     * 
     * @author DragonBoom
     * @since 2020.03.25
     * @throws IOException
     */
    public void run() throws IOException {
        this.run("https://konachan.com/post?page=1&tags=vote%3A3%3Adargonboom+order%3Avote");
    }
    
    /**
     * 新增已下载记录
     * 
     * @param code
     */
    private void addDownloadedRecord(String code) {
//        deprecated 没必要，只要在启动时，读取本地已下载文件而不是下载记录就行
//        // 通过增量修改文件实现实时更新，以应对下载一半中止的问题
//        try {
//            Files.write(expectOnlineRecordFileTmpPath, (code + ",").getBytes(), StandardOpenOption.APPEND);
//        } catch (IOException e) {
//            throw new WrapperException(e);
//        }
        ONLINE_CODES.add(code);
    }
    
    public Boolean close() {
        if (job != null) {
            if (job.getController() != null) {
                job.getController().close();
            }
        }
        return true;
    }
    
    /**
     * 删除不存在于已下载记录中的本地图片，在下载完成后调用
     * 
     * @throws IOException
     */
    protected void deleteNoRecord() throws IOException {
        JobUtils.delNoRecordFiles(ONLINE_CODES, DOWNLOADED_CODES, downloadPath);
        // 更新已下载记录的缓存
        JobUtils.refreshLocalFileCaches(downloadPath, DOWNLOADED_CODES);
    }
}
