package indi.crawler.bootstrap;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.FileTime;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.google.common.collect.ImmutableSet;

import indi.crawler.result.ResultHandler;
import indi.crawler.result.ResultHelper;
import indi.crawler.task.ResponseEntity.TYPE;
import indi.crawler.task.Task;
import indi.crawler.util.JobUtils;
import indi.crawler.util.RedisUtils;
import indi.data.Pair;
import indi.exception.WrapperException;
import indi.io.ClassPathProperties;
import indi.io.FileUtils;
import indi.io.JsonPersistCenter;
import indi.io.Persist;
import indi.io.PersistCenter;
import indi.util.CompressionUtils;
import indi.util.ProcessUtils;
import indi.util.StringUtils;
import indi.util.SystemUtils;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import net.lingala.zip4j.util.Zip4jConstants;

/**
 * 出版由copy Konachan版而来。这两个网站，似乎Konachan是Danbooru的分支或旧版本。
 * 
 * <p>2020.11.28 发现解析Danbooru的cookie会有问题，但不影响使用，因此直接禁用cookie机制
 * 
 * <p>爬取在Konachan收藏的稿件。尽量详细地输出日志
 * 
 * <p>
 * 
 * <li>2019.12.06 目前的代码为linux vps版本。需要考虑到
 * <pre>
 *      1. 与vps传输文件，上传慢但下载快
 *      2. vps本身执行速度（爬虫/压缩）特别快，与vps传输才是耗时最久的一环
 * </pre>
 * 
 * <p>目前版本兼容win与linx，但路径只能写死。
 * 
 * <p>2020.01.18：发现执行完最后一个请求会卡死。。。是因为关闭爬虫的监视器还在睡眠吗？Y 调低睡眠时间、提高监视频率后就没有这个问题了
 * 
 * <p>2021.08.04 新增按收藏顺序修改时间的功能，从而实现按收藏顺序排序（不是将创建时间改为收藏时间）（即使没有下载也会更新时间）。
 * 2022.08.01 补充：实际上，每次执行都会重置所有文件的时间为 本次执行起始时间+遍历序号，以此实现一种相对的排序
 * 
 * <p>2022.08.01  
 * 新增爬取作者的功能。如果检测不到作者信息，将记录到配置文件中。将任务的参数的格式由字符串改为专门的对象Arg
 * 
 * @author wzh
 */
@Slf4j
public class DanbooruFavBootstrap {
    /*
     * TODO: 可配化
     */
    /** 下载存储路径 for linux */
    private static final String LINUX_DOWNLOAD_PATH = "/root/danbooru";
    /** 下载存储路径 for win */
    private static final String WIN_DOWNLOAD_PATH = "F:\\byCrawler\\danbooru";
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
    
    /** 是否在下载完成后压缩稿件 */
    @Getter
    @Setter
    private Boolean isCompressWhenComplete;
    /** 压缩包路径 */
    @Getter
    private Path zipPath;
    /** 压缩包密码 */
    @Getter
    private String zipPwd = ClassPathProperties.getProperty("/account.properties", "zip-pwd");

    /** 本地稿件序号集合（缓存） */
    @Getter
    private static final Set<String> LOCAL_CODES = new HashSet<>();
    /** 已下载的稿件序号集合（缓存） */
    @Getter
    private static final Set<String> DOWNLOADED_CODES = new HashSet<>();
    /** 线上存在的稿件序号集合 */
    @Getter
    private static final Set<String> ONLINE_CODES = new HashSet<>();
    /** 开始执行时间，将以此为起点修改文件的创建、修改、访问时间 */
    private static Long startMillis;
    /** 临时ID，递减，串行访问（用于为文件排序：将该值与startMillis和作为文件的时间，以此为文件排序） */
    private static int orderId = 9999;
    
    @Setter
    private boolean logAllDetail = false;
    @Setter
    private boolean useProxy = false;
    @Setter
    private String proxyHost;
    @Setter
    private int proxyPort;
    
    /** 已经更新了作者名等元数据的作品的id集合（key） */
    @Persist @Setter @Getter
    private List<String> haveMetadatas = new LinkedList<>();
    /** 无法获得作者名等元数据的作品的id集合（key） */
    @Persist @Setter @Getter
    private List<String> noMetadatas = new LinkedList<>();
    
    PersistCenter persistCenter;
    
    public DanbooruFavBootstrap() {
        // 执行初始化逻辑
        init();
    }

    /**
     * 自定义压缩密码版本
     * 
     * @param zipPwd
     */
    public DanbooruFavBootstrap(String zipPwd) {
        this.zipPwd = zipPwd;
        init();
    }
    
    /** 不需要考虑的文件后缀集合 */
    private static final Set<String> SUFFIX_BLACKLIST = ImmutableSet.of("txt", "properties", "json");
    /** 用于过滤掉不需要考虑的文件 */
    private static Predicate<Path> VALIDATE_FILE_CHECKER = p -> {
        String str = p.toString();
        for (String suffix : SUFFIX_BLACKLIST) {
            if (str.endsWith(suffix)) {
                return false;
            }
        }
        return true;
    };
    
    /**
     * 初始化各项参数，目前有：
     * 
     * <li>稿件下载地址
     * <li>临时文件地址
     * <li>初始化已下载稿件的缓存
     */
    private void init() {
        log.info("开始初始化{}。。。", this.getClass().getSimpleName());
        if (SystemUtils.isWin()) {
            // windows
            downloadPath = Paths.get(WIN_DOWNLOAD_PATH);
            tmpDirPath = Paths.get(WIN_TMP_DIR_PATH);
            log.info("当前系统为Window系统，Danbooru目录为：{}", downloadPath);
        } else {
            // linux
            downloadPath = Paths.get(LINUX_DOWNLOAD_PATH);
            tmpDirPath = Paths.get(LINUX_TMP_DIR_PATH);
            log.info("当前系统为Linux系统，Danbooru目录为：{}", downloadPath);
        }
        zipPath = downloadPath.resolve("Danbooru.zip");// FIXME:
        
        // 若不存在，创建目录
        FileUtils.createDirectoryIfNotExist(downloadPath);
        FileUtils.createDirectoryIfNotExist(tmpDirPath);
        
        // 清理已下载的空文件
        clearEmptyDownloadedFile();
        
        // 刷新已下载记录缓存
        JobUtils.refreshLocalFilenameCaches(downloadPath, DOWNLOADED_CODES, VALIDATE_FILE_CHECKER);
        
        log.info("初始化 {} 完成", this.getClass().getSimpleName());
    }
    
    /**
     * 清理已下载的空文件
     */
    private void clearEmptyDownloadedFile() {
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
     * 用于解析解析Konachan详情页的url以获得id，可用于多种格式的url。
     * 
     * @param url 详情页url，而不是下载url（下载url不含id）
     */
    private String parseKeyFromDetailUrl(String url) {
        // v1
        // for https://danbooru.donmai.us/posts/4217133?q=elf
        // for https://danbooru.donmai.us/posts/4059841
        Matcher matcher = Pattern.compile("(?<=/)\\d+?(?=(/|\\?|$))").matcher(url);
        if (matcher.find()) {
            return matcher.group();
        }
        // v2 2019.10.26
        // for https://konachan.com/image/dfc49bea21da57ae0a526efc076017dd/Konachan.com%20-%20290554%20dress%20elsword%20eve...
        // FIXME: 暂时保留
        matcher = Pattern.compile("(?<=/danbooru.donmai.us%20-%20)\\d+(?=%20)").matcher(url);
        if (matcher.find()) {
            return matcher.group();
        }
        // v3 2021.04.13
        // for https://danbooru.donmai.us/posts/4193264?q=ordfav%3Adragonboom
        throw new IllegalArgumentException("无法从uri中解析处稿件id！！" + url);
    }
    
    private String getFullFileName(String key, String downloadUrl) {
        return key + "." + FileUtils.getExtension(downloadUrl);
    }
    
    /**
     * 获取稿件的存储路径
     * 
     * @param key 稿件id
     * @param downloadUrl 下载地址
     * @return
     */
    private Path getFilePath(String key, String downloadUrl) {
        return downloadPath.resolve(getFullFileName(key, downloadUrl));
    }

    @Deprecated
    public String encodeArg(@Nullable String key, @Nonnull String orderId, @Nullable String artist) {
        
        return key == null ? orderId : key + "|" + orderId.toString() + "|" + artist;
    }

    @Deprecated
    /** return <\key, orderId> */
    public Pair<String, String> decodeArg(Object arg) {
        String argStr = (String) arg;
        if (argStr.contains("|")) {
            String[] split = argStr.split("\\|");
            if (split == null || split.length != 2) {
                throw new IllegalArgumentException();
            }
            return Pair.of(split[0], split[1]);
        } else {
            return Pair.of(null, argStr);
        }
    }

    /**
     * 保存已下载的稿件
     */
    ResultHandler downloadResultHandler = (ctx, helper) -> {
        if (ctx.getResponseEntity().getContent() instanceof String) {
            System.out.println("response content type error");
            System.exit(-1);
        }
        File tmpFile = (File) ctx.getResponseEntity().getContent();
        String uri = ctx.getUri().toString();
        Arg arg = (Arg) ctx.getArg();
        String key = arg.getKey();
        int orderId = arg.getOrderId();
        
        Path p = getFilePath(key, uri);
        // 移动临时文件（将删除临时文件）
        Files.move(tmpFile.toPath(), p, StandardCopyOption.REPLACE_EXISTING);
        // 更新时间
        updateFileTime(p, orderId);
        updateMetadata(p, arg, logAllDetail);
        
        log.info("下载完成：{}", p);
    };
    
    /** 重新设置文件时间 */
    public void updateFileTime(Path p, int orderId) throws IOException {
        FileTime time = FileTime.fromMillis(startMillis + orderId * 1000);
        Files.getFileAttributeView(p, BasicFileAttributeView.class).setTimes(time, time, time);
    }

    /** 通过元数据，补充作品信息（如作者名），使得可以以此进行管理
     * 
     * <p>由于用Java执行windows命令，若直接执行完整命令，会将命令按系统默认的GBK去编码，会导致用UTF-8查看元数据时出现乱码；
     * 因此，现采用文本参数的形式执行命令，能达成目标但开销较大。
     * 
     * <p>复制自 PixivTotalTest
     * 
     * @param path 图片路径
     * @since 2021.02.04
     */
    private void updateMetadata(Path path, Arg work, boolean LOG_DETAIL) throws Exception {
        if (LOG_DETAIL) {
            log.info("开始更新标签 {}", path);
        }
        
        String artist = work.getArtist();
        if (StringUtils.isEmpty(artist)) {
            if (!noMetadatas.contains(work.getKey())) {
                noMetadatas.add(work.getKey());
                persistCenter.persist();
            }
            return;
        }
        // 在图片的同级路径下，创建用于读取utf8编码指令的临时文件
        Path tmp = FileUtils.createTmpFile(path.getParent()).toPath();
        if (LOG_DETAIL) {
            log.debug("生成传递命令的临时文件 {}", tmp);
        }
        String utf8Command = "-comment=" + artist;
        Files.write(tmp, utf8Command.getBytes("utf-8"), StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
        
        // 将作者名称写入到comment字段（xnview可用该字段排序）
        String[] commands = new String[] { "exiftool", 
                "\"" + path.toString() + "\"", 
                "-@", "\"" + tmp + "\"",// 通过-@参数，引用文件内容作为指令
                "-overwrite_original"// 不保留原文件
                };
        ProcessUtils.process(commands, null);
        // 移除临时文件
        if (LOG_DETAIL) {
            log.debug("移除传递命令的临时文件 {}", tmp);
        }
        Files.delete(tmp);
        if (!haveMetadatas.contains(work.getKey())) {
            haveMetadatas.add(work.getKey());
            persistCenter.persist();
        }
        if (LOG_DETAIL) {
            log.info("更新标签成功 {}", path);
        }
    }
    
    /** 根据已有文件信息，更新元数据记录。主要用于移除已删除文件的元数据记录 */
    private void refreshMetadataRecord() {
        // 去重
        haveMetadatas = haveMetadatas.stream().distinct().collect(Collectors.toList());
        // 删除不存在的文件的记录
        Iterator<String> iterator = haveMetadatas.iterator();
        while(iterator.hasNext()) {
            String key = iterator.next();
            if (!DOWNLOADED_CODES.contains(key)) {
                iterator.remove();
            }
        }
        iterator = noMetadatas.iterator();
        while(iterator.hasNext()) {
            String key = iterator.next();
            if (!DOWNLOADED_CODES.contains(key)) {
                iterator.remove();
            }
        }
    }
    
    private boolean errorCheck(Task ctx, Document doc, ResultHelper helper) {
        // 500 error check
        Element title = doc.getElementsByTag("head").get(0);
        if (title.getElementsByTag("title").html()
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
     * 处理详情页：
     * 
     * <li>获取下载地址
     * <li>获取作者信息
     */
    ResultHandler detailPageResultHandler = (ctx, helper) -> {
        String html = (String) ctx.getResponseEntity().getContent();
        URI uri = ctx.getUri();
        String key = parseKeyFromDetailUrl(uri.toString());
        // 更新任务参数：+key
        Arg arg = (Arg) ctx.getArg();
        arg.setKey(key);
        int currentOrderId = arg.getOrderId();
        
        Document doc = Jsoup.parse(html);
        if (!errorCheck(ctx, doc, helper)) {
            return;
        }
        String link = null;
        // a. 处理稿件格式
        Elements es = doc.getElementsByClass("image-view-original-link");
        // 1) 图片下的下载原图链接
        if (StringUtils.isEmpty(link) && es.size() != 0) {
            // 任取其中一个元素即可
            Element e = es.get(0);
            link = e.attr("href");
        }
        // 对于 Danbooru，除了稿件外可能还有webm视频格式（另外还有mp4格式的视频）
        // 2) 处理webm视频
        if (StringUtils.isEmpty(link)) {
            Elements video = doc.getElementsByTag("video");
            if (video != null) {
                link = video.attr("src");
            } 
        }
        // 3) 侧边栏的下载按钮
        // 注意，该按钮可能会下载视频的压缩包
        
        if (StringUtils.isEmpty(link)) {
            Element download = doc.getElementById("post-option-download");
            if (download != null) {
                link = download.child(0).attr("href");
                System.out.println();
                // 此时要去掉末尾可能存在的查询条件，避免引入特殊格式，需应对的链接如：
                // https://danbooru.donmai.us/data/__jeanne_d_arc_and_jeanne_...e0a5.jpg?download=1
                if (link.contains("?")) {
                    link = link.substring(0, link.lastIndexOf("?"));
                }
            }
        }
        // error
        if (StringUtils.isEmpty(link)) {
            log.warn("该稿件没有原图链接，可能不是图片格式：{}", ctx.getUri());
        }
        
        if (StringUtils.isEmpty(link)) {
            log.error("详情页链接解析错误 url为空 !!! " + ctx.getUri() + "\n" + es);
            // remove cache
            String redisCacheUri = ctx.getTaskDef().getRedisCacheUri();
            // 若使用了缓存，清除缓存的数据
            if (redisCacheUri != null) {
                RedisUtils.getAsyncCommands(redisCacheUri).hdel("LHCF-REQUEST", ctx.getIdentityKey()).get();
            }
            return;
        }
        // b. 获取作者信息
        String artist = null;
        Elements ul = doc.getElementsByClass("artist-tag-list");
        if (!ul.isEmpty()) {
            // 元素集合：[h3,ul]
            if (ul.size() >= 2) {
                Element e = ul.get(1);// ul
                if (e.childNodeSize() > 0) {
                    Element li = e.child(0);
                    artist = li.attr("data-tag-name");
                }
            }
        }
        if (StringUtils.isEmpty(artist)) {
            log.warn("获取不到作者信息：{}", ctx.getUri());
            if (!noMetadatas.contains(key)) {
                noMetadatas.add(key);
                persistCenter.persist();
            }
        } else {
            // 更新任务参数：+key
            arg.setArtist(artist);
        }
        
        // 跳过已下载
        if (DOWNLOADED_CODES.contains(key)) {
            log.info("文件已存在: {}", uri);
            
            // 更新时间（更新为相对的时间，以用于排序）
            Path p = getFilePath(key, link);
            updateFileTime(p, currentOrderId);
            if (!haveMetadatas.contains(key) && !noMetadatas.contains(key)) {
                updateMetadata(p, arg, logAllDetail);
            }
        } else {
            helper.addNewTask("Download", link, null, arg);
            log.info("获得下载链接：{}", link);
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
        if (!errorCheck(ctx, doc, helper)) {
            return;
        }
        String prefix = "https://danbooru.donmai.us";
        // 获取、添加本页稿件详情页的url
        Elements eles = doc.getElementsByClass("post-preview-link");
        for (Element ele : eles) {
            String url = prefix + ele.attr("href");
            helper.addNewTask("DetailPage", url, null, new Arg(orderId));
            orderId--;
        }
        // 获取、添加下一页
        String postfix = null;
        
        List<Element> nextPageButtons = doc.getElementsByClass("paginator-next");
        if (nextPageButtons.size() != 1) {
            log.error("无法在列表页中定位下一页按钮");
        }
        if (nextPageButtons.size() == 1) {
            Element nextPageButton = nextPageButtons.get(0);
            if (nextPageButton != null) {
                try {
                    postfix = nextPageButton.attr("href");
                } catch (Exception e) {
                    log.error("在列表页中找不到下一页按钮 {}", ctx.getUri());
                    return;
                }
            }
        }
        String nextPageUrl = null;
        if (postfix != null && postfix.length() > 0) {
            nextPageUrl = prefix + postfix;
            helper.addNewTask("GetFav", nextPageUrl);
        }
    };
    
    private CrawlerJob job;
    
    /**
     * 启动任务
     * 
     * @param args 爬虫参数数组，第0个为开始下载的收藏夹地址
     */
    public void run(String... args) throws Exception {
        persistCenter = new JsonPersistCenter(Paths.get(WIN_DOWNLOAD_PATH), this);
        persistCenter.read();
        
        startMillis = System.currentTimeMillis();
        String favUrl = args[0];
        // 刷新本地稿件序号的缓存
        JobUtils.refreshLocalFilenameCaches(downloadPath, DOWNLOADED_CODES, VALIDATE_FILE_CHECKER);
        
        refreshMetadataRecord();
        
//        FileUtils.clearDirectory(path);
//        log.info("清空目录");
        
        String redisURI = null;// 2019.10.26 设为null以测试速度会不会更快。。。好像确实快了很多。。。

        System.setProperty("jdk.http.auth.tunneling.disabledSchemes", "");// 对HTTPS启用代理
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
                    
                    // 打包下载的所有稿件
                    if (isCompressWhenComplete) {
                        String dest = downloadPath.toString();
                        log.info("打包下载文件到{}中，密码为：{}。。。", dest, zipPwd);
                        CompressionUtils.packZip(dest, zipPath.toString(), zipPwd, Zip4jConstants.DEFLATE_LEVEL_ULTRA, true);
                        log.info("打包完成！");
                    } else {
                        log.debug("未启用自动打包功能");
                    }
                    
                    // 接下来closeableMonitor 将结束整个爬虫体系，包括线程池的线程，还有相关的守护线程
                    try {
                        persistCenter.persist();
                    } catch (Exception e) {
                        throw new WrapperException(e);
                    }
                })
                .withTmpFolder(tmpDirPath.toString())// 指定整个爬虫业务的临时文件夹，可被单个爬虫任务定义的配置所覆盖
                .withRedisMQTaskPool(redisURI)
                .withHTTPProxy(proxyHost, proxyPort, useProxy)
                // 下载原图
                .withTask("Download")
                    .withResultHandler(downloadResultHandler)
                    .withTmpDir(tmpDirPath.toString())
                    .withResultType(TYPE.TMP_FILE)// FIXME: file 类型的redis存储不好实现。。。
                    .withPriority(3)
                    .withBlockingMillis(3000L)// 每3s执行一次下载
                    // 开启对RFC6265的支持
                    .withRequestConfigBuilder(RequestConfig.custom().setCookieSpec(CookieSpecs.STANDARD))
                    .withLogDetail(logAllDetail)
//                    .withRedisCache("redis://@localhost:6379/0")
                    .withKeepReceiveCookie()
                    .and()
                // 访问详情页
                .withTask("DetailPage")
//                    .withPriority(2)
                    .withResultHandler(detailPageResultHandler)
                    .withRedisCache(redisURI)
                    .withRequestConfigBuilder(RequestConfig.custom().setCookieSpec(CookieSpecs.STANDARD))
                    .withKeepReceiveCookie()
                    // 配置请求校验器，返回false时不执行该爬虫任务
                    .withURIChecker(uri -> {
                        // 获取稿件序号
                        String key = parseKeyFromDetailUrl(uri);
                        
                        // 新增线上记录
                        addOnlineRecord(key);
                        // 判断是否已下载至本地 且 已设置了元数据，是则跳过详情页  【！！！】
                        if (DOWNLOADED_CODES.contains(key) && 
                                (haveMetadatas.contains(key) || noMetadatas.contains(key))) {
                            log.debug("跳过详情页: {}", key);
                            return false;
                        }
                        return true;
                    })
                    // 配置爬虫的key的计算函数
                    .withKeyGenerator((url, request) -> {
                        // get 286704 from https://konachan.com/post/show/292694/animal-bird-clouds-grass-m-b-original-scenic-schoo
                        return "DetailPage" + parseKeyFromDetailUrl(url);
                    })
                    .withLogDetail(logAllDetail)
                    .and()
                // 获取分页列表页
                .withTask("GetFav")
                    .withResultHandler(listPageResultHandler)
                    .withRequestConfigBuilder(RequestConfig.custom().setCookieSpec(CookieSpecs.STANDARD))
                    .withLogDetail(logAllDetail)
                    .withKeepReceiveCookie()
                    .withBlockingMillis(2000L)// 每2s处理一个列表页
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
     * 新增线上记录
     * 
     * @param code
     */
    private void addOnlineRecord(String code) {
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
     * 删除不存在于已下载记录中的本地稿件，在下载完成后调用
     * 
     * @throws IOException
     */
    protected void deleteNoRecord() throws IOException {
        JobUtils.delLocalFileByFilenameWhiteList(ONLINE_CODES, DOWNLOADED_CODES, downloadPath, 
                String::compareTo, "删除线上已移除的文件：{}");
        // 更新已下载记录的缓存
        JobUtils.refreshLocalFilenameCaches(downloadPath, DOWNLOADED_CODES, VALIDATE_FILE_CHECKER);
    }

    /**
     * 启动任务。必须用main函数，而不是测试用例的方式启动，因为测试用例会在主线程执行完后就结束其他线程，无法完成整个下载流程。
     * 
     * @throws IOException
     */
    public static void main(String[] args) throws Exception {
        SystemUtils.correctRunnableJarLog4j2();

        DanbooruFavBootstrap bootstrap = new DanbooruFavBootstrap();
        bootstrap.setIsCompressWhenComplete(false);
        bootstrap.setLogAllDetail(false);
        bootstrap.setUseProxy(true);
        bootstrap.setProxyHost("127.0.0.1");
        bootstrap.setProxyPort(10810);
        bootstrap.run("https://danbooru.donmai.us/posts?tags=ordfav%3Adragonboom");
    }
    
    /**
     * 表示任务参数的对象
     * 
     * @author DragonBoom
     * @since 2022-08-01
     */
    @Data
    public static class Arg implements Serializable {
        public Arg(int orderId) {
            this.orderId = orderId;
        }
        private static final long serialVersionUID = 1L;
        private String key;// 作品id
        private Integer orderId;// 临时的用于排序的id
        private String artist;// 作者名（一般都是英文名）
    }
}
