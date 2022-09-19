/**
 * 
 */
package indi.crawler.bootstrap;

import java.io.File;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.http.ConnectionClosedException;
import org.apache.http.impl.client.RedirectLocations;
import org.apache.http.message.BasicHeader;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

import indi.crawler.result.ResultHelper;
import indi.crawler.task.ResponseEntity.TYPE;
import indi.crawler.task.Task;
import indi.exception.WrapperException;
import indi.io.ClassPathProperties;
import indi.io.FileUtils;
import indi.io.JsonPersistCenter;
import indi.io.Persist;
import indi.io.PersistCenter;
import indi.util.CheckUtils;
import indi.util.StringUtils;
import indi.util.SystemUtils;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * 实现从轻小说文库自动更新本地小说。将从线上覆盖本地存在的书
 * 
 * <p>有时会抛UnknownHostException:dl.wenku8.com，原因不明，但重试几次即可。。。
 * 
 * <p>2021.12 有时与网站的网速会变得特别差，凌晨时有所好转
 * 
 * @author wzh
 * @since 2020.09.04
 */
@Slf4j
public class WenKu8Bootstrap implements Runnable {
    private String userName = ClassPathProperties.getProperty("/account.properties", "WenKu8-username");
    private String password = ClassPathProperties.getProperty("/account.properties", "WenKu8-password");
    
    private Path localPath = Paths.get("f:", "小说");
    
    /** 书名与轻小说文库ID的映射 */
    @Getter
    @Setter
    @Persist
    private Map<String, String> bookNameIdMap = new HashMap<>();
    /** 书名-上次检查时间 */
    @Getter
    @Setter
    @Persist
    private Map<String, Long> bookNameLastTimeMap = new HashMap<>();
    /** 跳过在这个时间内更新过的书籍 */
    private static final long MAX_LAST_TIME_MILLIS = TimeUnit.DAYS.toMillis(5);
    
    private Set<String> updatedBookNames = new HashSet<>();
    /**
     * 不需要更新的书名，暂时通过修改持久化文件编辑
     */
    @Getter
    @Setter
    @Persist
    private ArrayList<String> blacklist = new ArrayList<>();
    
    private PersistCenter persistCenter = new JsonPersistCenter(localPath, this);
    
    private static final String WIN_TMP_DIR_PATH = "F:\\tmp\\小说";
    
    private WenKu8Bootstrap() throws Exception {
        // 从文件中获取数据
        persistCenter.read();
        log.info("从文件中获取映射成功");
    }

    
    /** 登陆成功后执行的逻辑 */
    private void afterLogin(Task task, ResultHelper helper) throws Exception {
        String html = (String) task.getResponseEntity().getContent();
        if (html.contains("登录成功")) {
            log.info("登录成功");
        } else {
            log.error("登录失败：\n{}", html);
            System.exit(-1);// !!!!!!!!!!!!!!!!!!!!!!!!!!!!
            throw new Exception("登陆失败");
        }
        // 遍历本地目录，获取可能需要更新的书籍名称
        Set<String> bookNames = null;
        try (Stream<Path> fStream = FileUtils.list(localPath)) {
            bookNames = fStream
                    .filter(p -> !Files.isDirectory(p))// 忽视目录
                    .filter(p -> p.getFileName().toString().endsWith(".txt"))// 只考虑txt文件
                    .map(FileUtils::getFileName)
                    .filter(bookName -> {
                        if (blacklist.contains(bookName)) {
                            log.info("忽略黑名单书籍：{}", bookName);
                            return false;
                        }
                        Long lastTime = bookNameLastTimeMap.get(bookName);
                        if (lastTime != null && (System.currentTimeMillis() - lastTime) <= MAX_LAST_TIME_MILLIS) {
                            log.info("忽略{}小时内检查过的书籍：{}", TimeUnit.MILLISECONDS.toHours(MAX_LAST_TIME_MILLIS),
                                    bookName);
                            return false;
                        }
                        return true;
                    })
                    .collect(Collectors.toSet());
        }
        for (String bookName : bookNames) {
            // 尝试通过保存在持久化文件中的记录，直接用书名获取id
            String id = bookNameIdMap.get(bookName);
            if (!StringUtils.isEmpty(id)) {
//                log.info("从持久化文件中获取到 id：{}  = {} ", bookName, id);
                helper.addNewTask("Download", "http://dl.wenku8.com/down.php?type=utf8&id=" + id, null, bookName);
            } else {
                // 不能获取id的，需要先进行查找
                // 注意，这里把书名作为参数传入了Task对象中
                helper.addNewTask("SearchOrDetail", "http://www.wenku8.net/modules/article/search.php?searchtype=articlename&searchkey=" 
                        + URLEncoder.encode(bookName, "gbk"), null, bookName);
            }
        }
        
        // 构建下一环节的任务：搜索书籍，获取详情页
        
    }
    
    /** 用于从书的详情页URL中解析出ID的正则，形如：https://www.wenku8.net/book/1213.htm */
    private Pattern urlIdPattern = Pattern.compile("(?<=/book/)\\d+(?=\\.)");
    
    /** 根据书名搜索后的逻辑：若有搜索结果，获取该书的id，从而获取下载链接，添加到任务池 */
    private void afterSearch(Task task, ResultHelper helper) throws Exception {
        String bookName = (String) task.getArg();
        String html = (String) task.getResponseEntity().getContent();
        
        // 搜索后，若只有一个结果，会直接重定向到详情页
        // 判断是否详情页
        Document doc = Jsoup.parse(html);
        log.info("Search Page title={}", doc.title());
        boolean isDetailPage = !doc.title().contains("搜索结果");
        String detailPageUrl;// 详情页URL
        if (!isDetailPage) {
            // 根据书名查找标签
            Elements eles = doc.getElementsContainingText(bookName);
            detailPageUrl = eles.stream()
                    .filter(ele -> ele.tagName().equals("a"))
                    .map(ele -> ele.attr("href"))
                    .findFirst()
                    .orElse(null);
            // 添加详情页的任务
            if (StringUtils.isEmpty(detailPageUrl)) {
                // 找不到书（可能不是在轻小说文库下载的）
                log.info("找不到书，将加入黑名单：{}", bookName);
                // 保存至黑名单并持久化到文件中
                blacklist.add(bookName);
                persistCenter.persist();
                return;
            }
        } else {
            // 以进入书的详情页，表明发生了重定向
            // 获取重定向地址
            RedirectLocations redirectLocations = task.getRedirectLocations();
            if (redirectLocations == null) {
                System.out.println(html);
                // 当作连接被关闭（返回该异常将重新访问）
                throw new ConnectionClosedException("无法获取重定向链接：" + task.getUri());
            }
            // 获取最后一次重定向的url，即返回响应的请求的url
            detailPageUrl = redirectLocations.get(redirectLocations.size() - 1).toString();
            if (StringUtils.isEmpty(detailPageUrl)) {
                throw new WrapperException("无法获取情页URL");
            }
        }
        // 从URL中解析出书的ID
        Matcher matcher = urlIdPattern.matcher(detailPageUrl);
        String id;
        if (matcher.find()) {
            id = matcher.group();
        } else {
            throw new WrapperException("无法解析详情页URL：" + detailPageUrl);
        }
        // 保存书名与id的映射信息
        
        log.info("成功获取书 的id：{} = {}", bookName, id);
        // 保存、持久化映射
        bookNameIdMap.put(bookName, id);
        persistCenter.persist();
        // 构建下载请求：
        helper.addNewTask("Download", "http://dl.wenku8.com/down.php?type=utf8&id=" + id, null, bookName);
    }
    
    /** 进入下载详情页的逻辑：获取下载链接 */
    private void afterDownload(Task task, ResultHelper helper) throws Exception {
        String bookName = (String) task.getArg();
        bookNameLastTimeMap.put(bookName, System.currentTimeMillis());
        try {
            persistCenter.persist();
        } catch (Exception e) {
            throw new WrapperException(e);
        }
        
        File tmpFile = (File) task.getResponseEntity().getContent();
        Path source = tmpFile.toPath();
        Path target = localPath.resolve(bookName + ".txt");
        // 通过CRC32判断小说是否有更新
        if (CheckUtils.getCRC32(source).equals(CheckUtils.getCRC32(target))) {
            // 小说没有更新，不需要覆盖文件，移除临时文件即可
            log.info("检测到书没有更新：{}", bookName);
            Files.delete(source);
            return;
        }
        updatedBookNames.add(bookName);
        
        boolean exists = Files.exists(target);
        // 移动临时文件
        Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
        if (exists) {
            log.info("!! 更新完成：{}", bookName);
        } else {
            log.info("!! 新增完成：{}", bookName);
            
        }
    }
    
    private void clearTmp() {
        Path path = Paths.get(WIN_TMP_DIR_PATH);
        FileUtils.clearDirectory(path, false);
    }


    @Override
    public void run() {
        boolean logAllDetail = false;

        // 执行前后将清空临时目录
        clearTmp();
        
        CrawlerJob job = CrawlerJob.build()
                .withThreadCount(1)
                .withTmpFolder("f:/tmp/wenku8")
                .withCloseCallback(() -> {
                    // 将需要持久化的内容写入文件
                    try {
                        persistCenter.persist();
                    } catch (Exception e) {
                        throw new WrapperException(e);
                    }
                    log.info("persist completed");
                    if (!updatedBookNames.isEmpty()) {
                        log.info("已更新书籍：{}", updatedBookNames);
                    } else {
                        log.info("未更新任何书籍");
                    }
                })
                .withTask("Login")
                    .withMethod("POST")// 必须大写
                    .withRequestHeaders(
                            new BasicHeader("Content-Type", "application/x-www-form-urlencoded"))
                    .withResultCharset(Charset.forName("gbk"))// 指定解析响应html的编码
                    .withKeepReceiveCookie()
                    .withResultHandler(this::afterLogin)
                    .withLogDetail(logAllDetail)
                    .and()
                .withTask("SearchOrDetail")
                    .withLogDetail()
                    .withKeepReceiveCookie()
                    .withBlockingMillis(6000)// 每个任务完成后6s再执行下一个任务（网站限制搜索间隔为5s）
                    .withResultCharset(Charset.forName("gbk"))// 指定解析响应html的编码
                    .withResultHandler(this::afterSearch)
                    .withLogDetail(logAllDetail)
                    .and()
                .withTask("Download")
                    .withBlockingMillis(2000)// 每个任务完成后2s再执行下一个任务
                    .withKeepReceiveCookie()
                    .withResultType(TYPE.TMP_FILE)
                    .withTmpDir(WIN_TMP_DIR_PATH)
                    .withResultCharset(Charset.forName("gbk"))// 指定解析响应html的编码
                    .withResultHandler(this::afterDownload)
                    .withLogDetail(logAllDetail)
//                    .withMaxLeasedTime(5000)// for test
                    .and()
                    ;
        
        boolean start = job.start("Login", "https://www.wenku8.net/login.php?do=submit", 
                HttpEntityBuilder.beginEncodedFormEntity()
                    .with("username", userName)
                    .with("password", password)
                    .with("action", "login")
                    .build())
                ;
        if (start) {
            System.out.println("---LHCF 爬虫任务启动完成---");
        } else {
            System.out.println("---LHCF 爬虫任务启动失败---");
        }
        
        // 执行前后将清空临时目录
        clearTmp();
    }
    
    public static void main(String[] args) throws Exception {
        SystemUtils.correctRunnableJarLog4j2();
        new WenKu8Bootstrap().run();
    }

}
