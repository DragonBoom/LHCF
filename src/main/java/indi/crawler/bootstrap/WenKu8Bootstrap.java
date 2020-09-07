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
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.http.ConnectionClosedException;
import org.apache.http.impl.client.RedirectLocations;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

import indi.crawler.result.ResultHelper;
import indi.crawler.task.ResponseEntity.TYPE;
import indi.crawler.task.Task;
import indi.exception.WrapperException;
import indi.io.Base64PersistCenter;
import indi.io.FileUtils;
import indi.util.StringUtils;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * 实现从轻小说文库自动更新本地小说。将从线上覆盖本地存在的书
 * 
 * @author wzh
 * @since 2020.09.04
 */
@Slf4j
public class WenKu8Bootstrap implements Runnable {
    // FIXME: 脱敏
    private String userName = "1312449403";
    private String password = "qq1312449403";
    
    private Path localPath = Paths.get("f:", "小说");
    
    private Base64PersistCenter persistCenter = new Base64PersistCenter(localPath, this, "bookNameIdMap", "bookNameDateMap");
    
    /** 书名与轻小说文库ID的映射 */
    @Getter
    @Setter
    private Map<String, String> bookNameIdMap = new HashMap<>();
    /** 轻小说文库中不存在的书的id的占位符 */
    private static final String NOT_EXISTS_ID_PLACEHOLDER = "00";
    
    /** 书名与上次更新时间的映射 */
    @Getter
    @Setter
    private Map<String, Date> bookNameDateMap = new HashMap<>();
    
    private static final String WIN_TMP_DIR_PATH = "F:\\tmp\\小说";
    
    private WenKu8Bootstrap() throws Exception {
        // 从文件中获取数据
        persistCenter.read();
        log.info("从文件中获取映射成功");
        
        System.out.println(bookNameDateMap);
        System.out.println(bookNameIdMap);
    }

    
    /** 登陆成功后执行的逻辑 */
    private void afterLogin(Task task, ResultHelper helper) throws Exception {
        String html = (String) task.getResponseEntity().getContent();
        if (html.contains("登录成功")) {
            log.info("登录成功");
        } else {
            log.error("登录成功：\n{}", html);
        }
        // 遍历本地目录，获取可能需要更新的书籍名称
        Set<String> bookNames = null;
        try (Stream<Path> fStream = FileUtils.list(localPath)) {
            bookNames = fStream
                    .filter(p -> !Files.isDirectory(p))
                    .filter(p -> p.getFileName().toString().endsWith(".txt"))
                    .map(p -> {
                        String fileName = p.getFileName().toString();
                        return fileName.substring(0, fileName.length() - 4);// 移除后缀
                    })
                    .collect(Collectors.toSet());
        }
        for (String bookName : bookNames) {
            // 直接通过书名获取id
            String id = bookNameIdMap.get(bookName);
            if (!StringUtils.isEmpty(id)) {
                if (!NOT_EXISTS_ID_PLACEHOLDER.equals(id)) {
                    log.info("从持久化文件中获取到 {} 的 id= {} ", bookName, id);
                    helper.addNewTask("Download", "http://dl.wenku8.com/down.php?type=utf8&id=" + id, null, bookName);
                }
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
        log.info("page title={}", doc.title());
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
                bookNameIdMap.put(bookName, NOT_EXISTS_ID_PLACEHOLDER);
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
                throw new RuntimeException("无法获取情页URL");
            }
        }
        // 从URL中解析出书的ID
        Matcher matcher = urlIdPattern.matcher(detailPageUrl);
        String id;
        if (matcher.find()) {
            id = matcher.group();
        } else {
            throw new RuntimeException("无法解析详情页URL：" + detailPageUrl);
        }
        // 保存书名与id的映射信息
        
        log.info("成功获取书 {} 的 id = {}", bookName, id);
        bookNameIdMap.put(bookName, id);
        // 构建下载请求：
        helper.addNewTask("Download", "http://dl.wenku8.com/down.php?type=utf8&id=" + id, null, bookName);
    }
    
    /** 进入下载详情页的逻辑：获取下载链接 */
    private void afterDownload(Task task, ResultHelper helper) throws Exception {
        // 每下载完成一本书，就持久化name-id的映射
        persistCenter.persist();
        
        String bookName = (String) task.getArg();
        File tmpFile = (File) task.getResponseEntity().getContent();
        Path source = tmpFile.toPath();
        Path target = localPath.resolve(task.getArg() + ".txt");
        // 判断小说是否有更新
        if (Files.exists(target) && (Files.size(source) == Files.size(target))) {
            if (!bookNameDateMap.containsKey(bookName)) {
                bookNameDateMap.put(bookName, new Date());
            }
            // 小说没有更新，不需要覆盖文件，移除临时文件即可
            Files.delete(source);
            return;
        }
        // 更新下载时间
        bookNameDateMap.put(bookName, new Date());
        
        // 移动临时文件
        Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
        log.info("下载完成：{}", task.getArg());
    }

    @Override
    public void run() {
        
        CrawlerJob job = CrawlerJob.build()
                .withCloseCallback(() -> {
                    // 将需要持久化的内容写入文件
                    try {
                        persistCenter.persist();
                    } catch (Exception e) {
                        throw new WrapperException(e);
                    }
                    log.info("persist completed");
                })
                .withTask("Login")
                    .withMethod("POST")// 必须大写
                    .withResultCharset(Charset.forName("gbk"))// 指定解析响应html的编码
                    .withKeepReceiveCookie()
                    .withResultHandler(this::afterLogin)
                    .and()
                .withTask("SearchOrDetail")
                    .withLogDetail()
                    .withKeepReceiveCookie()
                    .withBlockingMillis(6000)// 每个任务完成后6s再执行下一个任务（网站限制搜索间隔为5s）
                    .withResultCharset(Charset.forName("gbk"))// 指定解析响应html的编码
                    .withResultHandler(this::afterSearch)
                    .and()
                .withTask("Download")
                    .withBlockingMillis(2000)// 每个任务完成后2s再执行下一个任务
                    .withKeepReceiveCookie()
                    .withResultType(TYPE.File)
                    .withTmpDir(WIN_TMP_DIR_PATH)
                    .withResultCharset(Charset.forName("gbk"))// 指定解析响应html的编码
                    .withResultHandler(this::afterDownload)
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
    }
    
    public static void main(String[] args) throws Exception {
        new WenKu8Bootstrap().run();
    }

}
