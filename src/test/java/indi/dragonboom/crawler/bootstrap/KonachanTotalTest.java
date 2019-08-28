package indi.dragonboom.crawler.bootstrap;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.http.HttpResponse;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import indi.crawler.bootstrap.CrawlerJob;
import indi.crawler.result.ResultHandler;
import indi.crawler.result.ResultHelper;
import indi.crawler.task.ResponseEntity.TYPE;
import indi.crawler.task.Task;
import indi.util.FileUtils;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KonachanTotalTest {
    private static final String DOWNLOAD_PATH = "E:\\byCrawler\\konachan";

    static ResultHandler rh3 = (ctx, helper) -> {
        if (ctx.getResponseEntity().getContent() instanceof String) {
            System.out.println("type error");
            System.exit(-1);
        }
        File tmpFile = (File) ctx.getResponseEntity().getContent();
        String uri = ctx.getUri().toString();
        String fileName = uri.substring(uri.lastIndexOf("/") + 1);
        Path p = Paths.get(DOWNLOAD_PATH, fileName);

        Files.copy(tmpFile.toPath(), p, StandardCopyOption.REPLACE_EXISTING);// 移动临时文件
//        Files.write(p, bytes, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE,
//                StandardOpenOption.WRITE);
    };

    private static boolean errorCheck(Task ctx, Document doc, ResultHelper helper) {
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

    static ResultHandler rh2 = (ctx, helper) -> {
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
        if (es.size() == 0) {
            // 先判断是否为响应错误
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode != 200) {
                // 若响应错误，则不处理
                log.error("响应错误，状态码为 {}", statusCode);
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
            log.error("链接解析错误 url为空 !!! " + ctx.getUri() + "\n" + es);
            System.exit(0);// warn ! ! !
        }
        String fileName = link.substring(link.lastIndexOf("/") + 1);
        Path p = Paths.get(DOWNLOAD_PATH, fileName);
        if (!Files.exists(p, LinkOption.NOFOLLOW_LINKS)) {
            helper.addNewTask("Download", link);
        }
        else {
            log.info("文件已存在");
        }
    };

    static ResultHandler rh1 = (ctx, helper) -> {
        log.info("{}", ctx.getUri());
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
            System.exit(-1);
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

    public static void main(String[] args) {
        Path path = Paths.get(DOWNLOAD_PATH);
        FileUtils.createDirectoryIfNotExist(path);
//        FileUtils.clearDirectory(path);
//        log.info("清空目录");
        
        String redisURI = "redis://!qQ1312449403@172.104.66.71:6380";

        System.setProperty("jdk.http.auth.tunneling.disabledSchemes", "");// 启用HTTPS代理
        CrawlerJob.build()
                .withRedisMQTaskPool(redisURI)
//                .withContextPoolMonitor()
                .withCloseableMonitor()
//                .withHTTPProxy("127.0.0.1", 1080)
                .withTask("Download")
                    .withResultHandler(rh3)
                    .withResultType(TYPE.File)// file 类型的redis存储不好实现。。。
//                    .withPriority(3)
//                    .withLogDetail()
//                    .withRedisCache("redis://@localhost:6379/0")
                    .withKeepReceiveCookie()
                    .and()
                .withTask("PreDownload")
//                    .withPriority(2)
                                    .withResultHandler(rh2)
                    .withRedisCache(redisURI)
                    .withKeepReceiveCookie()
                    .withKeyGenerator((url, request) -> {
                        // get 286704 from http://konachan.com/post/show/286704/ayanami_...
                        Matcher matcher = Pattern.compile("(?<=/)\\d+?(?=/)").matcher(url);
                        matcher.find();
                        return "PreDownload" + matcher.group();
                    })
//                    .withLogDetail()
                    .and()
                .withTask("GetFav")
                    .withResultHandler(rh1)
                    .withLogDetail()
                    .withKeepReceiveCookie()
                    .and()
                .start("GetFav", "https://konachan.com/post?page=1&tags=vote%3A3%3Adargonboom+order%3Avote");
    }
}
