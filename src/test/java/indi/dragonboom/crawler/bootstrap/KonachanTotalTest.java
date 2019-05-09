package indi.dragonboom.crawler.bootstrap;

import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

import org.apache.http.HttpResponse;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import indi.crawler.bootstrap.CrawlerJob;
import indi.crawler.nest.CrawlerContext;
import indi.crawler.nest.ResponseEntity.TYPE;
import indi.crawler.result.ResultHandler;
import indi.crawler.task.SpecificTask;
import indi.util.FileUtils;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KonachanTotalTest {
    private static final String DOWNLOAD_PATH = "E:\\byCrawler\\konachan";

    static ResultHandler rh3 = (ctx) -> {
        byte[] bytes = (byte[]) ctx.getResponseEntity().getContent();
        String uri = ctx.getUri().toString();
        String fileName = uri.substring(uri.lastIndexOf("/") + 1);
        Path p = Paths.get(DOWNLOAD_PATH, fileName);
        Files.write(p, bytes, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE,
                StandardOpenOption.WRITE);
        return null;
    };

    private static boolean errorCheck(CrawlerContext ctx, Document doc, List<SpecificTask> tasks) {
        // 500 error check
        Element title = null;
        if ((title = doc.getElementsByTag("head").get(0)).getElementsByTag("title").html()
                .contains("Error")) {
            if (title.html().contains("Privoxy Error")) {
                System.out.println("!!! 代理网络错误... " + ctx.getUri());
                tasks.add(new SpecificTask(ctx.getTask().getName(), ctx.getUri().toString(), null));
            } else
                System.out.println(title.html());
            return false;

        } else {
            return true;
        }
    }

    static ResultHandler rh2 = ctx -> {
        HttpResponse response = ctx.getResponse();
        List<SpecificTask> tasks = new LinkedList<>();
        String html = (String) ctx.getResponseEntity().getContent();
        Document doc = Jsoup.parse(html);
        if (!errorCheck(ctx, doc, tasks)) {
            return tasks;
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
                log.error("响应错误，状态码为 {}" + statusCode);
                return new LinkedList<>();
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
        if (!Files.exists(p, LinkOption.NOFOLLOW_LINKS))
            tasks.add(new SpecificTask("Download", link, null));
        else
            log.info("文件已存在");
        return tasks;
    };

    static ResultHandler rh1 = ctx -> {
        log.info("{}", ctx.getUri());
        List<SpecificTask> tasks = new LinkedList<>();
        String html = (String) ctx.getResponseEntity().getContent();
        Objects.requireNonNull(html);
        
        Document doc = Jsoup.parse(html);
        String prefix = "http://konachan.com";
        if (!errorCheck(ctx, doc, tasks)) {
            return tasks;
        }
        // 获取下一页
        String postfix = null;
        try {
            postfix = doc.getElementsByClass("next_page").get(0).attr("href");
        } catch (Exception e) {
            log.error(html);
        }
        String nextPage = null;
        if (postfix != null && postfix.length() > 0) {
            nextPage = prefix + doc.getElementsByClass("next_page").get(0).attr("href");
            tasks.add(new SpecificTask("GetFav", nextPage, null));
        }
        // 获取本页图片预览url
        for (Element e : doc.getElementsByClass("thumb")) {
            String url = prefix + e.attr("href");
            tasks.add(new SpecificTask("PreDownload", url, null));
        }
        return tasks;
    };

    public static void main(String[] args) {
        Path path = Paths.get(DOWNLOAD_PATH);
//        FileUtils.clearDirectory(path);
//        log.info("清空目录");
        FileUtils.createDirectoryIfNotExist(path);

        System.setProperty("jdk.http.auth.tunneling.disabledSchemes", "");// 启用HTTPS代理
        CrawlerJob job = new CrawlerJob();
        job.withContextPoolMonitor()
                .withCloseableMonitor()
                .withHTTPProxy("127.0.0.1", 1080)
                .withTask("Download")
                    .withResultHandler(rh3)
                    .withResultType(TYPE.ByteArray)
//                    .withPriority(3)
//                    .withLogDetail()
                    .withRedisCache("redis://@localhost:6379/0")
                    .and()
                .withTask("PreDownload")
//                    .withPriority(2)
                    .withResultHandler(rh2)
                    .withRedisCache("redis://@localhost:6379/0")
//                    .withLogDetail()
                    .and()
                .withTask("GetFav")
                    .withResultHandler(rh1)
//                    .withLogDetail()
                    .and()
                .start("GetFav",
                    "https://konachan.com/post?page=1&tags=vote%3A3%3Adargonboom+order%3Avote", null);
    }
}
