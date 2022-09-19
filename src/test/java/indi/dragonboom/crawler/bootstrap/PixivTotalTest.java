package indi.dragonboom.crawler.bootstrap;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.text.StringEscapeUtils;
import org.apache.http.message.BasicHeader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.json.JSONObject;

import com.fasterxml.jackson.annotation.JsonFormat;

import indi.bean.ObjectMapperUtils;
import indi.crawler.bootstrap.CrawlerJob;
import indi.crawler.cookies.CookieStore;
import indi.crawler.result.ResultHandler;
import indi.crawler.task.ResponseEntity;
import indi.crawler.task.ResponseEntity.TYPE;
import indi.crawler.util.JobUtils;
import indi.exception.WrapperException;
import indi.io.ClassPathProperties;
import indi.io.FileUtils;
import indi.util.ProcessUtils;
import indi.util.StringUtils;
import indi.util.SystemUtils;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * error: 请进行reCAPTCHA验证。似乎是反爬虫程序升级了。。。现改为直接用浏览器的cookie进行登陆
 * 
 * <p>后续可改用Selenium进行模拟登陆？但有机器人验证问题
 * 
 * <p>用Selenium测试可以登陆，但登陆测试过多就需要验证（识图验证）；发现Pixiv的cookie有效期很久，为了省事，
 * 后续还是继续直接从浏览器复制Cookie吧
 * 
 * <p>（旧版）若作品在Pixiv上失效，会导致爬虫结束后将本地图片也一并删除；由于作品被删，无法获取到具体文件编号，无法准确匹配到
 * 本地文件，导致无法按旧流程规避删除，需要增加逻辑。
 * <p>2021.03.07 补充：目前，会先遍历收藏夹，再逐个尝试下载；在遍历收藏夹时，
 * 若对应的本地文件存在，<b>不管是否失效</b>，都会标记为已下载然后跳过下载，结束时也不会删除这些文件；因此，<b>目前已实现
 * 了避免删除失效作品这一功能</b>
 * 
 * <p>已失效作品在收藏夹api中只能获取到id
 * 
 * <p>2021.02.04 当文件下载成功后，将作者名写入到comment标签，使得用xnview时可以按作者名排序
 * 
 * <p>2021.03.10 考虑将作品的创建时间/修改时间，修改为收藏时间，便于排序管理。
 * 
 * <p>2021.04.04 在所用的API中没找到收藏时间，但有收藏id，将该id加到文件名中以按收藏时间排序（原本以作品id为文件名，现在看来作用不大）
 * 
 * <p>可考虑用之前写的文件持久化框架，持久化作品信息，进一步提高效率。
 * 
 * <p>2021.04.25 出现bug：错误的删除了线上存在的文件。
 * 直接原因是，线上记录集合中的ID少于实际收藏夹的ID。一开始发现是出现了不同收藏夹API返回部份相同ID的情况，
 * 后面发现，相同收藏夹API会返回不同的结果。。。
 * <p>即使是用浏览器上网站，随便手动刷新同一个收藏夹页面，也时不时会出现不同内容，可能是PIXIV官方的bug
 * <p>目前停用“删除已取消收藏文件”的功能
 * 
 * @author wzh
 * @since 2020.03.13
 */
@Slf4j
public class PixivTotalTest {
	private static final String DOWNLOAD_PATH_STR = "F:\\byCrawler\\pixiv";
	private static final Path DOWNLOAD_PATH = Paths.get(DOWNLOAD_PATH_STR);
	private static final String ID_SEPERATOR = "_";
	
	/** 临时文件夹对象 */
    @Getter
    private static Path tmpDirPath = Paths.get("F:\\tmp\\crawler");
	private static String userName = ClassPathProperties.getProperty("/account.properties", "pixiv-username");
	private static String password = ClassPathProperties.getProperty("/account.properties", "pixiv-password");
	// 从浏览器获得的cookie的值
	private static String PHPSESSID = "10127376_ekAaaiGQq4YPkKwLBFQCGfNr7lk4WcKB";
	
    /** 已下载的图片文件名集合（缓存）（不含后缀）（不保证实时更新） */
    @Getter
    private static final Set<String> DOWNLOADED_FILENAME = new HashSet<>();
    /** 收藏夹中存在的图片id（实时更新） */
    @Getter
    private static final Set<String> ONLINE_ILLUST_IDS = new HashSet<>();
    
    /** 是否强制更新元数据，若为true，即使图片已下载，也将更新一次元数据 */
    private static boolean FORCE_UPDATE_METADATA = false;
    /** 是否输出工具的详细日志 */
    private static boolean LOG_DETAIL = true;
	
	static ResultHandler preLoginRH = (ctx, helper) -> {
	    // 获取cookies后携带cookies发送登陆请求表单
	    String html = (String) ctx.getResponseEntity().getContent();
	    // get post key
	    Pattern p = Pattern.compile("(?<=post_key\" value=\").*?(?=\")");
	    Matcher m = p.matcher(html);
	    m.find();
	    String postKey = m.group();
	    // 构建请求表单（application/x-www-form-urlencoded）
	    // 形如：captcha=&g_recaptcha_response=&password=qq1312449403&pixiv_id=1312449403%40qq.com&post_key=d19cb72731ff195c24d773d464693ffd&source=pc&ref=&return_to=https%3A%2F%2Fwww.pixiv.net%2F&recaptcha_v3_token=03AHaCkAZyjRkVGCrH0JowlVVX7sEzoYeFBQmMHJ58l4YtHOwIiuUFRA9fyzMgkHLFww61UJDIiqwrTFeBhNH_lMv81yZtSym59L6VL25rBZFjCq30vxs9HGIvkkXZIBCSbRg5tcwK_Jaj64gTxVb0pJQoijY5HpBoLfTMiWQmmCMEscKVj5E6RWqCDh86B1v0GaRRngN4j5Niv59RKgh6ytnKRtLGuQJVSCQfcNBGPuUnr1BMi8nbG2dcO1tO-rOGT38ssMZc1Dn4Gu6603vP9UJJocSO9QoMDlLkegoqUSOwdaKiZaE_8Cnkba-5mT7UXP6SFvcdrIWmN1o0LEo_hkmKEMvWse6ufOexLsn82RLAlabvU2gYcwj3A8fRG4V6mKwvafoXnrvNgMle_zdpuqtbPsWo47yrwA
	    // 2020.04.07 发现近期新增了recaptcha_v3_token字段
	    // 似乎需要额外请求验证码（人机校验）：sitekey=6LfJ0Z0UAAAAANqP-8mvUln2z6mHJwuv5YGtC8xp
	    StringBuilder loginFrom = new StringBuilder();
	    loginFrom.append("pixiv_id=").append(userName).append("&");
	    loginFrom.append("captcha=").append("&");
	    loginFrom.append("g_recaptcha_response=").append("&");
	    loginFrom.append("password=").append(password).append("&");
	    loginFrom.append("post_key=").append(postKey);
	    loginFrom.append("&").append("source=").append("pc").append("&");
	    loginFrom.append("ref=").append("wwwtop_accounts_index").append("&");
	    loginFrom.append("return_to=").append("https://www.pixiv.net/");
	    
	    // 新增任务，调用登陆api
	    helper.addNewTask("Login", "https://accounts.pixiv.net/api/login?lang=zh", loginFrom.toString());
	};
	
	static ResultHandler loginRH = (ctx, helper) -> {
        System.out.println(ctx.getResponseEntity().getContent());
        helper.addNewTask("HomePage", "https://www.pixiv.net/");
    };
	
	static ResultHandler homePageRH = (ctx, helper) -> {
//        loginSuccess = true;
	    log.info("{}", ctx.getResponseEntity().getContent());
        log.info("登陆成功");

        // 访问收藏夹API
        // FIXME: 这里将用户ID写死了，有待完善
        String firstFavApi = "https://www.pixiv.net/ajax/user/10127376/illusts/bookmarks?tag=&offset=0&limit=48&rest=show";
        helper.addNewTask("PreFav", firstFavApi);
    };
    
    private static final Integer FAV_COUNT = 48;
    
    /** 处理收藏夹API，获取收藏图片数量，从而一次性构建所有收藏夹列表页的爬虫任务 */
    static ResultHandler preFavRH = (ctx, helper) -> {
        ResponseEntity responseEntity = ctx.getResponseEntity();
        String jsonStr = (String) responseEntity.getContent();
        JSONObject json = new JSONObject(jsonStr);
        if (json.getBoolean("error") == false) {
            JSONObject bodyJson = json.getJSONObject("body");
            int totalCounts = bodyJson.getInt("total");
            int counts = FAV_COUNT;
            int times = totalCounts % counts == 0 ? totalCounts / counts : totalCounts / counts + 1;
            for(int i = 0; i < times; i++) {
                int offset = i * counts;
                // FIXME: 这里将用户ID写死了，有待完善
                helper.addNewTask("Fav", "https://www.pixiv.net/ajax/user/10127376/illusts/bookmarks?tag=&offset="
                        + offset + "&limit=" + counts + "&rest=show");
            }
        } else {
            log.error("访问收藏夹api异常，可能是权限不足（登录失败或PHPSESSID失效）:" + jsonStr);
            System.exit(-1);
            throw new RuntimeException("访问收藏夹api异常:" + jsonStr);
        }
        
    };
    
    /** 处理收藏夹列表API，构建收藏夹详情API的爬虫任务
     * 
     * <p>所用API一般会在访问用户主页（如www.pixiv.net/users/{id}/bookmarks/artworks）的收藏标签时调用
     * （另外还有一个管理收藏的页面（www.pixiv.net/bookmark.php），这个是直接将数据写在php里的）
     * 
     */
    static ResultHandler favRH = (ctx, helper) -> {
        ResponseEntity responseEntity = ctx.getResponseEntity();
        String jsonStr = (String) responseEntity.getContent();
        if (StringUtils.isEmpty(jsonStr)) {
            throw new IllegalArgumentException("响应返回json为空，url：" + ctx.getUri());
        }
        BookmarksRsp rsp = ObjectMapperUtils.getMapper().readValue(jsonStr, BookmarksRsp.class);
        if (!rsp.error) {
            List<PixivWork> works = rsp.getBody().getWorks();
            for (PixivWork work : works) {
                // 遍历已下载记录，判断图片是否已存在
                // 2020.12.27 发现部份id格式是数字，由getString改为optString
                String illustId = work.getId();// 2020.11.02 由illustId改为id
                // 添加id记录，该记录用于避免误删（即使作品失效也会添加记录）
                if (ONLINE_ILLUST_IDS.contains(illustId)) {
                    System.out.println("重复ID：" + illustId + " " + ctx.getUri());
                }
                synchronized (ONLINE_ILLUST_IDS) {
                    ONLINE_ILLUST_IDS.add(illustId);
                }
                int pageCount = work.getPageCount();
                Set<String> downloadedPics = DOWNLOADED_FILENAME.stream()
                        .filter(filename -> isIDMatchFileName(illustId, filename))
                        .collect(Collectors.toSet());
                long fileCount = downloadedPics.size();
                if (fileCount != pageCount || FORCE_UPDATE_METADATA) {
                    helper.addNewTask("PreDownload", "https://www.pixiv.net/ajax/illust/" + illustId, null, work);
                } else {
                    // 该作品的线上文件数等于已下载文件数，即不需要下载
                    if (LOG_DETAIL) {
                        log.info("收藏夹列表页API：预测作品的文件均已存在({}/{})，将跳过：{}", fileCount, pageCount, downloadedPics);
                    }
                }
            }
        } else {
            throw new RuntimeException("访问收藏夹api异常，无法按预定格式解析响应:" + jsonStr);
        }
    };
    
    static ResultHandler preDownloadRH = (ctx, helper) -> {
        PixivWork work = (PixivWork) ctx.getArg();
        if (work == null) {
            throw new WrapperException("预下载时无法获取到收藏夹API的响应");
        }
        ResponseEntity responseEntity = ctx.getResponseEntity();
        // 接口返回数据对中文使用了unicode编码，但必须先解析json再转义，否则可能会打破json的结构
        String jsonStr = (String) responseEntity.getContent();
        JSONObject json;
        try {
            json = new JSONObject(jsonStr);
        } catch (RuntimeException e) {
            log.error("无法将响应信息解析为json：{}", jsonStr);
            throw e;
        }
        String msg = json.getString("message");
        if (StringUtils.notEmpty(msg)) {
            msg = StringEscapeUtils.unescapeJava(msg);// 转义unicode编码的特殊字符（如中文）
            json.put("message", msg);
        }
        if (json.getBoolean("error") == false) {
            JSONObject bodyJson = json.getJSONObject("body");
            int pageCount = bodyJson.getInt("pageCount");// 该作品的图片数
            JSONObject urlJson = bodyJson.getJSONObject("urls");
            String firstUrl = urlJson.getString("original");// 第一个图片的原图地址，一般后缀是p0，若有其他图片则是p1, p2...
            for (int i = 0; i < pageCount; i++) {
                String url = firstUrl.replaceFirst("(?<=_p)[0,10]+", Integer.toString(i));
                // 文件已存在则跳过，否则新增下载任务:
                Path filePath = getFilePath(url, work.getBookmarkData().getId());
                if (!Files.exists(filePath)) {
                    // 添加下载任务，并传递作品对象
                    helper.addNewTask("Download" , url, null, work);
                } else {
                    // 若已存在，则只更新元数据
                    updateMetadata(filePath, work);
                }
            }
        } else {
            // 尝试处理异常状况（若文件已存在，不会尝试下载；因此执行到这里的作品，都未下载）
            if (json != null && msg.equals("该作品已被删除，或作品ID不存在。")) {
                if (LOG_DETAIL) {
                    log.info("作品已失效：{}", ctx.getUri());
                }
            } else {
                throw new RuntimeException("访问插图api (" + ctx.getUri() + ")异常:" + jsonStr);
            }
        }
    };

    /**
     * 比较id与文件名是否匹配
     * 
     * @param id
     * @param filename {favId}_{illustId}_{type}{n} 如：123_123_p0 或 123_123_ugoira0
     * @return
     * @since 2021.04.25
     */
    static boolean isIDMatchFileName(String id, String filename) {
        return filename.matches("\\d+_" + id + "_[a-z]+\\d+");
    }

    /**
     * 根据url与收藏id，获取图片的路径
     * 
     * @author DragonBoom
     * @param url 以文件名结尾的url
     * @param 收藏id
     * @return
     */
    private static Path getFilePath(String url, String bookmarkId) {
        return Paths.get(DOWNLOAD_PATH_STR, bookmarkId + ID_SEPERATOR + url.substring(url.lastIndexOf("/") + 1));
    }
    
    static ResultHandler downloadRH = (ctx, helper) -> {
        ResponseEntity responseEntity = ctx.getResponseEntity();
        PixivWork work = (PixivWork) ctx.getArg();
//        if (!responseEntity.getType().equals(ResponseEntity.TYPE.String)) {
//            throw new IllegalArgumentException("下载错误，说下载内容的响应类型是字符串");
//        }
        
        File tmpFile = (File) responseEntity.getContent();
        String url = ctx.getUri().toString();
        Path path = getFilePath(url, work.getBookmarkData().getId());
        if (LOG_DETAIL) {
            log.info("下载成功 {}", path);
        }
        Files.move(tmpFile.toPath(), path, StandardCopyOption.REPLACE_EXISTING);// 注意，若文件已存在将覆盖
        
        updateMetadata(path, work);
        
        String filename = FileUtils.getFileName(path);
    };
    
    /** 通过元数据，补充作品信息（如作者名），使得可以以此进行管理
     * 
     * <p>由于用Java执行windows命令，若直接执行完整命令，会将命令按系统默认的GBK去编码，会导致用UTF-8查看元数据时出现乱码；
     * 因此，现采用文本参数的形式执行命令，能达成目标但开销较大。
     * 
     * @param path 图片路径
     * @since 2021.02.04
     */
    private static void updateMetadata(Path path, PixivWork work) throws IOException {
        if (LOG_DETAIL) {
            log.info("开始更新标签 {}", path);
        }
        
        String author = work.getUserName();
        if (StringUtils.isEmpty(author)) {
            throw new WrapperException("无法从收藏夹API中获取到作品作者名称");
        }
        // 在图片的同级路径下，创建用于读取utf8编码指令的临时文件
        Path tmp = FileUtils.createTmpFile(path.getParent()).toPath();
        if (LOG_DETAIL) {
            log.debug("生成传递命令的临时文件 {}", tmp);
        }
        String utf8Command = "-comment=" + author;
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
        if (LOG_DETAIL) {
            log.info("更新标签成功 {}", path);
        }
    }
    
    /** 从文件名中获取id，如从123_0中获取123 */
    private static String getIdByFilename(String filename) {
        Matcher matcher = Pattern.compile("^\\d+").matcher(filename);// 如49236540_p1.png中的49236540
        if (matcher.find()) {
            return matcher.group();
        }
        return null;
    }

	public static void main(String[] args) throws Exception {
	    SystemUtils.correctRunnableJarLog4j2();
	    
        FORCE_UPDATE_METADATA = false;
        LOG_DETAIL = false;
	    // 初始化已下载的缓存
	    JobUtils.refreshLocalFilenameCaches(DOWNLOAD_PATH, DOWNLOADED_FILENAME, FileUtils::isImage);
	    
//	    System.exit(-1);
	    
	    CrawlerJob job = CrawlerJob.build()
	            .withTmpFolder(tmpDirPath.toString())// 指定整个爬虫业务的临时文件夹，可被单个爬虫任务定义的配置所覆盖
	            .withHTTPProxy("127.0.0.1", 10809)
		        .withCloseCallback(() -> {// 定义爬虫执行完毕时的回调
		            log.info("开始清理已取消收藏的文件");
		            System.out.println(ONLINE_ILLUST_IDS.size());
	                JobUtils.delLocalFileByFilenameWhiteList(ONLINE_ILLUST_IDS, DOWNLOADED_FILENAME, DOWNLOAD_PATH, 
	                        (s1, s2) -> isIDMatchFileName(s1, s2) ? 0 : -1, "删除线上已移除文件：{}");
		            log.info("爬虫任务执行结束");
		            
//		            // ---删除线上有记录文件，但保留线上失效文件（仅用于清理时避免删除收藏夹中的失效文件）
//		            log.info("开始清理线上未失效的文件");
//		            try (Stream<Path> stream = Files.list(DOWNLOAD_PATH)) {
//		                stream.forEach(p -> {
//		                    if (!FileUtils.isImage(p)) {
//		                        return;
//		                    }
//		                    // 从文件名中获取编码
//		                    String filename = p.getFileName().toString();
//		                    String id = getIdByFilename(filename);
//		                    if (!ONLINE_INVALID_CODES.contains(id)) {
//		                        log.info("删除文件：{}", p);
//		                        try {
//		                            Files.delete(p);
//		                        } catch (IOException e) {
//		                            throw new WrapperException(e);
//		                        }
//		                    } else {
//		                        log.info("保留线上已失效文件：{}", p);
//		                    }
//		                });
//		            } catch (Exception e) {
//		                throw new WrapperException(e);
//		            }
//		            log.info("清理线上未失效的文件完成");
		        })
//		        .withContextPoolMonitor()
		        // 预登陆，获取登陆表单
		        .withTask("PreLogin")
		                .withResultHandler(preLoginRH)
//		                .withLogDetail()
		                .withKeepReceiveCookie()
	                    .and()
                // 登陆
                .withTask("Login")
	                    .withResultHandler(loginRH)
	                    .withMethod("POST")
	                    // httpclient会自动添加content-length字段
	                    .withRequestHeaders(
	                            new BasicHeader("Content-Type", "application/x-www-form-urlencoded"))
//	                    .withLogDetail()
	                    .withKeepReceiveCookie()
	                    .and()
                // 访问主页
		        .withTask("HomePage")
		                .withResultHandler(homePageRH)
//		                .withLogDetail()
		                .withKeepReceiveCookie()
		                .and()
                .withTask("PreFav")
                        .withResultHandler(preFavRH)
                        .withKeepReceiveCookie()
//                        .withLogDetail()
                        .and()
                .withTask("Fav")
                        .withResultHandler(favRH)
                        .withLogDetail()
                        .withKeepReceiveCookie()
                        .and()
                // 收藏夹详情页
                .withTask("PreDownload")
                        .withResultHandler(preDownloadRH)
                        .withBlockingMillis(1000)// 阻塞地执行，1s 1次请求，避免被ban
//                        .withLogDetail()
                        .withKeepReceiveCookie()
                        .withPriority(8)
                        .and()
                .withTask("Download")
//                        .withResultType(TYPE.ByteArray)
                        .withBlockingMillis(2000)// 2s 1个，避免被ban
                        .withResultType(TYPE.TMP_FILE)// 2020.04.07 使用文件类型
                        .withResultHandler(downloadRH)
                        .withKeepReceiveCookie()
                        .withRequestHeaders(new BasicHeader("Referer", "https://www.pixiv.net"))
//                        .withRedisCache("redis://@localhost:6379/0")// 目前已弃用
                        .withPriority(9)// 设置高优先级
                        .and();
	    
	    // 直接从浏览器获取登陆后的cookie
	    // 模拟登陆 模块在Pixiv更新后有问题
	    // 手动新增cookie（已测试仅需以下1个cookie）
	    CookieStore cookieStore = job.getCookieStore();
	    cookieStore.add("PHPSESSID=" + PHPSESSID +"; path=/; domain=.pixiv.net; secure; HttpOnly", 
	            URI.create("https://accounts.pixiv.net/api/login?lang=zh"));
//	    
//	    cookieStore.add("device_token=069de615bab965caf36a5c99258c7a15; expires=Thu, 07-May-2020 06:07:27 GMT; Max-Age=2592000; path=/; domain=.pixiv.net; secure; HttpOnly", 
//	            URI.create("https://accounts.pixiv.net/api/login?lang=zh"));
//	    
//	    cookieStore.add("privacy_policy_agreement=1; expires=Thu, 07-Apr-2022 06:07:27 GMT; Max-Age=63072000; path=/; domain=.pixiv.net; secure; HttpOnly", 
//	            URI.create("https://accounts.pixiv.net/api/login?lang=zh"));
	    
	    // 直接从获取收藏夹的步骤开始
	    job.start("PreFav", "https://www.pixiv.net/ajax/user/10127376/illusts/bookmarks?tag=&offset=0&limit=48&rest=show", null);
	    
//        // 第一步，预登陆
//	    job.start("PreLogin",
//                    "https://accounts.pixiv.net/login?lang=zh&source=pc&view_type=page&ref=wwwtop_accounts_index",
//                    null);
	}
	
	/**
	 * https://www.pixiv.net/ajax/user/{userid}/illusts/bookmarks 返回的数据结构
	 * 
	 * @author wzh
	 * @since 2021.04.04
	 */
	
	@Data
	private static class BookmarksRsp {
	    private BookmarksRspBody body;
	    private boolean error;
	    private String message;
	}
	@Data
	private static class BookmarksRspBody {
	    private List<PixivWork> works; 
	}
	@Data
	private static class PixivWork implements Serializable {
        private static final long serialVersionUID = 1L;
        
        private String id;
	    private PixivWookBookmarkData bookmarkData;
	    private Integer pageCount;
	    private String url;
	    private String userid;
	    private String userName;
	    // 作品创建日期，如：2020-09-16T01:03:03+09:00
	    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ssX")
	    private Date createDate;
	}
	@Data
	private static class PixivWookBookmarkData {
	    // 收藏夹id，可以此排序
	    private String id;
	    // 还有一个属性：private
	}
}
