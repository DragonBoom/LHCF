package indi.dragonboom.crawler.bootstrap;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.http.message.BasicHeader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.json.JSONArray;
import org.json.JSONObject;

import indi.crawler.bootstrap.CrawlerJob;
import indi.crawler.cookies.CookieStore;
import indi.crawler.result.ResultHandler;
import indi.crawler.task.ResponseEntity;
import indi.crawler.task.ResponseEntity.TYPE;
import indi.crawler.util.JobUtils;
import indi.io.ClassPathProperties;
import indi.io.FileUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * error: 请进行reCAPTCHA验证。似乎是反爬虫程序升级了。。。现改为直接用浏览器的cookie进行登陆
 * 
 * <p>后续可改用Selenium进行模拟登陆
 * 
 * <p>用Selenium测试可以登陆，但登陆测试过多就需要验证（识图验证）；发现Pixiv的cookie有效期很久，为了省事，
 * 后续还是继续直接从浏览器复制Cookie吧
 * 
 * @author wzh
 * @since 2020.03.13
 */
@Slf4j
public class PixivTotalTest {
	private static volatile boolean loginSuccess = false;
	private static final String DOWNLOAD_PATH_STR = "F:\\byCrawler\\pixiv";
	private static final Path DOWNLOAD_PATH = Paths.get(DOWNLOAD_PATH_STR);
	
	/** 临时文件夹对象 */
    @Getter
    private static Path tmpDirPath = Paths.get("F:\\tmp\\crawler");
	private static String userName = ClassPathProperties.getProperty("/account.properties", "pixiv-username");
	private static String password = ClassPathProperties.getProperty("/account.properties", "pixiv-password");
	// 从浏览器获得的cookie的值
	private static String PHPSESSID = "10127376_Us9Dx1ZXwDisqanTc9jLgBefXdcaHovF";
	
	/** 本地图片序号集合（缓存） */
    @Getter
    private static final Set<String> LOCAL_CODES = new HashSet<>();
    /** 已下载的图片序号集合（缓存） */
    @Getter
    private static final Set<String> DOWNLOADED_CODES = new HashSet<>();
    /** 线上存在的图片序号集合，从收藏夹获取图片时会向其中新增元素 */
    @Getter
    private static final Set<String> ONLINE_CODES = new HashSet<>();
	
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
        System.exit(-1);

        // 访问收藏夹API
        String firstFavApi = "https://www.pixiv.net/ajax/user/10127376/illusts/bookmarks?tag=&offset=0&limit=48&rest=show";
        helper.addNewTask("PreFav", firstFavApi);
    };
    
    /** 处理收藏夹API，根据收藏图片数量，构建收藏夹列表页的爬虫任务 */
    static ResultHandler preFavRH = (ctx, helper) -> {
        ResponseEntity responseEntity = ctx.getResponseEntity();
        String jsonStr = (String) responseEntity.getContent();
        JSONObject json = new JSONObject(jsonStr);
        if (json.getBoolean("error") == false) {
            JSONObject bodyJson = json.getJSONObject("body");
            int totalCounts = bodyJson.getInt("total");
            int counts = 40;
            int times = totalCounts % counts == 0 ? totalCounts / counts : totalCounts / counts + 1;
            for(int i = 0; i < times; i++) {
                int offset = i * counts;
                helper.addNewTask("Fav", "https://www.pixiv.net/ajax/user/10127376/illusts/bookmarks?tag=&offset="
                        + offset + "&limit=" + counts + "&rest=show");
            }
        } else {
            log.error("访问收藏夹api异常，可能是权限不足:" + jsonStr);
            System.exit(-1);
            throw new RuntimeException("访问收藏夹api异常:" + jsonStr);
        }
        
    };
    
    /** 处理收藏夹列表API，构建收藏夹详情API的爬虫任务 */
    static ResultHandler favRH = (ctx, helper) -> {
        ResponseEntity responseEntity = ctx.getResponseEntity();
        String jsonStr = (String) responseEntity.getContent();
        JSONObject json = new JSONObject(jsonStr);
        if (json.getBoolean("error") == false) {
            JSONArray worksArray = json.getJSONObject("body").getJSONArray("works");
            for (int i = 0; i < worksArray.length(); i++) {
                JSONObject illustJson = worksArray.getJSONObject(i);
                // 遍历已下载记录，判断图片是否已存在
                String illustId = illustJson.getString("illustId");
                int pageCount = illustJson.getInt("pageCount");
                Set<String> downloadedPics = 
                        DOWNLOADED_CODES.stream().filter(code -> code.startsWith(illustId)).collect(Collectors.toSet());
                long fileCount = downloadedPics.size();
                if (fileCount != pageCount) {
                    helper.addNewTask("PreDownload", "https://www.pixiv.net/ajax/illust/" + illustId);
                } else {
                    // 添加至线上记录，使得结束爬虫任务时不会被误判为线上已移除
                    ONLINE_CODES.addAll(downloadedPics);
                    log.info("收藏夹列表页API：文件已存在，将跳过：{}", downloadedPics);
                }
            }
        } else {
            throw new RuntimeException("访问收藏夹api异常:" + jsonStr);
        }
    };

    static ResultHandler preDownloadRH = (ctx, helper) -> {
        ResponseEntity responseEntity = ctx.getResponseEntity();
        String jsonStr = (String) responseEntity.getContent();
        JSONObject json = new JSONObject(jsonStr);
        if (json.getBoolean("error") == false) {
            JSONObject bodyJson = json.getJSONObject("body");
            int pageCount = bodyJson.getInt("pageCount");
            JSONObject urlJson = bodyJson.getJSONObject("urls");
            String firstUrl = urlJson.getString("original");
            for (int i = 0; i < pageCount; i++) {
                String url = firstUrl.replaceFirst("(?<=_p)[0,10]+", Integer.toString(i));
                // 文件已存在则跳过，否则新增下载任务:
                Path filePath = getFilePath(url);
                // 将该图片添加至线上记录
                ONLINE_CODES.add(FileUtils.getFileName(filePath));
                // 添加下载任务
                helper.addNewTask("Download" , url);
            }
        } else {
            throw new RuntimeException("访问插图api异常:" + jsonStr);
        }
    };
    
    /**
     * 根据url，获取图片的路径
     * 
     * @author DragonBoom
     * @param url
     * @return
     */
    private static Path getFilePath(String url) {
        String fileName = url.substring(url.lastIndexOf("/") + 1);// TODO to utils
        return Paths.get(DOWNLOAD_PATH_STR, fileName);
    }
    
    static ResultHandler downloadRH = (ctx, helper) -> {
        ResponseEntity responseEntity = ctx.getResponseEntity();
//        if (!responseEntity.getType().equals(ResponseEntity.TYPE.String)) {
//            throw new IllegalArgumentException("下载错误，说下载内容的响应类型是字符串");
//        }
        File tmpFile = (File) responseEntity.getContent();
        String url = ctx.getUri().toString();
        Path path = getFilePath(url);
        log.info("下载成功 {}", path);
        Files.move(tmpFile.toPath(), path, StandardCopyOption.REPLACE_EXISTING);// 注意，若文件已存在将覆盖
    };
    
    /**
     * 借助selenium实现登陆
     * 
     * @author DragonBoom
     * @since 2020.09.09
     */
    private void loginBySelenium() {
        
    }

	public static void main(String[] args) throws Exception {
	    // 由于用Eclipse直接打包为Runnable jar时日志的配置文件的路径不是/src/main/resources而是/resources，故用以下代码进行兼容
        URL log4j2Conf = KonachanTotalTest.class.getClassLoader().getResource("resources/log4j2.xml");
        
        if (log4j2Conf != null) {
            LoggerContext logContext = (LoggerContext) LogManager.getContext(false);
            logContext.setConfigLocation(log4j2Conf.toURI());
            logContext.reconfigure();
            log.info("使用可执行文件内的日志配置文件，请忽视找不到配置文件的报错");
        }
	    
	    
	    // 初始化已下载的缓存
	    JobUtils.refreshLocalFileCaches(DOWNLOAD_PATH, DOWNLOADED_CODES);
	    
	    System.out.println(DOWNLOADED_CODES);
//	    System.exit(-1);
	    
	    CrawlerJob job = CrawlerJob.build()
	            .withTmpFolder(tmpDirPath.toString())// 指定整个爬虫业务的临时文件夹，可被单个爬虫任务定义的配置所覆盖
	            .withHTTPProxy("127.0.0.1", 10809)
		        .withCloseableMonitor()
		        .withCloseCallback(() -> {// 定义爬虫执行完毕时的回调
		            try {
		                // 删除线上没有的记录
                        JobUtils.delNoRecordFiles(ONLINE_CODES, DOWNLOADED_CODES, DOWNLOAD_PATH);
                    } catch (IOException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
		        })
//		        .withContextPoolMonitor()
		        // 预登陆，获取登陆表单
		        .withTask("PreLogin")
		                .withResultHandler(preLoginRH)
		                .withLogDetail()
		                .withKeepReceiveCookie()
	                    .and()
                // 登陆
                .withTask("Login")
	                    .withResultHandler(loginRH)
	                    .withMethod("POST")
	                    // httpclient会自动添加content-length字段
	                    .withRequestHeaders(
	                            new BasicHeader("Content-Type", "application/x-www-form-urlencoded"))
	                    .withLogDetail()
	                    .withKeepReceiveCookie()
	                    .and()
                // 访问主页
		        .withTask("HomePage")
		                .withResultHandler(homePageRH)
		                .withLogDetail()
		                .withKeepReceiveCookie()
		                .and()
                .withTask("PreFav")
                        .withResultHandler(preFavRH)
                        // 先记录cookie
                        .withKeepReceiveCookie()
                        // 再打印详情
                        .withLogDetail()
                        .and()
                .withTask("Fav")
                        .withResultHandler(favRH)
//                        .withLogDetail()
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
                        .withResultType(TYPE.File)// 2020.04.07 使用文件类型
                        .withResultHandler(downloadRH)
                        .withKeepReceiveCookie()
                        .withRequestHeaders(new BasicHeader("Referer", "https://www.pixiv.net"))
//                        .withRedisCache("redis://@localhost:6379/0")// FIXME:目前已弃用
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
}
