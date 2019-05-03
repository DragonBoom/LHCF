package indi.dragonboom.crawler.bootstrap;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.http.message.BasicHeader;
import org.json.JSONObject;

import com.google.common.collect.Lists;

import indi.crawler.bootstrap.CrawlerJob;
import indi.crawler.result.ResultHandler;
import indi.crawler.task.SpecificTask;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PixivTotalTest {
	private static volatile boolean loginSuccess = false;
	private static final String DOWNLOAD_PATH = "E:\\byCrawler\\pixiv";
	private static String userName = "1312449403@qq.com";
	private static String password = "qq1312449403";
	
	static ResultHandler preLoginRH = ctx -> {
	    // 获取cookies后携带cookies发送登陆请求表单
	    String html = (String) ctx.getResponseEntity().getContent();
	    // get post key
	    Pattern p = Pattern.compile("(?<=post_key\" value=\").*?(?=\")");
	    Matcher m = p.matcher(html);
	    m.find();
	    String postKey = m.group();
	    StringBuilder loginFrom = new StringBuilder();
	    loginFrom.append("pixiv_id=").append(userName).append("&");
	    loginFrom.append("captcha=").append("&");
	    loginFrom.append("g_recaptcha_response=").append("&");
	    loginFrom.append("password=").append(password).append("&");
	    loginFrom.append("post_key=").append(postKey);
	    loginFrom.append("&").append("source=").append("pc").append("&");
	    loginFrom.append("ref=").append("wwwtop_accounts_index").append("&");
	    loginFrom.append("return_to=").append("https://www.pixiv.net/");
	    SpecificTask st = new SpecificTask("Login",
	            "https://accounts.pixiv.net/api/login?lang=zh", loginFrom.toString());
	    LinkedList<SpecificTask> child = new LinkedList<>();
	    child.add(st);
	    return child;
	};
	
	static ResultHandler loginRH = ctx -> {
        LinkedList<SpecificTask> tasks = new LinkedList<>();
        System.out.println(ctx.getResponseEntity().getContent());
        tasks.add(new SpecificTask("HomePage", "https://www.pixiv.net/", null));
        return tasks;
    };
	
	static ResultHandler homePageRH = ctx -> {
        loginSuccess = true;
        log.info("登陆成功");
        // start scan fav...
        String fav = "https://www.pixiv.net/ajax/user/10127376/illusts/bookmarks?tag=&offset=0&limit=48&rest=show";
        
        return Lists.newArrayList(new SpecificTask("InitFavTask", fav, null));
    };
    
    static ResultHandler initFav = ctx -> {
        String jsonStr = (String) ctx.getResponseEntity().getContent();
        System.out.println(jsonStr);
        JSONObject jsonObject = new JSONObject(jsonStr);
        int total = jsonObject.getJSONObject("body").getInt("total");
        log.info("一共有 {} 项", total);
        return null;
    };

	/**
	 * 若存在返回null，否则返回解析出来的文件名
	 * 
	 * @param uri
	 * @return
	 */
	private static String checkExist(String uri) {
		String picName = uri.substring(uri.lastIndexOf("/") + 1);
		Path supposePath = Paths.get(DOWNLOAD_PATH, picName);
		return Files.exists(supposePath) == true ? null : picName;
	}
	
	static ResultHandler favRH = ctx -> {
	    
	    return null;
	};


	public static void main(String[] args) {
		new CrawlerJob().withHTTPProxy("127.0.0.1", 1080)
		        .withCloseableMonitor()
		        .withContextPoolMonitor()
		        .withTask("PreLogin")
		                .withResultHandler(preLoginRH)
		                .withLogDetail()
		                .withKeepReceiveCookie()
	                    .and()
                .withTask("Login")
	                    .withResultHandler(loginRH)
	                    .withMethod("POST")
	                    // httpclient会自动添加content-length字段
	                    .withRequestHeaders(
	                            new BasicHeader("Content-Type", "application/x-www-form-urlencoded"))
	                    .withKeepReceiveCookie()
	                    .and()
		        .withTask("HomePage")
		                .withResultHandler(homePageRH)
		                .withKeepReceiveCookie()
		                .and()
                .withTask("InitFavTask")
                        .withResultHandler(initFav)
                        .withLogDetail()
//                        .withKeepReceiveCookie()
                        .and()
		        .start("PreLogin",
                    "https://accounts.pixiv.net/login?lang=zh&source=pc&view_type=page&ref=wwwtop_accounts_index",
                    null);
		
		while (!loginSuccess) {
			try {
				Thread.sleep(700);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		// 等待第一步完成
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
	}
}
