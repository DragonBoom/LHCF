package indi.dragonboom.crawler.bootstrap;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.LinkedList;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.http.message.BasicHeader;
import org.json.JSONArray;
import org.json.JSONObject;

import indi.crawler.bootstrap.CrawlerJob;
import indi.crawler.nest.ResponseEntity;
import indi.crawler.nest.ResponseEntity.TYPE;
import indi.crawler.result.ResultHandler;
import indi.crawler.task.SpecificTask;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PixivTotalTest {
	private static volatile boolean loginSuccess = false;
	private static final String DOWNLOAD_PATH = "E:\\byCrawler\\pixiv";
	private static String userName = "";
	private static String password = "";
	
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

        // 访问收藏夹API
        String firstFavApi = "https://www.pixiv.net/ajax/user/10127376/illusts/bookmarks?tag=&offset=0&limit=48&rest=show";
        LinkedList<SpecificTask> tasks = new LinkedList<>();
        tasks.add(new SpecificTask("PreFav", firstFavApi, null));
        return tasks;
    };
    
    static ResultHandler preFavRH = ctx -> {
        ResponseEntity responseEntity = ctx.getResponseEntity();
        String jsonStr = (String) responseEntity.getContent();
        JSONObject json = new JSONObject(jsonStr);
        LinkedList<SpecificTask> tasks = new LinkedList<>();
        if (json.getBoolean("error") == false) {
            JSONObject bodyJson = json.getJSONObject("body");
            int totalCounts = bodyJson.getInt("total");
            int counts = 40;
            int times = totalCounts % counts == 0 ? totalCounts / counts : totalCounts / counts + 1;
            for(int i = 0; i < times; i++) {
                int offset = i * counts;
                tasks.add(new SpecificTask("Fav", "https://www.pixiv.net/ajax/user/10127376/illusts/bookmarks?tag=&offset="
                        + offset + "&limit=" + counts + "&rest=show", null));
            }
            return tasks;
        } else {
            throw new RuntimeException("访问收藏夹api异常:" + jsonStr);
        }
        
    };
    
    static ResultHandler favRH = ctx -> {
        ResponseEntity responseEntity = ctx.getResponseEntity();
        String jsonStr = (String) responseEntity.getContent();
        JSONObject json = new JSONObject(jsonStr);
        if (json.getBoolean("error") == false) {
            JSONArray worksArray = json.getJSONObject("body").getJSONArray("works");
            return worksArray.toList().stream()
                    .map(obj -> {
                        Map<String, Object> map = (Map<String, Object>) obj;
                        String illustId = (String) map.get("id");
                        return new SpecificTask("PreDownload", "https://www.pixiv.net/ajax/illust/" + illustId, null); 
                    })
                    .collect(Collectors.toList());
        } else {
            throw new RuntimeException("访问收藏夹api异常:" + jsonStr);
        }
    };

    static ResultHandler preDownloadRH = ctx -> {
        LinkedList<SpecificTask> tasks = new LinkedList<>();
        
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
                tasks.add(new SpecificTask("Download" , url, null));
            }
            return tasks;
        } else {
            throw new RuntimeException("访问插图api异常:" + jsonStr);
        }
    };
    
    static ResultHandler downloadRH = ctx -> {
        ResponseEntity responseEntity = ctx.getResponseEntity();
        if (!responseEntity.getType().equals(ResponseEntity.TYPE.ByteArray)) {
            throw new IllegalArgumentException("下载错误，响应类型不是字节数组");
        }
        byte[] bytes = (byte[]) responseEntity.getContent();
        String uri = ctx.getUri().toString();
        String fileName = uri.substring(uri.lastIndexOf("/") + 1);// TODO to utils
        log.info("下载成功 {}", fileName);
        Files.write(Paths.get(DOWNLOAD_PATH, fileName), bytes, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        return null;
    };

	public static void main(String[] args) {
		new CrawlerJob().withHTTPProxy("127.0.0.1", 1080)
		        .withCloseableMonitor()
		        .withContextPoolMonitor()
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
	                    .withKeepReceiveCookie()
	                    .and()
                // 访问主页
		        .withTask("HomePage")
		                .withResultHandler(homePageRH)
		                .withKeepReceiveCookie()
		                .and()
                .withTask("PreFav")
                        .withResultHandler(preFavRH)
//                        .withLogDetail()
                        .withKeepReceiveCookie()
                        .and()
                .withTask("Fav")
                        .withResultHandler(favRH)
//                        .withLogDetail()
                        .withKeepReceiveCookie()
                        .and()
                .withTask("PreDownload")
                        .withResultHandler(preDownloadRH)
//                        .withLogDetail()
                        .withKeepReceiveCookie()
                        .withPriority(8)
                        .and()
                .withTask("Download")
                        .withResultType(TYPE.ByteArray)
                        .withResultHandler(downloadRH)
                        .withKeepReceiveCookie()
                        .withRequestHeaders(new BasicHeader("Referer", "https://www.pixiv.net"))
                        .withRedisCache("redis://@localhost:6379/0")
                        .withPriority(9)// 设置高优先级
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
