package indi.dragonboom.crawler.bootstrap;

import java.io.IOException;

import org.apache.http.HttpHost;
import org.apache.http.client.CookieStore;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.junit.jupiter.api.Test;

class HttpClientTest {

	@Test
	void main() {
		PoolingHttpClientConnectionManager manager = new PoolingHttpClientConnectionManager();
		manager.setDefaultMaxPerRoute(1);
		CookieStore store = new BasicCookieStore();
		CloseableHttpClient client = HttpClientBuilder.create().setDefaultCookieStore(store)
				.setConnectionManager(manager).build();
		HttpGet get = new HttpGet("http://www.baidu.com");
		RequestConfig config = RequestConfig.custom().setProxy(new HttpHost("127.0.0.1", 1080))
				.build();
		get.setConfig(config);
		try {
			System.out.println("pool stats:" + manager.getTotalStats());
			CloseableHttpResponse rsp = client.execute(get);
			System.out.println(EntityUtils.toString(rsp.getEntity()));
			System.out.println("pool stats:" + manager.getTotalStats());
			rsp = client.execute(get);
			System.out.println(EntityUtils.toString(rsp.getEntity()));
			System.out.println("pool stats:" + manager.getTotalStats());
			rsp = client.execute(get);
			System.out.println(EntityUtils.toString(rsp.getEntity()));
			System.out.println("pool stats:" + manager.getTotalStats());
			rsp = client.execute(get);
			System.out.println(EntityUtils.toString(rsp.getEntity()));
			System.out.println("pool stats:" + manager.getTotalStats());
			rsp = client.execute(get);
			System.out.println(EntityUtils.toString(rsp.getEntity()));
			System.out.println("pool stats:" + manager.getTotalStats());
			rsp.close();
		} catch (IOException e2) {
			e2.printStackTrace();
		} finally {
			try {
				client.close();
				System.out.println("close complete");
			} catch (IOException e3) {
				e3.printStackTrace();
			}
		}
		System.out.println(store);
	}
}
