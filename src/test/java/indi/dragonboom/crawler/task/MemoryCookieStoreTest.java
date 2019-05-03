package indi.dragonboom.crawler.task;

import java.net.URI;
import java.net.URISyntaxException;

import org.junit.jupiter.api.Test;

import indi.crawler.cookies.MemoryCookieStore;

class MemoryCookieStoreTest {

	@Test
	void addTest() throws URISyntaxException {
		MemoryCookieStore store = new MemoryCookieStore();
		store.add("BAIDUID=CB2AE427A7219B8A08CF90ACCD60DFC9:FG=1; expires=Thu, 31-Dec-37 23:55:55 GMT; max-age=2147483647; path=/; domain=.baidu.com",
				new URI("https://www.nowcoder.com/8649939"));
		String result = store.get(new URI("http://www.baidu.com"));
		System.out.println(result);
	}
}
