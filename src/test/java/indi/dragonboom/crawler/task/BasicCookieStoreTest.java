package indi.dragonboom.crawler.task;

import java.util.LinkedList;

import org.junit.jupiter.api.Test;

import indi.crawler.cookies.BasicCookieStore;

class BasicCookieStoreTest {

	@Test
	void hostPaseTest() {
		String host = "fanyi.baidu.com";
		LinkedList<String> domains = BasicCookieStore.parseAllDomain(host);
		for (String domain : domains) {
			System.out.println(domain);
		}
	}

}
