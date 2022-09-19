package indi.crawler.cookies;

import java.net.HttpCookie;
import java.net.URI;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.StringTokenizer;

import org.apache.logging.log4j.util.Strings;

import indi.util.StringUtils;

public abstract class BasicCookieStore implements CookieStore {

	public BasicCookieStore() {
	}

	/**
	 * 获取domain对应的所有cookie，实现时注意确保返回的cookie不能重复
	 */
	protected abstract Collection<HttpCookie> get0(String domain);

	/**
	 * 將Cookie集合转换为Http请求的cookie字段
	 */
	protected String parseCookies(List<HttpCookie> cookies) {
		return StringUtils.join("; ", cookies);
	}

	/**
	 * 获取该uri所需的cookie字段
	 */
	public String get(URI uri) {
		String host = uri.getHost();
		Objects.requireNonNull(host, "uri=" + uri);
		LinkedList<String> domains = parseAllDomain(host);
		LinkedList<HttpCookie> cookies = new LinkedList<>();
		for (String domain : domains) {
			Collection<HttpCookie> tmp = get0(domain);
			if (tmp != null && tmp.size() > 0)
				cookies.addAll(get0(domain));
		}
		return parseCookies(cookies);
	}

	/**
	 * 储存cookie，实现时注意cookie重复的情况
	 */
	protected abstract void add0(HttpCookie cookie);

	protected final static int DEFAULT_COOKIE_VERSION = 0;

	/**
	 * 解析、保存服务器返回的set-cookie字段
	 */
	public void add(String setCookie, URI uri) {
		List<HttpCookie> cookies = HttpCookie.parse(setCookie);
		String host = uri.getHost();
		for (HttpCookie cookie : cookies) {
		    String domain = cookie.getDomain();
			if (Strings.isEmpty(domain) || domain.equals("null")) {
				cookie.setDomain(host);
			}
			cookie.setVersion(DEFAULT_COOKIE_VERSION);
			add0(cookie);
		}
	}

	abstract boolean clear0(String domain);

	public boolean clear(String domain) {
		return clear0(domain);
	}

	/**
	 * 解析域名，得到所有对应的cookie名<br>
	 * 如 www.baidu.com 对应 “.www.baidu.com”， “www.baidu.com”， “.baidu”, “.baidu.com”
	 */
	public static LinkedList<String> parseAllDomain(String host) {
		// String 分隔器
		StringTokenizer token = new StringTokenizer(host, ".");
		// 因为将host解析为域名需要从末尾开始，因此使用堆来存储拆出来的各部分字段
		LinkedList<String> tokenList = new LinkedList<>();
		// String[] temp0 = new String[size];
		// 将分隔的结果入堆
		while (token.hasMoreElements()) {
			tokenList.add(token.nextToken());
		}
		LinkedList<String> result = new LinkedList<>();
		StringBuilder sb = new StringBuilder();
		// 出堆
		String part = tokenList.pollLast();
		// 跳过url后缀
		sb.append(".");
		sb.append(part);
		while (tokenList.size() > 1) {
			part = tokenList.pollLast();
			// 将出堆的元素插入到字符串头部
			sb.insert(0, part);
			sb.insert(0, ".");
			result.add(sb.toString());
		}
		// 既要有www.baidu.com 也要有.www.baidu.com，即前缀加点与不加点都要
		part = tokenList.pollLast();
		sb.insert(0, part);
		result.add(sb.toString());
		sb.insert(0, ".");
		result.add(sb.toString());
		return result;
	}
}