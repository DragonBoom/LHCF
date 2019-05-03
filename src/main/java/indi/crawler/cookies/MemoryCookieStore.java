package indi.crawler.cookies;

import java.net.HttpCookie;
import java.util.LinkedList;
import java.util.List;

import com.google.common.collect.ListMultimap;
import com.google.common.collect.MultimapBuilder;

public class MemoryCookieStore extends BasicCookieStore {
    // domain -> cookies
    private ListMultimap<String, HttpCookie> store;

    private void init() {
        store = MultimapBuilder.SetMultimapBuilder.linkedHashKeys().arrayListValues().build();
    }

    public MemoryCookieStore() {
        init();
    }

    @Override
    public synchronized List<HttpCookie> get0(String domain) {
        LinkedList<HttpCookie> result = new LinkedList<>();
        List<HttpCookie> cookies = store.get(domain);
        cookies.forEach(cookie -> {
            result.add(cookie);
        });
        return result;
    }

    @Override
    public synchronized void add0(HttpCookie cookie) {
        String domain = cookie.getDomain();
        store.put(domain, cookie);
    }

    @Override
    public synchronized boolean clear0(String domain) {
        return store.removeAll(domain) != null;
    }

}
