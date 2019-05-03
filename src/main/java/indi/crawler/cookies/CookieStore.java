package indi.crawler.cookies;

import java.net.URI;

/**
 * Cookie存取库
 * 
 * @author dragonboom
 *
 */
public interface CookieStore {

    String get(URI uri);

    void add(String setCookie, URI uri);

    boolean clear(String domain);
}
