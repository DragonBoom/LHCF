/**
 * 
 */
package indi.crawler.bootstrap;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.LinkedList;
import java.util.List;

import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicNameValuePair;

import indi.exception.WrapperException;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * 用于快速链式地构建HttpEntity
 * 
 * @author wzh
 * @since 2020.09.04
 */
public class HttpEntityBuilder {
    
    /**
     * 构建请求实体为StringEntity格式的请求
     * 
     * @param content
     * @return
     * @author DragonBoom
     * @since 2020.09.04
     */
    public static StringEntity buildStringEntity(String content) {
        try {
            return new StringEntity(content);
        } catch (UnsupportedEncodingException e) {
            throw new WrapperException(e);
        }
    }
    
    /**
     * 链式地构建请求实体为UrlEncodedForm的请求
     * 
     * @param content
     * @return
     * @author DragonBoom
     * @since 2020.09.04
     */
    public static UrlEncodedFormEntityBuilder beginEncodedFormEntity() {
        return new UrlEncodedFormEntityBuilder();
    }

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static class UrlEncodedFormEntityBuilder {
        List <NameValuePair> nvps = new LinkedList<>();
        
        public UrlEncodedFormEntityBuilder with(String key, String value) {
            nvps.add(new BasicNameValuePair(key, value));
            return this;
        }
        
        public UrlEncodedFormEntity build(Charset charset) {
            return new UrlEncodedFormEntity(nvps, charset);
        }
        
        public UrlEncodedFormEntity build() {
            return new UrlEncodedFormEntity(nvps, Charset.defaultCharset());
        }
    }
}
