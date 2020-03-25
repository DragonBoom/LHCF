package indi.dragonboom.crawler.bootstrap;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import indi.crawler.bootstrap.KonachanFavBootstrap;
import lombok.extern.slf4j.Slf4j;

/**
 * <li>2019.12.06 目前的代码未linux vps版本。需要考虑到，
 * <pre>
 *      1. 与vps传输文件，上传慢但下载快
 *      2. vps本身执行速度（爬虫/压缩）特别快，与vps传输才是耗时最久的一环
 * </pre>
 * 
 * @author wzh
 */
@Slf4j
public class KonachanTotalTest {
    /**
     * 启动任务。必须用main函数，而不是测试用例的方式启动，因为测试用例会在主线程执行完后就结束其他线程，无法完成整个下载流程。
     * 
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        KonachanFavBootstrap bootstrap = new KonachanFavBootstrap();
        bootstrap.setIsCompressWhenComplete(false);
        bootstrap.run();
    }

}
