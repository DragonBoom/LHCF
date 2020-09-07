package indi.dragonboom.crawler.bootstrap;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.junit.jupiter.api.Test;

import com.google.common.reflect.ClassPath;

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
    public static void main(String[] args) throws Exception {
        // 由于用Eclipse直接打包为Runnable jar时日志的配置文件的路径不是/src/main/resources而是/resources，故用以下代码进行兼容
        URL log4j2Conf = KonachanTotalTest.class.getClassLoader().getResource("resources/log4j2.xml");
        
        if (log4j2Conf != null) {
            LoggerContext logContext = (LoggerContext) LogManager.getContext(false);
            logContext.setConfigLocation(log4j2Conf.toURI());
            logContext.reconfigure();
            log.info("使用可执行文件内的日志配置文件，请忽视找不到配置文件的报错");
        }
        
        
        KonachanFavBootstrap bootstrap = new KonachanFavBootstrap();
        bootstrap.setIsCompressWhenComplete(false);
        bootstrap.run();
    }

}
