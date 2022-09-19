/**
 * 
 */
package indi.crawler.util;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

import indi.exception.WrapperException;
import indi.io.FileUtils;
import lombok.extern.slf4j.Slf4j;

/**
 * @author wzh
 * @since 2020.04.07
 */
@Slf4j
public class JobUtils {
    
    /**
     * 更新本地文件名的缓存
     * 
     * @author DragonBoom
     * @since 2020.04.07
     * @param downloadPath 文件路径
     * @param filenameCaches 文件名集合（不含后缀）
     * @param predicate 判断那些文件需要记录（函数返回true表示需要记录）
     */
    public static void refreshLocalFilenameCaches(Path downloadPath, Set<String> filenameCaches,
            Predicate<? super Path> predicate) {
        log.info("开始刷新下载文件标识缓存。。。{}", downloadPath);
        filenameCaches.clear();
        // 缓存已下载的图片序号
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(downloadPath)) {
            stream.forEach(p -> Optional.ofNullable(p)
                    .filter(predicate)
                    .ifPresent(picPath -> {
                        filenameCaches.add(FileUtils.getFileName(picPath));
                    }));
        } catch (IOException e) {
            throw new WrapperException(e);
        }
        log.info("刷新本地图片序号缓存完成");
    }

    /**
     * FIXME: 这里面code的概念与业务耦合太过严重；该方法目前并不通用，不应该工具化
     * 
     * @author DragonBoom
     * @since 2020.04.07
     * @param onlineCodes 线上存在的图片序号集合
     * @return
     * @throws IOException 
     */
    public static void delNoRecordFiles(Set<String> onlineCodes, Set<String> localCodes, Path downloadPath) throws IOException {
        log.info("开始移除没有线上记录的文件");
        // 读取本次下载的图片序号
        if (onlineCodes.isEmpty()) {
            throw new IllegalAccessError("线上下载记录为空，可能没有记录下载内容或没有下载，无法移除没有记录的文件");
        }
        
        // 已下载 - 线上记录 = 已下载且线上没记录
        HashSet<String> deleteCodes = new HashSet<>();
        deleteCodes.addAll(localCodes);
        deleteCodes.removeAll(onlineCodes);
        
        if (deleteCodes.isEmpty()) {
            log.info("所有文件均有记录，无需移除");
            return;
        }
        
        // 删除 已下载且线上没记录 的文件
        // a. 构建 code -> Path 的映射
        Map<String, Path> codePathMap = new HashMap<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(downloadPath)) {
            stream.forEach(p -> {
                String fileName = FileUtils.getFileName(p.toString());
                codePathMap.put(fileName, p);
            });
            
        }
        // b. 逐个删除文件
        for (String code : deleteCodes) {
            Path deletePath = codePathMap.get(code);
            log.info("删除线上已移除的文件：{}", deletePath);
            Files.delete(deletePath);
        }
        // 更新已下载记录
        log.info("移除没有下载记录的文件完成");
    
    }
    
    /**
     * 根据文件名白名单，删除本地文件
     * 
     * @author DragonBoom
     * @since 2020.04.07
     * @param whiteList 文件名白名单集合；是否含后缀由参数中的comparator决定
     * @param filenames 需处理的文件名集合，不会进行修改，将从中选择不存在于白名单的文件进行删除；即只删除该集合中的文件
     * （不含后缀）
     * @param path 文件上级路径
     * @param comparator 用于匹配第一个、第二个集合的元素 (whiteName, filename)；函数中，返回0表匹配
     * @param delMsg 删除时的日志。可使用一个{}参数，代表所删除文件的路径
     * @return
     * @throws IOException 
     */
    public static void delLocalFileByFilenameWhiteList(Set<String> whiteList, Set<String> filenames, Path path, 
            Comparator<String> comparator, String delMsg) {
                if (whiteList.isEmpty()) {
                    throw new IllegalAccessError("白名单记录为空，可能发生异常，为保证数据安全不移除文件");
                }

                HashSet<String> filenamesTmp = new HashSet<>(filenames);
                // n*n
                whiteList.forEach(whiteName -> 
                        filenamesTmp.removeIf(filename -> comparator.compare(whiteName, filename) == 0));
//                if (true) {
//                    System.out.println(filenamesTmp);
//                    System.out.println("待删除文件数：" + filenamesTmp.size());
//                    return;
//                }
                try (Stream<Path> stream = Files.list(path)) {
                    stream
                            .filter(p -> !Files.isDirectory(p))
                            .forEach(p -> {
                                String pathFilename = FileUtils.getFileName(p);
                                if (filenamesTmp.contains(pathFilename)) {
                                    log.info(delMsg, p);
                                    try {
                                        Files.delete(p);
                                    } catch (IOException e) {
                                        throw new WrapperException(e);
                                    }
                                }
                            });
                } catch (IOException e) {
                    throw new WrapperException(e);
                }
    }
}
