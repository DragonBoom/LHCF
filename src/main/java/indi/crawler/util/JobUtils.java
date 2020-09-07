/**
 * 
 */
package indi.crawler.util;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

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
     * 更新本地下载记录的缓存
     * 
     * @author DragonBoom
     * @since 2020.04.07
     * @param downloadPath
     * @param downloadedFileNameCaches
     */
    public static void refreshLocalFileCaches(Path downloadPath, Set<String> downloadedFileNameCaches) {
        log.info("开始刷新下载文件标识缓存。。。{}", downloadPath);
        downloadedFileNameCaches.clear();
        // 缓存已下载的图片序号
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(downloadPath)) {
            stream.forEach(p -> Optional.ofNullable(p)
                    // 过滤掉非图片文件
                    .filter(FileUtils::isImage)
                    .ifPresent(picPath -> {
                        downloadedFileNameCaches.add(FileUtils.getFileName(picPath));
                    }));
        } catch (IOException e) {
            throw new WrapperException(e);
        }
        log.info("刷新本地图片序号缓存完成");
    }

    /**
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
        if (onlineCodes.size() == 0) {
            throw new IllegalAccessError("线上下载记录为空，推测没有记录下载内容或没有下载，无法移除没有下载的文件");
        }
        
        // 已下载 - 线上记录 = 已下载且线上没记录
        HashSet<String> deleteCodes = new HashSet<>();
        deleteCodes.addAll(localCodes);
        deleteCodes.removeAll(onlineCodes);
        
        if (deleteCodes.size() == 0) {
            log.info("所有文件均有记录，无需移除");
            return;
        }
        
        // 删除 已下载且线上没记录 的图片
        // a. 构建 code -> Path 的映射
        Map<String, Path> codePathMap = new HashMap<>();
        Files.newDirectoryStream(downloadPath)
            .forEach(p -> {
                String fileName = FileUtils.getFileName(p.toString());
                codePathMap.put(fileName, p);
            });
        // b. 逐个删除文件
        for (String code : deleteCodes) {
            Path deletePath = codePathMap.get(code);
            log.info("删除线上已移除的文件：{}", deletePath);
            Files.delete(deletePath);
        }
        // 更新已下载记录
        log.info("移除没有下载记录的文件完成");
    
    }
}
