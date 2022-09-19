package indi.crawler.bootstrap;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;

import com.google.common.collect.Lists;

import indi.crawler.result.ResultHelper;
import indi.crawler.task.ResponseEntity;
import indi.crawler.task.Task;
import indi.data.Wrapper.IntWrapper;
import indi.exception.WrapperException;
import indi.io.FileUtils;
import indi.io.JsonPersistCenter;
import indi.io.PersistCenter;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

/**
 * 来源 https://fgowiki.com<br>
 * 图片地址都是有规律的，应该不用解析html，直接获取即可
 * 
 * <p>2020.09.14 旧网址已废弃，使用新域名：http://fgo-cdn.vgtime.com
 * 
 * <p>礼装：http://fgo-cdn.vgtime.com/media/fgo/equip/card/1294A.jpg
 * <p>英灵： http://fgo-cdn.vgtime.com/media/fgo/servant/card/291A.png
 * 
 * <p>2020.09.14 问题：异常响应（500）的概率太大，需要重复执行才能爬取所有图片
 * 
 * <p>2022.02.21 目前代码存在问题，必须手动确认ID的最大值才能确保不遗漏：一旦设置的ID最大值大于实际最大值，就会导致将那些尚未出现的图片拉黑。
 * 目前有两个应对手段：启动前会清除所有大于已下载图片最大ID的黑名单记录；下载时，只有至少下载了特定类型(如：A)的图片才会被拉黑
 * 
 * @author DragonBoom
 *
 */
@Slf4j
public class FateGOBootstrap {
    private String servantURIPrefix = "http://fgo-cdn.vgtime.com/media/fgo/servant/card/";
    private String equipURIPrefix = "http://fgo-cdn.vgtime.com/media/fgo/equip/card/";
    /** 最大英灵ID */
    @NonNull
    private final Integer maxServantId;
    /** 最大礼装ID */
    @NonNull
    private final Integer maxEquipId;
    private static final String PIC_EXT = ".png";
    /** 英灵立绘类型，一般最多为E，极个别有F甚至G（像玛修） */
    @Getter
    @Setter
    private List<String> servantPicTypes = Lists.newArrayList("A", "B", "C", "D", "E");
    /** 礼装立绘类型，目前只有A */
    @Getter
    @Setter
    private List<String> equipPicTypes = Lists.newArrayList("A");
    
    @NonNull
    private Path sourcePath;
    @NonNull
    private Path servantPath;
    @NonNull
    private Path equipPath;
    @NonNull
    private Boolean useProxy;
    @Getter
    @Setter
    private String proxyHost;
    @Getter
    @Setter
    private Integer proxyPort;
    
    /**
     * 用于记录不存在的图片id；将在请求图片后返回404时更新
     */
    @Getter
    @Setter
    private List<String> notExistServantPngNames = new ArrayList<>();
    /**
     * 用于记录不存在的图片id；将在请求图片后返回404时更新
     */
    @Getter
    @Setter
    private List<String> notExistEquipPngNames = new ArrayList<>();
    private PersistCenter persistCenter;
    /** 必须按从小到大的顺序排列！对于一个id，至少已下载该类型的图片后才会被当作不存在的图片拉黑（否则可能是图片尚未发布，不宜拉黑） */
    private static final List<String> TYPE_THRESHOLDS = Lists.newArrayList("A");
    
    private AtomicLong persistMark = new AtomicLong();
    /** 每多少次更新后要持久化到文件中 */
    private int persistPer = 3;
    
    /**
     * 从文件名中获取图片ID的正则
     */
    private static final Pattern NAME_ID_PATTERN = Pattern.compile("^\\d+");
    
    public FateGOBootstrap(Integer maxServantId, Integer maxEquipId, Path sourcePath, Path servantPath, Path equipPath,
            Boolean useProxy) {
        this.maxServantId = maxServantId;
        this.maxEquipId = maxEquipId;
        this.sourcePath = sourcePath;
        this.servantPath = servantPath;
        this.equipPath = equipPath;
        this.useProxy = useProxy;
        
        persistCenter = new JsonPersistCenter(sourcePath, this, "notExistServantPngNames", "notExistEquipPngNames");
    }
    
    /**
     * 
     * @param task
     * @param destDir 图片存储目录，取servantPath或equipPath，以此区分图片来源了
     * @author DragonBoom
     * @since 2020.09.14
     */
    @SneakyThrows
    private void handleDownloadedFile(Task task, ResultHelper helper, Path destDir) {
        if (destDir != servantPath && destDir != equipPath) {
            throw new IllegalArgumentException("未知路径：" + destDir);
        }
        File tmp = (File) task.getResponseEntity().getContent();
        HttpResponse response = task.getResponse();
        StatusLine statusLine = response.getStatusLine();
        
        String uriStr = task.getUri().toString();
        String fileName = getFileNameByURL(uriStr);
        Matcher matcher = NAME_ID_PATTERN.matcher(fileName);
        if (!matcher.find()) {
            throw new WrapperException("无法从url中获取文件名：" + uriStr);
        }
        String id = matcher.group();// 123A.png 中的 123 
        String type = fileName.substring(id.length(), id.length() + 1);// 123A.png 中的 A
        int statusCode = statusLine.getStatusCode();
        if (statusCode != 200) {
            if (statusCode == 404) {// 仅响应码为404才保存至记录
                // 检验类型，判断是否需要拉黑
                
                // 若该类型位于阈值内，不能拉黑（可能图片尚未发布）
                if (TYPE_THRESHOLDS.contains(type)) {
                    log.warn("检测到尝试拉黑ID，但根据类型阈值而取消了拉黑");
                    return;
                }
                // 若该id的阈值类型文件不存在，也无法拉黑（同上）
                for (String TYPE_THRESHOLD : TYPE_THRESHOLDS) {
                    String thresholdFilename = buildFilename(id, TYPE_THRESHOLD);
                    if (!Files.exists(destDir.resolve(thresholdFilename))) {
                        log.warn("检测到尝试拉黑ID，但根据类型阈值而取消了拉黑");
                        return;
                    }
                }
                
                List<String> types = null;
                // 记录不存在的文件名
                if (destDir.equals(servantPath)) {
                    notExistServantPngNames.add(fileName);
                    types = servantPicTypes;
                } else {
                    notExistEquipPngNames.add(fileName);
                    types = equipPicTypes;
                }
                // 拼接字符串，得到更高级的类型，添加到黑名单集合中
                int typeI = types.indexOf(type);
                while (++typeI < type.length()) {
                    String next = id + types.get(typeI) + PIC_EXT;
                    log.info("计算得到不存在的图片名：{} -> {}", fileName, next);
                }
                
                // 持久化
                long mark = persistMark.incrementAndGet();
                if (mark % persistPer == 0) {
                    persistCenter.persist();
                    log.info("更新持久化文件，每修改{}次集合持久化一次", persistPer);
                }
            } else {
                log.error("获取到错误的响应：{}, {}", uriStr, statusLine);
            }
            // 删除下载的html文件
            tmp.delete();
            return;
        }

        Path targetPath = destDir.resolve(fileName);
        if (Files.exists(targetPath)) {
            tmp.delete();
        } else {
            log.info("成功下载文件：{}", fileName);
            Files.move(tmp.toPath(), targetPath, StandardCopyOption.REPLACE_EXISTING);
        }
        return;
    }
    
    /**
     * 
     * @param url
     * @return 123A.png
     * @author DragonBoom
     * @since 2020.09.14
     */
    private String getFileNameByURL(String url) {
        return url.substring(url.lastIndexOf("/") + 1);
    }
    
    private void formatCollections() {
        // 格式化集合
        // a. 去重
        notExistEquipPngNames = notExistEquipPngNames.stream().distinct().collect(Collectors.toList());
        notExistServantPngNames = notExistServantPngNames.stream().distinct().collect(Collectors.toList());
        // b. 排序
        notExistEquipPngNames.sort(Comparator.naturalOrder());
        notExistServantPngNames.sort(Comparator.naturalOrder());
    }
    
    @SneakyThrows
    public void go() {
        persistCenter.read();
        formatCollections();
        persistCenter.persist();
        
        // 创建目录
        FileUtils.createDirectoryIfNotExist(sourcePath);
        FileUtils.createDirectoryIfNotExist(servantPath);
        FileUtils.createDirectoryIfNotExist(equipPath);
        
        // go
        CrawlerJob job = CrawlerJob.build();
        job
                .withCloseCallback(() -> {
                    try {
                        formatCollections();
                        persistCenter.persist();
                        log.info("更新持久化文件成功");
                    } catch (Exception e) {
                        throw new WrapperException(e);
                    }
                })
                .withTask("Servant")
                    .withResultType(ResponseEntity.TYPE.TMP_FILE)
                    .withTmpDir("F:\\tmp\\fgo")
                    .withResultHandler((ctx, helper) -> handleDownloadedFile(ctx, helper, servantPath))
                    .withLogDetail()
                    .withBlockingMillis(1000l)
                    .withURIChecker(uri -> {
                        return !notExistServantPngNames.contains(getFileNameByURL(uri));
                    })
                    .and()
                .withTask("Equip")
                    .withResultType(ResponseEntity.TYPE.TMP_FILE)
                    .withTmpDir("F:\\tmp\\fgo")
                    .withResultHandler((ctx, helper) -> handleDownloadedFile(ctx, helper, equipPath))
                    .withLogDetail()
                    .withBlockingMillis(1000l)
                    .withURIChecker(uri -> {
                        return !notExistEquipPngNames.contains(getFileNameByURL(uri));
                    })
                    .and()
                ;
        if (useProxy) {
            job.withHTTPProxy(proxyHost, proxyPort);
        }
        job.start();
        
        // 开始创建下载任务
        createServantTask(job);
        createEquipTask(job);
    }
    
    private void createServantTask(CrawlerJob job) {
        // 通过遍历构建所有可能的ID组合
        IntWrapper count = IntWrapper.of(0);
        for (Integer i = 0; i <= maxServantId; i++) {
            String id = new StringBuilder(String.format("%03d", i)).toString();
            // 为每个图遍历所有类型（如：A、B、C...）
            for (String type : servantPicTypes) {
                String fileName = buildFilename(id, type);
                // 跳过已下载文件
                if (Files.exists(servantPath.resolve(fileName))) {
                    continue;// 继续尝试添加更高级的图片(如123A.png -> 123B.png)
                }
                // 根据黑名单，跳过不存在的图片
                if (notExistServantPngNames.contains(fileName)) {
                    break;// 没必要遍历更高级的图片
                }
                String uri = servantURIPrefix + fileName;
                job.addSpecificTask("Servant", uri, null);
                count.plus();
            }
        }
        log.info("已记录{}个不存在的英灵id组合", notExistServantPngNames.size());
        log.info("共添加{}个英灵任务", count.getValue());
    }
    
    private void createEquipTask(CrawlerJob job) {
        // 插入新任务
        IntWrapper count = IntWrapper.of(0);
        for (Integer i = 0; i <= maxEquipId; i++) {
            
            String id = new StringBuilder(String.format("%03d", i)).toString();
            equipPicTypes.forEach(type -> {
                String fileName = buildFilename(id, type);
                // 跳过已下载文件
                if (Files.exists(equipPath.resolve(fileName))) {
                    return;
                }
                // 根据黑名单，跳过不存在的图片
                if (notExistEquipPngNames.contains(fileName)) {
                    return;// 因为没有continue语句，这里会导致无谓地循环更高级的图片
                }
                String uri = equipURIPrefix + fileName;
                job.addSpecificTask("Equip", uri, null);
                count.plus();
            });
        }
        log.info("已记录{}个不存在的礼装id组合", notExistEquipPngNames.size());
        log.info("共添加{}个礼装任务", count.getValue());
    }
    
    private String buildFilename(String id, String type) {
        return new StringBuilder(id).append(type).append(PIC_EXT).toString();
    }
    
    /**
     * 完全清除之前尝试下载时发现的不存在记录；用于下载在原本内容基础上新增的图片
     * 
     * <p>执行后将立即更新持久化文件
     * 
     * @since 2021.01.26
     */
    public void clearNoneExistRecord() {
        notExistEquipPngNames.clear();
        notExistServantPngNames.clear();
        
        try {
            persistCenter.persist();
        } catch (Exception e) {
            throw new WrapperException(e);
        }
    }
    
    /**
     * 清除id超出已下载最大值的不存在记录。将在已下载图片中根据id寻找最新一个，清除其后图片的黑名单记录；
     * 在游戏内容更新后调用，确保能下载到全新的内容
     * 
     * <p>执行后将立即更新持久化文件
     * 
     * @since 2021.01.26
     */
    public void limitNoneExistRecord() {
        log.info("开始清除id超出已下载最大值的不存在记录");
        if (notExistServantPngNames.isEmpty()) {
            try {
                persistCenter.read();
            } catch (Exception e) {
                throw new WrapperException(e);
            }
        }
        // servant
        int cleanServantCount = limitNotExistRecord0(notExistServantPngNames, servantPath);
        log.info("已清理servant不存在记录{}个", cleanServantCount);
        // equip
        int cleanEquipCount = limitNotExistRecord0(notExistEquipPngNames, equipPath);
        log.info("已清理equip不存在记录{}个", cleanEquipCount);
        
        try {
            persistCenter.persist();
        } catch (Exception e) {
            throw new WrapperException(e);
        }
        log.info("清除id超出已下载最大值的不存在记录完成");
    }
    
    /**
     * 
     * @param recordNames
     * @param path
     * @return 清理数量
     * @since 2021.01.26
     */
    private int limitNotExistRecord0(Collection<String> recordNames, Path path) {
        int count = 0;
        Pattern pattern = Pattern.compile("[0-9]+(?=[A-Z])");
        int maxIdNum = -1;
        try (Stream<Path> list = Files.list(path)) {
            Optional<Integer> maxOptional = list
                    .filter(p -> !Files.isDirectory(p))
                    // 1315A.png -> 1315
                    .map(p -> {
                        String name = p.getFileName().toString();
                        Matcher matcher = pattern.matcher(name);
                        if (matcher.find()) {
                            return Integer.valueOf(matcher.group());
                        } else {
                            log.error("无法从文件名中识别出id数字：{}", p);
                            return -1;
                        }
                    })
                    .max(Integer::compare);
            maxIdNum = maxOptional.orElseThrow(() -> new RuntimeException("无法获取最大servant文件id"));
        } catch (IOException e) {
            throw new WrapperException(e);
        }
        
        Iterator<String> iterator = recordNames.iterator();
        while (iterator.hasNext()) {
            String name = iterator.next();
            Matcher matcher = pattern.matcher(name);
            if (matcher.find()) {
                Integer servantId = Integer.valueOf(matcher.group());
                if (servantId > maxIdNum) {
                    iterator.remove();
                    count++;
                }
            }
        }
        return count;
    }

    public static void main(String... args) {
        Path sourcePath = Paths.get("f:", "byCrawler", "fgo");
        Path servantPath = Paths.get("f:", "byCrawler", "fgo", "servant");
        Path equipPath = Paths.get("f:", "byCrawler", "fgo", "equip");
        
        FateGOBootstrap bootstrap = new FateGOBootstrap(336, 1562,sourcePath, servantPath, equipPath, false);
        bootstrap.limitNoneExistRecord();
        
        bootstrap.setProxyHost("127.0.0.1");
        bootstrap.setProxyPort(10810);
        bootstrap.go();
    }

}
