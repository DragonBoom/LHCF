package indi.dragonboom.crawler.bootstrap;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.function.Consumer;

import com.google.common.collect.ImmutableList;

import indi.crawler.bootstrap.CrawlerJob;
import indi.crawler.task.ResponseEntity;
import indi.crawler.task.def.TaskType;
import lombok.extern.slf4j.Slf4j;

/**
 * 来源 https://fgowiki.com<br>
 * 图片地址都是有规律的，应该不用解析html，直接获取即可
 * 
 * <p>礼装 http://img.fgowiki.com/fgo/card/equip/001A.jpg
 * <p>英灵 https://img.fgowiki.com/fgo/card/servant/223A.png
 * 
 * @author DragonBoom
 *
 */
@Slf4j
public class FateGOTest {
    private static final Path path = Paths.get("e:", "byCrawler", "fgo");
    private static final Path servantPath = Paths.get("e:", "byCrawler", "fgo", "servant");
    private static final Path equipPath = Paths.get("e:", "byCrawler", "fgo", "equip");
    
    private static final ImmutableList<String> servantPicTypes = ImmutableList.of("A", "B", "C", "D", "E");

    public static void main(String... args) {
        // craeteIfNotExist
        Consumer<Path> craeteIfNotExist = path -> {
            log.info("检测目标路径 {}", path);
            if (!Files.isDirectory(path)) {
                try {
                    Files.createDirectories(path);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        };
        craeteIfNotExist.accept(path);
        craeteIfNotExist.accept(servantPath);
        craeteIfNotExist.accept(equipPath);
        // go
        CrawlerJob job = CrawlerJob.build()
                .withContextPoolMonitor()
                .withTask("servant")
                    .withType(TaskType.HTTP_TOPICAL)
                    .withResultType(ResponseEntity.TYPE.ByteArray)
                    .withResultHandler((ctx, helper) -> {
                        byte[] bytes = (byte[]) ctx.getResponseEntity().getContent();
                        String uriStr = ctx.getUri().toString();
                        String fileName = uriStr.substring(uriStr.lastIndexOf("/") + 1);
                        if (bytes.length < 1024) {// 如图片不存在
                            return;
                        }
                        Path filePath = servantPath.resolve(fileName);
//                        if (Files.exists(filePath)) {
//                            System.out.println(fileName + " 已存在");
//                            return null;
//                        }
                        Files.write(filePath, bytes, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
                        return;
                    })
                    .withLogDetail()
                    .and()
                ;
        job.addSpecificTask("https://img.fgowiki.com/fgo/card/servant/223A.png");
        for (Integer i = 0; i <= 223; i++) {
            StringBuilder sb = new StringBuilder("http://img.fgowiki.com/fgo/card/servant/");
            int numStrLength = i.toString().length();
            while (numStrLength < 3) {
                sb.append("0");
                numStrLength++;
            }
            sb.append(i);
            servantPicTypes.forEach(type -> {
                String uri = new StringBuilder(sb.toString()).append(type).append(".png").toString();
                System.out.println(uri);
                job.addSpecificTask(uri);
            });
        }
    }
}
