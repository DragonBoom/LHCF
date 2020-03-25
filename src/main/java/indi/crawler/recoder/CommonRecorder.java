package indi.crawler.recoder;

import java.util.HashSet;

import indi.crawler.task.Task;

/**
 * 爬虫任务记录器，用于判断是否执行过爬虫任务。该类通过记录Task的哈希值实现
 * 
 * @author wzh
 * @since 2020.01.18
 */
public class CommonRecorder implements Recorder {
    private HashSet<Integer> records;

    public CommonRecorder() {
        records = new HashSet<>();
    }

    @Override
    public synchronized boolean chechAndRecord(Task ctx) {
        // 仅缓存hashCode，以降低内存占用
        if (records.contains(ctx.hashCode())) {
            return false;
        }
        return records.add(ctx.hashCode());
    }

    @Override
    public synchronized boolean checkRecord(Task ctx) {
        return records.contains(ctx.hashCode());
    }

    @Override
    public synchronized boolean removeRecord(Task ctx) {
        return records.remove(ctx.hashCode());
    }

}
