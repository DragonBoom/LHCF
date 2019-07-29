package indi.crawler.recoder;

import java.util.HashSet;

import indi.crawler.task.Task;

public class CommonRecorder implements Recorder {
    private HashSet<Integer> records;

    public CommonRecorder() {
        records = new HashSet<>();
    }

    @Override
    public synchronized boolean chechAndRecord(Task ctx) {
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
