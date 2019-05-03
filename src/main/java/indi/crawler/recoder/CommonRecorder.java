package indi.crawler.recoder;

import java.util.HashSet;

import indi.crawler.nest.CrawlerContext;

public class CommonRecorder implements Recorder {
    private HashSet<Integer> records;

    public CommonRecorder() {
        records = new HashSet<>();
    }

    @Override
    public synchronized boolean chechAndRecord(CrawlerContext ctx) {
        if (records.contains(ctx.hashCode())) {
            return false;
        }
        return records.add(ctx.hashCode());
    }

    @Override
    public synchronized boolean checkRecord(CrawlerContext ctx) {
        return records.contains(ctx.hashCode());
    }

    @Override
    public synchronized boolean removeRecord(CrawlerContext ctx) {
        return records.remove(ctx.hashCode());
    }

}
