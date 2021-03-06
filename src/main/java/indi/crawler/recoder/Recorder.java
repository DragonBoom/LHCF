package indi.crawler.recoder;

import indi.crawler.task.Task;

/**
 * 用于记录爬取历史，根据任务类型进行分类
 * 
 * @author DragonBoom
 *
 */
public interface Recorder {

    /**
     * 若没有记录，则添加记录并返回false，若有记录，则返回true<br>
     * 该方法必须是线程安全的
     * 
     * @param ctx
     * @return
     */
	boolean chechAndRecord(Task ctx);

	/**
	 * 
	 * @param ctx
	 * @return 若有记录，则返回true
	 */
	boolean checkRecord(Task ctx);
	
	boolean removeRecord(Task ctx);
	
}
