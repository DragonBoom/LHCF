/**
 * 
 */
package indi.crawler.task;

/**
 * 提供通用逻辑
 * 
 * @author wzh
 * @since 2021.12.10
 */
public abstract class BasicTaskPool implements TaskPool {
    

    @Override
    public boolean offer(Task task) {
        boolean result = offer0(task);
        if (result) {
            task.setLeasedTime(-1);
            task.setRegisteredTime(System.currentTimeMillis());
        }
        return result;
    }
    
    abstract boolean offer0(Task task); 

    @Override
    public Task poll() {
        Task ctx = poll0();
        if (ctx != null) {
            ctx.setLeasedTime(System.currentTimeMillis());
        }
        return ctx;
    }
    
    abstract Task poll0();
    
    @Override
    public boolean deferral(Task task, Long wakeUpTime) {
        boolean result = deferral0(task, wakeUpTime);
        if (result) {
            task.checkAndSetStatus(CrawlerStatus.DEFERRED);
        }
        return result;
    }
    
    abstract boolean deferral0(Task task, Long wakeUpTime);
    
}
