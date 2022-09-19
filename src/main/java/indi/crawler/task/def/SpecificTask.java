package indi.crawler.task.def;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;
import java.util.Optional;

import org.apache.http.HttpEntity;

import indi.crawler.task.CrawlerController;
import indi.crawler.task.Task;
import indi.obj.Cleanupable;

/**
 * Task + URI + RequestEntity
 * 
 * @author DragonBoom
 *
 */
public class SpecificTask implements Cleanupable {
	private String taskName;
	private URI uri;
	private HttpEntity requestEntity;

	public SpecificTask(String taskName, String uri, HttpEntity requestEntity) {
		this.taskName = Optional.of(taskName).get();
		Objects.requireNonNull(uri);
		try {
			this.uri = new URI(uri);
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
		this.requestEntity = requestEntity;
	}

	public String getTaskName() {
		return taskName;
	}

	public URI getUri() {
		return uri;
	}

	@Override
	public void cleanup() {
		taskName = null;
		uri = null;
		requestEntity = null;
	}

	public Task toTask(CrawlerController controller) {
		TaskDef task = controller.getJob().getTaskDef(taskName);
		Objects.requireNonNull(task);
		return controller.getTaskFactory().build(task, uri, requestEntity);
	}

}
