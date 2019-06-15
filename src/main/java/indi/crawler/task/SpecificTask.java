package indi.crawler.task;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;
import java.util.Optional;

import indi.crawler.nest.CrawlerContext;
import indi.crawler.nest.CrawlerController;
import indi.util.Cleanupable;

/**
 * Task + URI + RequestEntity
 * 
 * @author DragonBoom
 *
 */
public class SpecificTask implements Cleanupable {
	private String taskName;
	private URI uri;
	private String entity;

	public SpecificTask(String taskName, String uri, String entity) {
		this.taskName = Optional.of(taskName).get();
		Objects.requireNonNull(uri);
		try {
			this.uri = new URI(uri);
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
		this.entity = entity;
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
		entity = null;
	}

	public CrawlerContext toCrawlerContext(CrawlerController controller) {
		Task task = controller.getJob().getTask(taskName);
		Objects.requireNonNull(task);
		return controller.getContextFactory().createContext(task, uri, entity);
	}

}
