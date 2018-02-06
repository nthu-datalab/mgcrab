package org.vanilladb.core.server.task;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * The task manager of VanillaCore. This manager is responsible for maintaining
 * the thread pool of worker thread.
 * 
 */
public class TaskMgr {
	private ExecutorService executor;
	private final static int THREAD_POOL_SIZE;

	static {
		String prop = System.getProperty(TaskMgr.class.getName()
				+ ".THREAD_POOL_SIZE");

		THREAD_POOL_SIZE = (prop == null) ? 150 : Integer.parseInt(prop);
	}

	public TaskMgr() {
		executor = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
	}

	public void runTask(Task task) {
		executor.execute(task);
	}
}
