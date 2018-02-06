package org.vanilladb.dd.cache.oda;

import java.util.concurrent.BlockingQueue;

import org.vanilladb.core.server.task.Task;
import org.vanilladb.dd.sql.RecordKey;
import org.vanilladb.dd.sql.RecordVersion;

public class WriteBackTask extends Task {

	OdaCacheMgr cacheMgr;
	private BlockingQueue<RecordVersion> taskQueue;

	public WriteBackTask(OdaCacheMgr cacheMgr) {
		this.cacheMgr = cacheMgr;
	}

	public void addRecord(RecordKey key, Long txn) {
		taskQueue.add(new RecordVersion(key, txn));
	}

	@Override
	public void run() {

		while (true) {

			// blocked if the task queue is empty
			try {
				RecordVersion task = taskQueue.take();

				// cacheMgr.flushToLocalStorage(key, srcTxNum, tx)

			} catch (InterruptedException e) {
				e.printStackTrace();
			}

		}

	}
}
