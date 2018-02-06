package org.vanilladb.dd.schedule.tpart.sink;

import java.util.Iterator;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.server.task.Task;
import org.vanilladb.dd.server.task.tpart.TPartStoredProcedureTask;

/**
 * The task that sets up the cached records (read from sink), remote flags and
 * write back flags for input T-part stored procedures. After it prepared the
 * cache records for all local and remote procedures, it schedules the stored
 * procedures.
 * 
 */
public class SinkingTask extends Task {
	Iterator<TPartStoredProcedureTask> plans;
	int mySinkProcessId;

	public SinkingTask(Iterator<TPartStoredProcedureTask> plans,
			int mySinkProcessId) {
		this.plans = plans;
		this.mySinkProcessId = mySinkProcessId;

		// TODO: add inputs in the arg.

		// sink read info: <RecordKey, src:mySinkId, dest, dest-partitionId>
		// write back flag.: <RecordKey, src>
		// remote flag: <RecordKey, src, dest>

	}

	@Override
	public void run() {
		// 1. lock reading items for own task to perform 2. and 3.

		// 2. create the cache record from sink with proper entry key
		// use NewTPartCacheMgr.createCacheRecord()

		// 3. send record read from this sink to remote
		// use NewTPartCacheMgr.readFromSink() and ConnectionMgr

		// 4. schedule the stored procedures and run the tasks in
		// Iterator<TPartStoredProcedureTask> plans
		while (plans.hasNext()) {
			TPartStoredProcedureTask spt = plans.next();
			spt.lockConservatively();
			// create worker thread to serve the request
			VanillaDb.taskMgr().runTask(spt);
		}

	}

}
