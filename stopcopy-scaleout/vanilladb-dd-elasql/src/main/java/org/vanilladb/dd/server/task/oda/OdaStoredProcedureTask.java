package org.vanilladb.dd.server.task.oda;

import java.util.Map;

import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.dd.schedule.DdStoredProcedure;
import org.vanilladb.dd.schedule.oda.OdaStoredProcedure;
import org.vanilladb.dd.server.VanillaDdDb;
import org.vanilladb.dd.server.task.StoredProcedureTask;
import org.vanilladb.dd.sql.RecordKey;

public class OdaStoredProcedureTask extends StoredProcedureTask {

	private static long startTime;

	// private static long startTime;
	//
	static {
		startTime = System.nanoTime();
	}

	public OdaStoredProcedureTask(int cid, int rteId, long txNum,
			DdStoredProcedure sp) {
		super(cid, rteId, txNum, sp);
	}

	public void run() {
		// long time = System.currentTimeMillis();

		OdaStoredProcedure sp = (OdaStoredProcedure) this.sp;
		sp.prepareRecords();
		SpResultSet rs = sp.execute();
		VanillaDdDb.connectionMgr().sendClientResponse(cid, rteId, txNum, rs);

		// System.out.println("Commit: " + (System.nanoTime() - startTime));
		// System.out.println("RTE: " + rteId);

		sp.updateCacheMgr();
		sp.releaseCachedRecords();
	}

	public void setReadLinks(Map<RecordKey, Long> link) {
		((OdaStoredProcedure) sp).setReadLinks(link);
	}

}
