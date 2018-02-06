package org.vanilladb.dd.server.task.tpart;

import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.util.Timers;
import org.vanilladb.dd.schedule.tpart.TPartStoredProcedure;
import org.vanilladb.dd.schedule.tpart.sink.SunkPlan;
import org.vanilladb.dd.server.VanillaDdDb;
import org.vanilladb.dd.server.task.StoredProcedureTask;
import org.vanilladb.dd.sql.RecordKey;

public class TPartStoredProcedureTask extends StoredProcedureTask {

	private TPartStoredProcedure sp;
	private int cid, rteId, parId;
	private long txNum;
	private static long startTime = System.nanoTime();
	private long taskStartTime = System.nanoTime();

	public TPartStoredProcedureTask(int cid, int rteId, long txNum,
			TPartStoredProcedure sp) {
		super(cid, rteId, txNum, sp);
		this.cid = cid;
		this.rteId = rteId;
		this.txNum = txNum;
		this.sp = sp;
	}

	@Override
	public void run() {
//		Timers.createTimer(txNum);
		SpResultSet rs = null;
//		Timers.getTimer().startExecution();

//		try {
			rs = sp.execute();
//		} finally {
//			Timers.getTimer().stopExecution();
//		}

		if (sp.isMaster()) {
			VanillaDdDb.connectionMgr().sendClientResponse(cid, rteId, txNum,
					rs);
			//System.out.println("Commit: " + (System.nanoTime() - startTime));
		}
		// System.out.println("task time:" + (System.nanoTime() -
		// taskStartTime));
//		Timers.reportTime();
	}

	public long getTxNum() {
		return txNum;
	}

	public RecordKey[] getReadSet() {
		return sp.getReadSet();
	}

	public RecordKey[] getWriteSet() {
		return sp.getWriteSet();
	}

	public double getWeight() {
		return sp.getWeight();
	}

	public int getPartitionId() {
		return parId;
	}

	public void setPartitionId(int parId) {
		this.parId = parId;
	}

	public void setSunkPlan(SunkPlan plan) {
		sp.setSunkPlan(plan);
	}

	public int getProcedureType() {
		if (sp == null)
			return TPartStoredProcedure.NOP;
		return sp.getProcedureType();
	}

	public boolean isReadOnly() {
		return sp.isReadOnly();
	}
}
