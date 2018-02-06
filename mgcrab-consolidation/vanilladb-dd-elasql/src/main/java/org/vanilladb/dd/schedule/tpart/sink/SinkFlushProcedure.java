package org.vanilladb.dd.schedule.tpart.sink;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.dd.cache.tpart.NewTPartCacheMgr;
import org.vanilladb.dd.schedule.tpart.TPartStoredProcedure;
import org.vanilladb.dd.server.VanillaDdDb;
import org.vanilladb.dd.sql.RecordKey;
import org.vanilladb.dd.sql.RecordVersion;
import org.vanilladb.dd.storage.tx.concurrency.ConservativeOrderedCcMgr;

public class SinkFlushProcedure implements TPartStoredProcedure {
	private Set<RecordVersion> writeBackList;
	private Transaction tx;
	private RecordKey[] writeKeys;
	private long txNum;
	private int sinkId;

	public SinkFlushProcedure(long txNum, int sinkId) {
		this.txNum = txNum;
		this.sinkId = sinkId;
	}

	public void requestConservativeLocks() {
		if (writeBackList == null)
			return;
		List<RecordKey> keys = new ArrayList<RecordKey>();
		if (writeBackList != null)
			for (RecordVersion rv : writeBackList)
				keys.add(rv.key);

		writeKeys = keys.toArray(new RecordKey[keys.size()]);

		tx = VanillaDdDb.txMgr().transaction(
				Connection.TRANSACTION_SERIALIZABLE, false, txNum);
		ConservativeOrderedCcMgr ccMgr = (ConservativeOrderedCcMgr) tx
				.concurrencyMgr();
		ccMgr.prepareWriteBack(writeKeys);
	}

	@SuppressWarnings("unchecked")
	public void prepare(Object... pars) {
		if (pars[0] != null)
			writeBackList = (Set<RecordVersion>) pars[0];
	}

	public SpResultSet execute() {
		if (writeBackList == null)
			return null;

		NewTPartCacheMgr cm = VanillaDdDb.tPartCacheMgr();

		// write back to local storage
		ConservativeOrderedCcMgr ccMgr = (ConservativeOrderedCcMgr) tx
				.concurrencyMgr();
		ccMgr.executeWriteBack(writeKeys);

		for (RecordVersion rv : writeBackList) {
			cm.writeBack(rv.key, rv.srcTxNum, sinkId, tx);
			ccMgr.releaseWriteBackLock(rv.key);
		}
		tx.commit();
		return null;
	}

	public RecordKey[] getReadSet() {
		return null;
	}

	public RecordKey[] getWriteSet() {
		return null;
	}

	public void setSunkPlan(SunkPlan plan) {
	}

	public double getWeight() {
		return 0;
	}

	public int getProcedureType() {
		return 0;
	}

	public boolean isReadOnly() {
		return true;
	}
}