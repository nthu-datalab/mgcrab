package org.vanilladb.dd.schedule.tpart.sink;

import java.sql.Connection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.dd.cache.CachedRecord;
import org.vanilladb.dd.cache.tpart.NewTPartCacheMgr;
import org.vanilladb.dd.remote.groupcomm.TupleSet;
import org.vanilladb.dd.schedule.tpart.TPartStoredProcedure;
import org.vanilladb.dd.server.VanillaDdDb;
import org.vanilladb.dd.sql.RecordKey;
import org.vanilladb.dd.storage.tx.concurrency.ConservativeOrderedCcMgr;

public class SinkPushProcedure implements TPartStoredProcedure {
	private Map<Integer, Set<PushInfo>> pushInfo;
	private Map<RecordKey, Set<Long>> readMap = new HashMap<RecordKey, Set<Long>>();
	private Transaction tx;
	private RecordKey[] readKeys;
	private long txNum;
	private int sinkId;
	private int partId;

	public SinkPushProcedure(long txNum, int partId, int sinkId) {
		this.txNum = txNum;
		this.sinkId = sinkId;
		this.partId = partId;
	}

	public void requestConservativeLocks() {
		if (pushInfo == null)
			return;
		Set<RecordKey> keySet = new HashSet<RecordKey>();
		// prepare all keys for read
		for (Entry<Integer, Set<PushInfo>> entry : pushInfo.entrySet()) {
			for (PushInfo info : entry.getValue())
				keySet.add(info.getRecord());
		}

		for (Entry<RecordKey, Set<Long>> entry : readMap.entrySet()) {
			keySet.add(entry.getKey());
		}

		// get read key set's locks
		readKeys = keySet.toArray(new RecordKey[keySet.size()]);
		tx = VanillaDdDb.txMgr().transaction(
				Connection.TRANSACTION_SERIALIZABLE, false, txNum);
		ConservativeOrderedCcMgr ccMgr = (ConservativeOrderedCcMgr) tx
				.concurrencyMgr();
		ccMgr.prepareSp(readKeys, null);
	}

	@SuppressWarnings("unchecked")
	public void prepare(Object... pars) {
		if (pars[0] != null)
			pushInfo = (Map<Integer, Set<PushInfo>>) pars[0];
		if (pars[1] != null)
			readMap = (Map<RecordKey, Set<Long>>) pars[1];
	}

	public SpResultSet execute() {
//		System.out.println("sink push start");
		if (pushInfo == null)
			return null;

		ConservativeOrderedCcMgr ccMgr = (ConservativeOrderedCcMgr) tx
				.concurrencyMgr();
		ccMgr.executeSp(readKeys, null);

		NewTPartCacheMgr cm = VanillaDdDb.tPartCacheMgr();

		// create cached records for local txns
		for (Entry<RecordKey, Set<Long>> entry : readMap.entrySet()) {
			RecordKey key = entry.getKey();
			Long[] destsL = (Long[]) entry.getValue().toArray(new Long[0]);
			long[] dests = new long[destsL.length];
			for (int i = 0; i < destsL.length; i++) {
				dests[i] = destsL[i];
			}
			cm.createCacheRecordFromSink(key, sinkId, tx, dests);
		}

		// read from local storage and send to remote site
		for (Entry<Integer, Set<PushInfo>> entry : pushInfo.entrySet()) {
			int targetServerId = entry.getKey();
			TupleSet rs = new TupleSet(sinkId);
			for (PushInfo pushInfo : entry.getValue()) {
				long sinkTxnNum = NewTPartCacheMgr.getPartitionTxnId(partId);
				CachedRecord rec = cm.readFromSink(pushInfo.getRecord(),
						sinkId, tx);
				// TODO deal with null value record
//				System.out.println("Push Remote Key:" + pushInfo.getRecord()
//						+ ",src:" + txNum + ",dest:" + pushInfo.getDestTxNum());
				rec.setSrcTxNum(sinkTxnNum);
				rs.addTuple(pushInfo.getRecord(), sinkTxnNum,
						pushInfo.getDestTxNum(), rec);
			}
			VanillaDdDb.connectionMgr().pushTupleSet(targetServerId, rs);
		}

//		System.out.println("sink push finish");
		tx.commit();
		return null;
	}

	public RecordKey[] getReadSet() {
		return null;
	}

	public RecordKey[] getWriteSet() {
		return null;
	}

	public double getWeight() {
		return 0;
	}

	public int getProcedureType() {
		return 0;
	}

	public void setSunkPlan(SunkPlan plan) {
	}

	public boolean isReadOnly() {
		return false;
	}
}