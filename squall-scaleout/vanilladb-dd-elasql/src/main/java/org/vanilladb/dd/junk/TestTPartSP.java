package org.vanilladb.dd.junk;

import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.DoubleConstant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.sql.storedprocedure.SpResultRecord;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.dd.cache.CachedRecord;
import org.vanilladb.dd.cache.tpart.NewTPartCacheMgr;
import org.vanilladb.dd.remote.groupcomm.TupleSet;
import org.vanilladb.dd.schedule.tpart.TPartStoredProcedure;
import org.vanilladb.dd.schedule.tpart.sink.PushInfo;
import org.vanilladb.dd.schedule.tpart.sink.SunkPlan;
import org.vanilladb.dd.server.VanillaDdDb;
import org.vanilladb.dd.sql.RecordKey;
import org.vanilladb.dd.storage.tx.concurrency.ConservativeOrderedCcMgr;

public class TestTPartSP implements TPartStoredProcedure {
	private long txNum;
	private Transaction tx;
	private SunkPlan plan;
	private RecordKey[] keys = new RecordKey[2];
	private boolean isCommitted;
	private RecordKey[] localWriteBackKeys;

	public TestTPartSP(long txNum) {
		this.txNum = txNum;
	}

	public void requestConservativeLocks() {
		startTransaction();
		ConservativeOrderedCcMgr ccMgr = (ConservativeOrderedCcMgr) tx
				.concurrencyMgr();
		localWriteBackKeys = plan.getLocalWriteBackInfo().toArray(
				new RecordKey[0]);
		if (plan.isLocalTask()) {
			ccMgr.prepareSp(keys, keys);
			ccMgr.prepareWriteBack(localWriteBackKeys);
		} else if (plan.hasLocalWriteBack()) {
			ccMgr.prepareSp(null, localWriteBackKeys);
			ccMgr.prepareWriteBack(localWriteBackKeys);
		}

	}

	public void prepare(Object... pars) {
		Constant widCon = new IntegerConstant(1);
		Constant didCon = new IntegerConstant(3);

		Map<String, Constant> keyEntryMap = new HashMap<String, Constant>();
		keyEntryMap.put("w_id", widCon);
		keys[0] = new RecordKey("warehouse", keyEntryMap);

		keyEntryMap = new HashMap<String, Constant>();
		keyEntryMap.put("d_w_id", widCon);
		keyEntryMap.put("d_id", didCon);
		keys[1] = new RecordKey("district", keyEntryMap);
	}

	public boolean isReadOnly() {
		return false;
	}

	private Transaction startTransaction() {
		if (tx == null)
			tx = VanillaDdDb.txMgr().transaction(
					Connection.TRANSACTION_SERIALIZABLE, false, txNum);
		return tx;
	}

	public SpResultSet execute() {
		startTransaction();
		try {
			executeQuery();

			tx.commit();
			isCommitted = true;
		} catch (Exception e) {
			tx.rollback();
			isCommitted = false;
			e.printStackTrace();
		}
		return createResultSet();
	}

	public RecordKey[] getReadSet() {
		return keys;
	}

	public RecordKey[] getWriteSet() {
		return keys;
	}

	public void setSunkPlan(SunkPlan plan) {
		this.plan = plan;
	}

	public double getWeight() {
		return 3;
	}

	private double wTax, dTax;

	private void executeQuery() {
		ConservativeOrderedCcMgr ccMgr = (ConservativeOrderedCcMgr) tx
				.concurrencyMgr();
		NewTPartCacheMgr cacheMgr = VanillaDdDb.tPartCacheMgr();
		long txNum = tx.getTransactionNumber();

		if (plan.isLocalTask()) {
			ccMgr.executeSp(keys, keys);

			/*********************** local read write *********************/

			cacheMgr.read(keys[0], plan.getReadSrcTxNum(keys[0]), txNum);

			/********** release slock and xlock ************/
			ccMgr.finishSp(keys, keys);

			/********** send response to client ************/
			// TODO: send the resp. early

			/********** push to remote site ************/
			Map<Integer, Set<PushInfo>> pi = plan.getPushingInfo();
			if (pi != null) {
				// read from local storage and send to remote site

				for (Entry<Integer, Set<PushInfo>> entry : pi.entrySet()) {
					int targetServerId = entry.getKey();
					TupleSet rs = new TupleSet(-1);
					for (PushInfo pushInfo : entry.getValue()) {
						rec = cm.read(pushInfo.getRecord(), txNum, tx, true);
						rs.addTuple(pushInfo.getRecord(), txNum,
								pushInfo.getDestTxNum(), rec);
					}
					VanillaDdDb.connectionMgr()
							.pushTupleSet(targetServerId, rs);
				}
			}

			/********** local write back ************/
			ccMgr.executeWriteBack(localWriteBackKeys);
			for (RecordKey wk : localWriteBackKeys) {
				CachedRecord rec = cacheMgr.read(wk, txNum,
						-VanillaDdDb.serverId());
				cacheMgr.writeBack(key, src, sinkProcessId, tx);
			}
		} else if (plan.hasLocalWriteBack()) {
			/********** local write back ************/
			ccMgr.executeSp(null, localWriteBackKeys);
			ccMgr.executeWriteBack(localWriteBackKeys);
			for (RecordKey wk : localWriteBackKeys) {
				CachedRecord rec = cacheMgr.read(wk, txNum,
						-VanillaDdDb.serverId());
				cacheMgr.writeBack(key, src, sinkProcessId, tx);
			}
		}

	}

	private SpResultSet createResultSet() {
		Schema sch = new Schema();
		Type statusType = Type.VARCHAR(10);
		sch.addField("status", statusType);
		sch.addField("w_tax", Type.DOUBLE);

		SpResultRecord rec;
		rec = new SpResultRecord();
		String status = isCommitted ? "committed" : "abort";
		rec.setVal("status", new VarcharConstant(status, statusType));
		rec.setVal("w_tax", new DoubleConstant(wTax));

		return new SpResultSet(sch, rec);
	}

	public int getProcedureType() {
		return TPartStoredProcedure.KEY_ACCESS;
	}
}
