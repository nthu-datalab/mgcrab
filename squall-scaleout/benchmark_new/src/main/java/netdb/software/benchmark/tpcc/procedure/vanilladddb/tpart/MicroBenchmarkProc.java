package netdb.software.benchmark.tpcc.procedure.vanilladddb.tpart;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import netdb.software.benchmark.tpcc.procedure.MicroBenchmarkProcParamHelper;

import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.DoubleConstant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.dd.cache.CachedRecord;
import org.vanilladb.dd.cache.tpart.CachedEntryKey;
import org.vanilladb.dd.cache.tpart.NewTPartCacheMgr;
import org.vanilladb.dd.remote.groupcomm.TupleSet;
import org.vanilladb.dd.schedule.tpart.TPartStoredProcedure;
import org.vanilladb.dd.schedule.tpart.sink.PushInfo;
import org.vanilladb.dd.schedule.tpart.sink.SunkPlan;
import org.vanilladb.dd.server.VanillaDdDb;
import org.vanilladb.dd.sql.RecordKey;
import org.vanilladb.dd.storage.tx.concurrency.ConservativeOrderedCcMgr;

public class MicroBenchmarkProc implements TPartStoredProcedure {

	private long txNum;
	private Transaction tx;
	private SunkPlan plan;

	private List<RecordKey> readKeyList = new ArrayList<RecordKey>();
	private Map<RecordKey, Constant> writingMap = new HashMap<RecordKey, Constant>();
	private List<CachedEntryKey> entryKeys = new ArrayList<CachedEntryKey>();

	private NewTPartCacheMgr cm = (NewTPartCacheMgr) VanillaDdDb.cacheMgr();

	private RecordKey[] readKeys, writeKeys;
	private RecordKey[] localWriteBackKeys;

	private RecordKey[] localReadKeys;

	private MicroBenchmarkProcParamHelper paramHelper = new MicroBenchmarkProcParamHelper();

	public MicroBenchmarkProc(long txNum) {
		this.txNum = txNum;
	}

	public Transaction startTransaction() {
		if (tx == null)
			tx = VanillaDdDb.txMgr().transaction(
					Connection.TRANSACTION_SERIALIZABLE,
					paramHelper.isReadOnly(), txNum);
		return tx;
	}

	@Override
	public boolean isReadOnly() {
		return (paramHelper.getWriteCount() == 0);
	}

	@Override
	public void prepare(Object... pars) {
		paramHelper.prepareParameters(pars);

		startTransaction();

		int readCount = paramHelper.getReadCount();
		int writeCount = paramHelper.getWriteCount();
		int[] readItemId = paramHelper.getReadItemId();
		int[] writeItemId = paramHelper.getWriteItemId();
		double[] newItemPrice = paramHelper.getNewItemPrice();

		readKeys = new RecordKey[readCount];
		writeKeys = new RecordKey[writeCount];

		RecordKey key;
		Map<String, Constant> keyEntryMap;
		List<RecordKey> localReadKeyList = new ArrayList<RecordKey>();
		// readings
		for (int i = 0; i < readCount; i++) {
			keyEntryMap = new HashMap<String, Constant>();
			keyEntryMap.put("i_id", new IntegerConstant(readItemId[i]));
			key = new RecordKey("item", keyEntryMap);
			readKeyList.add(key);
			readKeys[i] = key;
			if (VanillaDdDb.partitionMetaMgr().getPartition(key) == VanillaDdDb
					.serverId()) {
				localReadKeyList.add(key);
			}
		}
		localReadKeys = localReadKeyList.toArray(new RecordKey[0]);

		// writings
		for (int i = 0; i < writeCount; i++) {
			keyEntryMap = new HashMap<String, Constant>();
			keyEntryMap.put("i_id", new IntegerConstant(writeItemId[i]));
			key = new RecordKey("item", keyEntryMap);
			Constant v = new DoubleConstant(newItemPrice[i]);
			writingMap.put(key, v);
			writeKeys[i] = key;
		}
	}

	@Override
	public SpResultSet execute() {
		try {
			executeQuery();
			doUncache();
			tx.commit();
			paramHelper.setCommitted(true);
		} catch (Exception e) {

			tx.rollback();
			paramHelper.setCommitted(false);
		}
		return paramHelper.createResultSet();
	}

	private void doUncache() {
		for (CachedEntryKey key : entryKeys) {
			cm.uncache(key.getRecordKey(), key.getSource(),
					key.getDestination());
		}
	}

	protected void executeQuery() {
		ConservativeOrderedCcMgr ccMgr = (ConservativeOrderedCcMgr) tx
				.concurrencyMgr();
		int sinkId = plan.sinkProcessId();
		localWriteBackKeys = plan.getLocalWriteBackInfo().toArray(
				new RecordKey[0]);

		// ccMgr.executeSp(localReadKeys, null);

		if (plan.isLocalTask()) {

			for (RecordKey k : plan.getSinkReadingInfo()) {
				cm.createCacheRecordFromSink(k, plan.sinkProcessId(), tx, txNum);
			}

			String name;
			double price;
			CachedRecord rec;
			for (RecordKey k : readKeyList) {
				long sourceTxNum = plan.getReadSrcTxNum(k);

				long src = sourceTxNum;
				long dest = txNum;
				long time = System.currentTimeMillis();
				rec = cm.read(k, sourceTxNum, txNum);
				if (src > 0 && dest > 0 && src != dest) {
					long tmp = System.currentTimeMillis() - time;
					// System.out.println((dest - src) + " " + tmp);
				}
				name = (String) rec.getVal("i_name").asJavaVal();
				price = (Double) rec.getVal("i_price").asJavaVal();
				// int paid = 0;
				// while (paid < price)
				// paid += 1;
				entryKeys.add(new CachedEntryKey(k, sourceTxNum, txNum));
			}

			Map<String, Constant> fldVals;
			// update the items
			for (Entry<RecordKey, Constant> e : writingMap.entrySet()) {
				// doesn't allow partial write
				long sourceTxNum = plan.getReadSrcTxNum(e.getKey());
				rec = cm.read(e.getKey(), sourceTxNum, txNum);
				fldVals = new HashMap<String, Constant>(rec.getFldValMap());
				fldVals.put("i_price", e.getValue());

				cm.update(e.getKey(), fldVals, tx,
						plan.getWritingDestOfRecord(e.getKey()));
			}

			// //////////////////
			// Release locks
			// //////////////////

			// ccMgr.finishSp(readKeys, writeKeys);

			// //////////////////
			// Push To Remote ///
			// //////////////////

			Map<Integer, Set<PushInfo>> pi = plan.getPushingInfo();
			if (pi != null) {
				// read from local storage and send to remote site
				for (Entry<Integer, Set<PushInfo>> entry : pi.entrySet()) {
					int targetServerId = entry.getKey();
					TupleSet rs = new TupleSet(sinkId);
					for (PushInfo pushInfo : entry.getValue()) {
						rec = cm.read(pushInfo.getRecord(), txNum,
								pushInfo.getDestTxNum());
						entryKeys.add(new CachedEntryKey(pushInfo.getRecord(),
								txNum, pushInfo.getDestTxNum()));

						rs.addTuple(pushInfo.getRecord(), txNum,
								pushInfo.getDestTxNum(), rec);
					}
					VanillaDdDb.connectionMgr()
							.pushTupleSet(targetServerId, rs);
				}
			}
			/********** local write back ************/

			// ccMgr.executeWriteBack(localWriteBackKeys);
			for (RecordKey wk : localWriteBackKeys) {
				cm.writeBack(wk, txNum, sinkId, tx);
			}
		} else {
			if (plan.hasSinkPush()) {
				// ccMgr.executeSp(localReadKeys, null);
				for (Entry<Integer, Set<PushInfo>> entry : plan
						.getSinkPushingInfo().entrySet()) {
					int targetServerId = entry.getKey();
					TupleSet rs = new TupleSet(sinkId);
					for (PushInfo pushInfo : entry.getValue()) {
						long sinkTxnNum = NewTPartCacheMgr
								.getPartitionTxnId(VanillaDdDb.serverId());
						CachedRecord rec = cm.readFromSink(
								pushInfo.getRecord(), sinkId, tx);
						// TODO deal with null value record
						rec.setSrcTxNum(sinkTxnNum);
						// System.out.println("Push to remote: "
						// + pushInfo.getRecord() + ", dest: "
						// + pushInfo.getDestTxNum());
						rs.addTuple(pushInfo.getRecord(), sinkTxnNum,
								pushInfo.getDestTxNum(), rec);
					}
					VanillaDdDb.connectionMgr()
							.pushTupleSet(targetServerId, rs);
				}
			}

			if (plan.hasLocalWriteBack()) {

				/********** local write back ************/
				// ccMgr.executeWriteBack(localWriteBackKeys);
				for (RecordKey wk : localWriteBackKeys) {
					cm.writeBack(wk, txNum, sinkId, tx);
				}
			}
		}

	}

	@Override
	public RecordKey[] getReadSet() {
		return readKeys;
	}

	@Override
	public RecordKey[] getWriteSet() {
		return writeKeys;
	}

	@Override
	public void setSunkPlan(SunkPlan plan) {
		this.plan = plan;
	}

	@Override
	public double getWeight() {
		return paramHelper.getWriteCount() + paramHelper.getReadCount();
	}

	@Override
	public int getProcedureType() {
		return TPartStoredProcedure.KEY_ACCESS;
	}

	@Override
	public boolean isMaster() {
		return plan.isLocalTask();
	}

	@Override
	public void requestConservativeLocks() {
		ConservativeOrderedCcMgr ccMgr = (ConservativeOrderedCcMgr) tx
				.concurrencyMgr();
		// ccMgr.prepareSp(localReadKeys, null);
	}

	@Override
	public SunkPlan getSunkPlan() {
		return plan;
	}
}
