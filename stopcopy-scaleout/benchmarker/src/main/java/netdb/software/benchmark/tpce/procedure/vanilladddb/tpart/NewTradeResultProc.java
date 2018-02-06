package netdb.software.benchmark.tpce.procedure.vanilladddb.tpart;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import netdb.software.benchmark.tpce.procedure.TradeResultProcedure;

import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.sql.BigIntConstant;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.DoubleConstant;
import org.vanilladb.core.sql.VarcharConstant;
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

public class NewTradeResultProc extends TradeResultProcedure implements
		TPartStoredProcedure {
	private long txNum;
	private SunkPlan plan;

	private RecordKey[] readKeys, writeKeys;
	private RecordKey accountKey, customerKey, brokerKey, tradeKey,
			tradeHistoryKey;
	private RecordKey[] localWriteBackKeys;
	private List<CachedEntryKey> entryKeys = new ArrayList<CachedEntryKey>();

	public NewTradeResultProc(long txNum) {
		this.txNum = txNum;
	}

	public Transaction startTransaction() {
		if (tx == null)
			tx = VanillaDdDb.txMgr().transaction(
					Connection.TRANSACTION_SERIALIZABLE, false, txNum);
		return tx;
	}

	@Override
	public void requestConservativeLocks() {
		// System.out.println("ntrp rCL: " + txNum);
		// ConservativeOrderedCcMgr ccMgr = (ConservativeOrderedCcMgr) tx
		// .concurrencyMgr();
		// localWriteBackKeys = plan.getLocalWriteBackInfo().toArray(
		// new RecordKey[0]);
		//
		// if (plan.isLocalTask()) {
		// ccMgr.prepareSp(readKeys, writeKeys);
		// ccMgr.prepareWriteBack(localWriteBackKeys);
		// } else if (plan.hasLocalWriteBack()) {
		// ccMgr.prepareSp(null, localWriteBackKeys);
		// ccMgr.prepareWriteBack(localWriteBackKeys);
		// }
	}

	@Override
	public boolean isReadOnly() {
		return false;
	}

	@Override
	public void prepare(Object... pars) {
		// System.out.println("prepare new trp: " + txNum);
		prepareParameters(pars);

		startTransaction();
		readKeys = new RecordKey[4];
		writeKeys = new RecordKey[2];

		// account
		Map<String, Constant> keyEntryMap = new HashMap<String, Constant>();
		keyEntryMap.put("ca_id", new BigIntConstant(acctId));
		RecordKey key = new RecordKey("customer_account", keyEntryMap);
		accountKey = key;
		readKeys[0] = key;
		writeKeys[0] = key;

		// customer
		keyEntryMap = new HashMap<String, Constant>();
		keyEntryMap.put("c_id", new BigIntConstant(customerId));
		key = new RecordKey("customer", keyEntryMap);
		customerKey = key;
		readKeys[1] = key;

		// broker
		keyEntryMap = new HashMap<String, Constant>();
		keyEntryMap.put("b_id", new BigIntConstant(brokerId));
		key = new RecordKey("broker", keyEntryMap);
		brokerKey = key;
		readKeys[2] = key;

		// trade
		keyEntryMap = new HashMap<String, Constant>();
		keyEntryMap.put("t_id", new BigIntConstant(tradeId));
		key = new RecordKey("trade", keyEntryMap);
		tradeKey = key;
		readKeys[3] = key;

		// insert new history
		keyEntryMap = new HashMap<String, Constant>();
		keyEntryMap.put("th_t_id", new BigIntConstant(tradeId));
		key = new RecordKey("trade_history", keyEntryMap);
		tradeHistoryKey = key;
		writeKeys[1] = key;

	}

	@Override
	public SpResultSet execute() {
		try {
			executeQuery();
			doUncache();

			tx.commit();
		} catch (Exception e) {
			tx.rollback();
			isCommitted = false;
			e.printStackTrace();
		}
		return createResultSet();
	}

	private void doUncache() {
		for (CachedEntryKey key : entryKeys) {
			((NewTPartCacheMgr) VanillaDdDb.cacheMgr()).uncache(
					key.getRecordKey(), key.getSource(), key.getDestination());
		}
	}

	private void executeQuery() {
		int sinkId = plan.sinkProcessId();
		localWriteBackKeys = plan.getLocalWriteBackInfo().toArray(
				new RecordKey[0]);
		NewTPartCacheMgr cm = (NewTPartCacheMgr) VanillaDdDb.cacheMgr();

		if (plan.isLocalTask()) {
			for (RecordKey k : plan.getSinkReadingInfo()) {
				cm.createCacheRecordFromSink(k, plan.sinkProcessId(), tx, txNum);
			}

			// SELECT ca_name, ca_b_id, ca_c_id FROM customer_account WHERE
			// ca_id =
			// acctId
			Long srcTxNum = plan.getReadSrcTxNum(accountKey);
			CachedRecord customerAccount = cm.read(accountKey, srcTxNum, txNum);
			String accountName = (String) customerAccount.getVal("ca_name")
					.asJavaVal();
			entryKeys.add(new CachedEntryKey(accountKey, srcTxNum, txNum));

			// SELECT c_name FROM customer WHERE c_id = customerId
			srcTxNum = plan.getReadSrcTxNum(customerKey);
			customerName = (String) cm.read(customerKey, srcTxNum, txNum)
					.getVal("c_name").asJavaVal();
			entryKeys.add(new CachedEntryKey(customerKey, srcTxNum, txNum));

			// SELECT b_name FROM broker WHERE b_id = brokerId
			srcTxNum = plan.getReadSrcTxNum(brokerKey);
			String brokerName = (String) cm.read(brokerKey, srcTxNum, txNum)
					.getVal("b_name").asJavaVal();
			entryKeys.add(new CachedEntryKey(brokerKey, srcTxNum, txNum));

			// SELECT trade_infos FROM trade WHERE t_id = tradeId
			srcTxNum = plan.getReadSrcTxNum(tradeKey);
			long tradeTime = (Long) cm.read(tradeKey, srcTxNum, txNum)
					.getVal("t_dts").asJavaVal();

			// INSERT INTO trade_history(th_t_id, th_dts)
			long currentTime = System.currentTimeMillis();
			Map<String, Constant> fldVals = new HashMap<String, Constant>();
			fldVals = new HashMap<String, Constant>();
			fldVals.put("th_t_id", new BigIntConstant(tradeId));
			fldVals.put("th_dts", new BigIntConstant(currentTime));
			cm.insert(tradeHistoryKey, fldVals, tx,
					plan.getWritingDestOfRecord(tradeHistoryKey));

			// update customer_account set ca_bal = ca_bal + tradePrice WHERE
			// ca_id = acctId
			fldVals = new HashMap<String, Constant>(
					customerAccount.getFldValMap());
			fldVals.put("ca_id", accountKey.getKeyVal("ca_id"));
			fldVals.put("ca_bal", new DoubleConstant(1000));
			fldVals.put("ca_name", new VarcharConstant(accountName));
			cm.update(accountKey, fldVals, tx,
					plan.getWritingDestOfRecord(accountKey));

			pushToRemote();

			/********** local write back ************/
			for (RecordKey wk : localWriteBackKeys) {
				// CachedRecord rec = cm.read(wk, txNum, NewTPartCacheMgr
				// .getPartitionTxnId(VanillaDdDb.serverId()));
				cm.writeBack(wk, txNum, sinkId, tx);
			}
		} else {
			/********** local write back ************/
			if (plan.hasSinkPush()) {
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
						// if (pushInfo.getRecord().getKeyFldSet()
						// .contains("ca_id"))
						// System.out.println("Push to remote: "
						// + pushInfo.getRecord() + ", dest: "
						// + pushInfo.getDestTxNum()
						// + ", targetServer: " + targetServerId);
						rs.addTuple(pushInfo.getRecord(), sinkTxnNum,
								pushInfo.getDestTxNum(), rec);
					}
					VanillaDdDb.connectionMgr()
							.pushTupleSet(targetServerId, rs);
				}
			}

			if (plan.hasLocalWriteBack()) {
				/********** local write back ************/

				for (RecordKey wk : localWriteBackKeys) {

					cm.writeBack(wk, txNum, sinkId, tx);
				}
			}
		}
	}

	private void pushToRemote() {
		int sinkId = plan.sinkProcessId();
		NewTPartCacheMgr cm = (NewTPartCacheMgr) VanillaDdDb.cacheMgr();
		CachedRecord rec;
		Map<Integer, Set<PushInfo>> pi = plan.getPushingInfo();
		if (pi != null) {
			// read from local storage and send to remote site
			for (Entry<Integer, Set<PushInfo>> entry : pi.entrySet()) {
				int targetServerId = entry.getKey();
				TupleSet rs = new TupleSet(sinkId);
				for (PushInfo pushInfo : entry.getValue()) {
					// XXX not sure if it's right
					// System.out.println("Push to remote: "
					// + pushInfo.getRecord() + ", dest: "
					// + pushInfo.getDestTxNum() + ", targetServer: "
					// + targetServerId);
					rec = cm.read(pushInfo.getRecord(), txNum,
							pushInfo.getDestTxNum());
					rs.addTuple(pushInfo.getRecord(), txNum,
							pushInfo.getDestTxNum(), rec);
				}
				VanillaDdDb.connectionMgr().pushTupleSet(targetServerId, rs);
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
		return 6;
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
	public SunkPlan getSunkPlan() {
		return plan;
	}
}
