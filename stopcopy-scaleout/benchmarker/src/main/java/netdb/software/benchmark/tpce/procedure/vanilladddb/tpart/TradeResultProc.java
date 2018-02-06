package netdb.software.benchmark.tpce.procedure.vanilladddb.tpart;

import java.sql.Connection;
import java.util.HashMap;
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
import org.vanilladb.dd.cache.tpart.TPartCacheMgr;
import org.vanilladb.dd.remote.groupcomm.TupleSet;
import org.vanilladb.dd.schedule.tpart.TPartStoredProcedure;
import org.vanilladb.dd.schedule.tpart.sink.SunkPlan;
import org.vanilladb.dd.server.VanillaDdDb;
import org.vanilladb.dd.sql.RecordKey;
import org.vanilladb.dd.storage.tx.concurrency.ConservativeOrderedCcMgr;

public class TradeResultProc extends TradeResultProcedure implements
		TPartStoredProcedure {
	private long txNum;
	private SunkPlan plan;

	private RecordKey[] readKeys, writeKeys;
	private RecordKey accountKey, customerKey, brokerKey, tradeKey,
			tradeHistoryKey;

	public TradeResultProc(long txNum) {
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
		startTransaction();
		ConservativeOrderedCcMgr ccMgr = (ConservativeOrderedCcMgr) tx
				.concurrencyMgr();

		ccMgr.prepareSp(readKeys, writeKeys);
	}

	@Override
	public boolean isReadOnly() {
		return false;
	}

	@Override
	public void prepare(Object... pars) {
		prepareParameters(pars);

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
			executeSql();
			VanillaDdDb.tPartCacheMgr().finishRead(plan.sinkId(), txNum);

			tx.commit();
		} catch (Exception e) {
			tx.rollback();
			isCommitted = false;
			e.printStackTrace();
		}
		return createResultSet();
	}

	private void executeSql() {
		((ConservativeOrderedCcMgr) tx.concurrencyMgr()).executeSp(readKeys,
				writeKeys);
		int sinkId = plan.sinkId();
		TPartCacheMgr cm = VanillaDdDb.tPartCacheMgr();

		// SELECT ca_name, ca_b_id, ca_c_id FROM customer_account WHERE ca_id =
		// acctId
		Long srcTxNum = plan.getSrcTxNum(accountKey);
		String accountName = (String) cm.read(sinkId, accountKey, srcTxNum, tx)
				.getVal("ca_name").asJavaVal();

		// SELECT c_name FROM customer WHERE c_id = customerId
		srcTxNum = plan.getSrcTxNum(customerKey);
		customerName = (String) cm.read(sinkId, customerKey, srcTxNum, tx)
				.getVal("c_name").asJavaVal();

		// SELECT b_name FROM broker WHERE b_id = brokerId
		srcTxNum = plan.getSrcTxNum(brokerKey);
		String brokerName = (String) cm.read(sinkId, brokerKey, srcTxNum, tx)
				.getVal("b_name").asJavaVal();

		// SELECT trade_infos FROM trade WHERE t_id = tradeId
		srcTxNum = plan.getSrcTxNum(tradeKey);
		// long tradeTime = (Long) cm.read(sinkId, tradeKey, srcTxNum, tx)
		// .getVal("t_dts").asJavaVal();

		// INSERT INTO trade_history(th_t_id, th_dts)
		long currentTime = System.currentTimeMillis();
		Map<String, Constant> fldVals = new HashMap<String, Constant>();
		fldVals = new HashMap<String, Constant>();
		fldVals.put("th_t_id", new BigIntConstant(tradeId));
		fldVals.put("th_dts", new BigIntConstant(currentTime));
		cm.insert(sinkId, tradeHistoryKey, fldVals, tx);

		// update customer_account set ca_bal = ca_bal + tradePrice WHERE
		// ca_id = acctId
		fldVals = new HashMap<String, Constant>();
		fldVals.put("ca_bal", new DoubleConstant(1000));
		fldVals.put("ca_name", new VarcharConstant(accountName));
		cm.update(sinkId, accountKey, fldVals, tx);

		pushToRemote();
	}

	private void pushToRemote() {
		int sinkId = plan.sinkId();
		TPartCacheMgr cm = VanillaDdDb.tPartCacheMgr();
		CachedRecord rec;
		Map<Integer, Set<RecordKey>> pi = plan.getPushingInfo();
		if (pi != null) {
			// read from local storage and send to remote site
			for (Entry<Integer, Set<RecordKey>> entry : pi.entrySet()) {
				int targetServerId = entry.getKey();
				TupleSet rs = new TupleSet(sinkId);
				for (RecordKey k : entry.getValue()) {
					rec = cm.read(sinkId, k, txNum, tx);
					rs.addTuple(k, txNum, rec);
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
}
