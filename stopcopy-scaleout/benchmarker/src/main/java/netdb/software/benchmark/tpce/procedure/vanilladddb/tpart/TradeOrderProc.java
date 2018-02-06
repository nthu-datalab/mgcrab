package netdb.software.benchmark.tpce.procedure.vanilladddb.tpart;

import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import netdb.software.benchmark.tpce.procedure.TradeOrderProcedure;

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

public class TradeOrderProc extends TradeOrderProcedure implements
		TPartStoredProcedure {
	private long txNum;
	private SunkPlan plan;

	private RecordKey[] readKeys, writeKeys;

	public TradeOrderProc(long txNum) {
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

	private RecordKey accountKey, customerKey, brokerKey, securityKey,
			lastTradeKey, tradeTypeKey, tradeKey, tradeHistoryKey;

	@Override
	public void prepare(Object... pars) {
		prepareParameters(pars);

		readKeys = new RecordKey[6];
		writeKeys = new RecordKey[2];

		// account
		Map<String, Constant> keyEntryMap = new HashMap<String, Constant>();
		keyEntryMap.put("ca_id", new BigIntConstant(acctId));
		RecordKey key = new RecordKey("customer_account", keyEntryMap);
		accountKey = key;
		readKeys[0] = key;

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

		// security
		keyEntryMap = new HashMap<String, Constant>();
		keyEntryMap.put("s_symb", new VarcharConstant(sSymb));
		key = new RecordKey("security", keyEntryMap);
		securityKey = key;
		readKeys[3] = key;

		// last trade
		keyEntryMap = new HashMap<String, Constant>();
		keyEntryMap.put("lt_s_symb", new VarcharConstant(sSymb));
		key = new RecordKey("last_trade", keyEntryMap);
		lastTradeKey = key;
		readKeys[4] = key;

		// trade type
		keyEntryMap = new HashMap<String, Constant>();
		keyEntryMap.put("tt_id", new VarcharConstant(tradeTypeId));
		key = new RecordKey("trade_type", keyEntryMap);
		tradeTypeKey = key;
		readKeys[5] = key;

		// insert new trade
		keyEntryMap = new HashMap<String, Constant>();
		keyEntryMap.put("t_id", new BigIntConstant(tradeId));
		key = new RecordKey("trade", keyEntryMap);
		tradeKey = key;
		writeKeys[0] = key;

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

		// System.out.println("cid: " + customerId + ", ca_id: " + acctId
		// + ", b_id: " + brokerId);

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

		// SELECT s_co_id, s_name FROM security WHERE s_symb = sSymb
		srcTxNum = plan.getSrcTxNum(securityKey);
		long companyId = (Long) cm.read(sinkId, securityKey, srcTxNum, tx)
				.getVal("s_co_id").asJavaVal();
		String securityName = (String) cm
				.read(sinkId, securityKey, srcTxNum, tx).getVal("s_name")
				.asJavaVal();

		// SELECT lt_price FROM last_trade WHERE lt_s_symb = sSymb
		srcTxNum = plan.getSrcTxNum(lastTradeKey);
		Constant marketPriceCon = cm.read(sinkId, lastTradeKey, srcTxNum, tx)
				.getVal("lt_price");
		marketPrice = (Double) marketPriceCon.asJavaVal();

		// SELECT tt_is_mrkt, tt_is_sell FROM trade_type WHERE tt_id =
		// tradeTypeId
		srcTxNum = plan.getSrcTxNum(tradeTypeKey);
		Integer typeIsMarket = (Integer) cm
				.read(sinkId, tradeTypeKey, srcTxNum, tx).getVal("tt_is_mrkt")
				.asJavaVal();
		Integer typeIsSell = (Integer) cm
				.read(sinkId, tradeTypeKey, srcTxNum, tx).getVal("tt_is_sell")
				.asJavaVal();

		// INSERT INTO trade(t_id, t_dts, t_tt_id, t_s_symb, t_qty, t_bid_price,
		// t_ca_id, t_trade_price) VALUES()
		Map<String, Constant> fldVals = new HashMap<String, Constant>();

		tradePrice = requestedPrice;
		long currentTime = System.currentTimeMillis();
		fldVals.put("t_id", new BigIntConstant(tradeId));
		fldVals.put("t_dts", new BigIntConstant(currentTime));
		fldVals.put("t_tt_id", new VarcharConstant(tradeTypeId));
		fldVals.put("t_s_symb", new VarcharConstant(sSymb));
		fldVals.put("t_qty", new BigIntConstant(tradeQty));
		fldVals.put("t_bid_price", marketPriceCon);
		fldVals.put("t_ca_id", new BigIntConstant(acctId));
		fldVals.put("t_trade_price", new DoubleConstant(tradePrice));
		cm.insert(sinkId, tradeKey, fldVals, tx);

		// INSERT INTO trade_history(th_t_id, th_dts)
		fldVals = new HashMap<String, Constant>();
		fldVals.put("th_t_id", new BigIntConstant(tradeId));
		fldVals.put("th_dts", new BigIntConstant(currentTime));
		cm.insert(sinkId, tradeHistoryKey, fldVals, tx);

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
		return 8;
	}

	@Override
	public int getProcedureType() {
		return TPartStoredProcedure.KEY_ACCESS;
	}
}
