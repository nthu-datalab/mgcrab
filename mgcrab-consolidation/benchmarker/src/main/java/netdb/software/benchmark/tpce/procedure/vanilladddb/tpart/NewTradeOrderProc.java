package netdb.software.benchmark.tpce.procedure.vanilladddb.tpart;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
import org.vanilladb.dd.cache.tpart.CachedEntryKey;
import org.vanilladb.dd.cache.tpart.NewTPartCacheMgr;
import org.vanilladb.dd.remote.groupcomm.TupleSet;
import org.vanilladb.dd.schedule.tpart.TPartStoredProcedure;
import org.vanilladb.dd.schedule.tpart.sink.PushInfo;
import org.vanilladb.dd.schedule.tpart.sink.SunkPlan;
import org.vanilladb.dd.server.VanillaDdDb;
import org.vanilladb.dd.sql.RecordKey;
import org.vanilladb.dd.storage.tx.concurrency.ConservativeOrderedCcMgr;

public class NewTradeOrderProc extends TradeOrderProcedure implements
		TPartStoredProcedure {
	private long txNum;
	private SunkPlan plan;

	private RecordKey[] readKeys, writeKeys;
	private List<CachedEntryKey> entryKeys = new ArrayList<CachedEntryKey>();
	private RecordKey[] localWriteBackKeys;

	public NewTradeOrderProc(long txNum) {
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

		// ConservativeOrderedCcMgr ccMgr = (ConservativeOrderedCcMgr) tx
		// .concurrencyMgr();
		//
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

	private RecordKey accountKey, customerKey, brokerKey, securityKey,
			lastTradeKey, tradeTypeKey, tradeKey, tradeHistoryKey;

	@Override
	public void prepare(Object... pars) {
		prepareParameters(pars);

		startTransaction();
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

			executeQuery();
			doUncache();

			tx.commit();
			isCommitted = true;
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
		ConservativeOrderedCcMgr ccMgr = (ConservativeOrderedCcMgr) tx
				.concurrencyMgr();
		int sinkId = plan.sinkProcessId();
		localWriteBackKeys = plan.getLocalWriteBackInfo().toArray(
				new RecordKey[0]);
		NewTPartCacheMgr cm = (NewTPartCacheMgr) VanillaDdDb.cacheMgr();

		// System.out.println("Tx " + txNum + " on node " +
		// VanillaDdDb.serverId()
		// + " is local " + plan.isLocalTask());
		if (plan.isLocalTask()) {
			for (RecordKey k : plan.getSinkReadingInfo()) {
				cm.createCacheRecordFromSink(k, plan.sinkProcessId(), tx, txNum);
			}

			// SELECT ca_name, ca_b_id, ca_c_id FROM customer_account WHERE
			// ca_id = acctId
			Long srcTxNum = plan.getReadSrcTxNum(accountKey);
			String accountName = (String) cm.read(accountKey, srcTxNum, txNum)
					.getVal("ca_name").asJavaVal();
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

			// SELECT s_co_id, s_name FROM security WHERE s_symb = sSymb
			srcTxNum = plan.getReadSrcTxNum(securityKey);
			long companyId = (Long) cm.read(securityKey, srcTxNum, txNum)
					.getVal("s_co_id").asJavaVal();
			String securityName = (String) cm
					.read(securityKey, srcTxNum, txNum).getVal("s_name")
					.asJavaVal();
			entryKeys.add(new CachedEntryKey(securityKey, srcTxNum, txNum));

			// SELECT lt_price FROM last_trade WHERE lt_s_symb = sSymb
			srcTxNum = plan.getReadSrcTxNum(lastTradeKey);
			Constant marketPriceCon = cm.read(lastTradeKey, srcTxNum, txNum)
					.getVal("lt_price");
			marketPrice = (Double) marketPriceCon.asJavaVal();
			entryKeys.add(new CachedEntryKey(lastTradeKey, srcTxNum, txNum));

			// SELECT tt_is_mrkt, tt_is_sell FROM trade_type WHERE tt_id =
			// tradeTypeId
			srcTxNum = plan.getReadSrcTxNum(tradeTypeKey);
			Integer typeIsMarket = (Integer) cm
					.read(tradeTypeKey, srcTxNum, txNum).getVal("tt_is_mrkt")
					.asJavaVal();
			Integer typeIsSell = (Integer) cm
					.read(tradeTypeKey, srcTxNum, txNum).getVal("tt_is_sell")
					.asJavaVal();
			entryKeys.add(new CachedEntryKey(tradeTypeKey, srcTxNum, txNum));
			// INSERT INTO trade(t_id, t_dts, t_tt_id, t_s_symb, t_qty,
			// t_bid_price,
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
			cm.insert(tradeKey, fldVals, tx,
					plan.getWritingDestOfRecord(tradeKey));

			// INSERT INTO trade_history(th_t_id, th_dts)
			fldVals = new HashMap<String, Constant>();
			fldVals.put("th_t_id", new BigIntConstant(tradeId));
			fldVals.put("th_dts", new BigIntConstant(currentTime));
			cm.insert(tradeHistoryKey, fldVals, tx,
					plan.getWritingDestOfRecord(tradeHistoryKey));

			// //////////////////
			// Release locks
			// //////////////////

			// //////////////////
			// Push To Remote ///
			// //////////////////

			CachedRecord rec;
			Map<Integer, Set<PushInfo>> pi = plan.getPushingInfo();
			if (pi != null) {
				// read from local storage and send to remote site
				for (Entry<Integer, Set<PushInfo>> entry : pi.entrySet()) {
					int targetServerId = entry.getKey();
					TupleSet rs = new TupleSet(sinkId);
					for (PushInfo pushInfo : entry.getValue()) {
						// XXX not sure if it's right

						// System.out.println("Push Remote Key:"
						// + pushInfo.getRecord() + ",src:" + txNum
						// + ",dest:" + pushInfo.getDestTxNum());

						rec = cm.read(pushInfo.getRecord(), txNum,
								pushInfo.getDestTxNum());

						rs.addTuple(pushInfo.getRecord(), txNum,
								pushInfo.getDestTxNum(), rec);
					}
					VanillaDdDb.connectionMgr()
							.pushTupleSet(targetServerId, rs);
				}
			}

			/********** local write back ************/

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
						// System.out.println("@@ Push to remote: "
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
		return 8;
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
