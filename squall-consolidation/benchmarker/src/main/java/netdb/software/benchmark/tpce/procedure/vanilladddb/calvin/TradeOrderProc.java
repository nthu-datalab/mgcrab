package netdb.software.benchmark.tpce.procedure.vanilladddb.calvin;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import netdb.software.benchmark.tpce.procedure.TradeOrderParamHelper;

import org.vanilladb.core.sql.BigIntConstant;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.DoubleConstant;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.dd.cache.CachedRecord;
import org.vanilladb.dd.cache.calvin.CalvinCacheMgr;
import org.vanilladb.dd.remote.groupcomm.TupleSet;
import org.vanilladb.dd.schedule.calvin.CalvinStoredProcedure;
import org.vanilladb.dd.server.VanillaDdDb;
import org.vanilladb.dd.sql.RecordKey;
import org.vanilladb.dd.storage.metadata.PartitionMetaMgr;

public class TradeOrderProc extends
		CalvinStoredProcedure<TradeOrderParamHelper> {

	public TradeOrderProc(long txNum) {
		super(txNum, new TradeOrderParamHelper());
	}

	private RecordKey accountKey, customerKey, brokerKey, securityKey,
			lastTradeKey, tradeTypeKey, tradeKey, tradeHistoryKey;

	// Remote Info
	private int masterNodeId;

	@Override
	public void prepareKeys() {

		int myId = VanillaDdDb.serverId();
		int readPerNode[] = new int[PartitionMetaMgr.NUM_PARTITIONS];
		int writePerNode[] = new int[PartitionMetaMgr.NUM_PARTITIONS];
		PartitionMetaMgr parMgr = VanillaDdDb.partitionMetaMgr();

		// initialize arrays
		for (int i = 0; i < readPerNode.length; i++)
			readPerNode[i] = 0;
		for (int i = 0; i < writePerNode.length; i++)
			writePerNode[i] = 0;

		/***************** Construct Record Key *******************/
		// account
		Map<String, Constant> keyEntryMap = new HashMap<String, Constant>();
		keyEntryMap.put("ca_id", new BigIntConstant(paramHelper.getAcctId()));
		accountKey = new RecordKey("customer_account", keyEntryMap);

		// customer
		keyEntryMap = new HashMap<String, Constant>();
		keyEntryMap
				.put("c_id", new BigIntConstant(paramHelper.getCustomerId()));
		customerKey = new RecordKey("customer", keyEntryMap);

		// broker
		keyEntryMap = new HashMap<String, Constant>();
		keyEntryMap.put("b_id", new BigIntConstant(paramHelper.getBrokerId()));
		brokerKey = new RecordKey("broker", keyEntryMap);

		// security
		keyEntryMap = new HashMap<String, Constant>();
		keyEntryMap.put("s_symb", new VarcharConstant(paramHelper.getsSymb()));
		securityKey = new RecordKey("security", keyEntryMap);

		// last trade
		keyEntryMap = new HashMap<String, Constant>();
		keyEntryMap.put("lt_s_symb",
				new VarcharConstant(paramHelper.getsSymb()));
		lastTradeKey = new RecordKey("last_trade", keyEntryMap);

		// trade type
		keyEntryMap = new HashMap<String, Constant>();
		keyEntryMap.put("tt_id",
				new VarcharConstant(paramHelper.getTradeTypeId()));
		tradeTypeKey = new RecordKey("trade_type", keyEntryMap);

		// insert new trade
		keyEntryMap = new HashMap<String, Constant>();
		keyEntryMap.put("t_id", new BigIntConstant(paramHelper.getTradeId()));
		tradeKey = new RecordKey("trade", keyEntryMap);

		// insert new history
		keyEntryMap = new HashMap<String, Constant>();
		keyEntryMap
				.put("th_t_id", new BigIntConstant(paramHelper.getTradeId()));
		tradeHistoryKey = new RecordKey("trade_history", keyEntryMap);

		/***************** Add Keys to local or particiant node *******************/

		// account
		if (parMgr.getPartition(accountKey) == myId) {
			localReadKeys.add(accountKey);
		} else {
			remoteReadKeys.add(accountKey);
			//participantNode.add(parMgr.getPartition(accountKey));

		}

		// customer
		if (parMgr.getPartition(customerKey) == myId) {
			localReadKeys.add(customerKey);
		} else {
			remoteReadKeys.add(customerKey);
			//participantNode.add(parMgr.getPartition(customerKey));
		}

		// broker
		if (parMgr.getPartition(brokerKey) == myId) {
			localReadKeys.add(brokerKey);
		} else {
			remoteReadKeys.add(brokerKey);
			//participantNode.add(parMgr.getPartition(brokerKey));
		}

		// security
		if (parMgr.getPartition(securityKey) == myId) {
			localReadKeys.add(securityKey);
		} else {
			remoteReadKeys.add(securityKey);
			//participantNode.add(parMgr.getPartition(securityKey));
		}

		// last trade
		if (parMgr.getPartition(lastTradeKey) == myId) {
			localReadKeys.add(lastTradeKey);
		} else {
			remoteReadKeys.add(lastTradeKey);
			//participantNode.add(parMgr.getPartition(lastTradeKey));
		}
		
		// trade type
		if (parMgr.getPartition(tradeTypeKey) == myId) {
			localReadKeys.add(tradeTypeKey);
		} else {
			remoteReadKeys.add(tradeTypeKey);
			participantNode.add(parMgr.getPartition(tradeTypeKey));
		}

		// insert new trade
		int targetNodeId = parMgr.getPartition(tradeKey);
		if (targetNodeId == myId) {
			localWriteKeys.add(tradeKey);
		} else {
			participantNode.add(parMgr.getPartition(tradeTypeKey));
		}

		// insert new history
		targetNodeId = parMgr.getPartition(tradeHistoryKey);
		if (targetNodeId == myId) {
			localWriteKeys.add(tradeHistoryKey);
		} else {
			participantNode.add(targetNodeId);
		}

		/**************** Decide master ****************/
		masterNodeId = VanillaDdDb.partitionMetaMgr().getPartition(tradeKey);
		if (masterNodeId == VanillaDdDb.serverId())
			isMaster = true;

	}

	@Override
	public boolean isMasterNode() {
		return isMaster;
	}

	@Override
	public boolean executeSql() {
		CalvinCacheMgr cm = (CalvinCacheMgr) VanillaDdDb.cacheMgr();
		/*********************** local read *********************/
		HashMap<RecordKey, CachedRecord> readings = new HashMap<RecordKey, CachedRecord>();
		for (RecordKey k : localReadKeys) {
			CachedRecord rec = cm.read(k, txNum, tx, true);
			readings.put(k, rec);
		}

		/*************** push readings to remote ****************/
		TupleSet ts = new TupleSet(-1);
		for (Entry<RecordKey, CachedRecord> e : readings.entrySet()) {
			ts.addTuple(e.getKey(), txNum, txNum, e.getValue());
		}
		for (Integer n : participantNode)
			VanillaDdDb.connectionMgr().pushTupleSet(n, ts);

		// active participated
		if (localWriteKeys.size() != 0) {
			/********** perform tx logic and write local ************/
			String accountName = (String) cm
					.read(accountKey, txNum, tx,
							localReadKeys.contains(accountKey))
					.getVal("ca_name").asJavaVal();

			paramHelper.setCustomerName((String) cm
					.read(customerKey, txNum, tx,
							localReadKeys.contains(customerKey))
					.getVal("c_name").asJavaVal());

			String brokerName = (String) cm
					.read(brokerKey, txNum, tx,
							localReadKeys.contains(brokerKey)).getVal("b_name")
					.asJavaVal();

			CachedRecord rec = cm.read(securityKey, txNum, tx,
					localReadKeys.contains(securityKey));
			long companyId = (Long) rec.getVal("s_co_id").asJavaVal();
			String securityName = (String) rec.getVal("s_name").asJavaVal();

			Constant marketPriceCon = cm.read(lastTradeKey, txNum, tx,
					localReadKeys.contains(lastTradeKey)).getVal("lt_price");
			paramHelper.setMarketPrice((Double) marketPriceCon.asJavaVal());

			rec = cm.read(tradeTypeKey, txNum, tx,
					localReadKeys.contains(tradeTypeKey));
			Integer typeIsMarket = (Integer) rec.getVal("tt_is_mrkt")
					.asJavaVal();
			Integer typeIsSell = (Integer) rec.getVal("tt_is_sell").asJavaVal();

			// INSERT INTO trade(t_id, t_dts, t_tt_id, t_s_symb, t_qty,
			// t_bid_price,
			// t_ca_id, t_trade_price) VALUES()
			Map<String, Constant> fldVals = new HashMap<String, Constant>();

			paramHelper.setTradePrice(paramHelper.getRequestedPrice());
			long currentTime = System.currentTimeMillis();
			fldVals.put("t_id", new BigIntConstant(paramHelper.getTradeId()));
			fldVals.put("t_dts", new BigIntConstant(currentTime));
			fldVals.put("t_tt_id",
					new VarcharConstant(paramHelper.getTradeTypeId()));
			fldVals.put("t_s_symb", new VarcharConstant(paramHelper.getsSymb()));
			fldVals.put("t_qty", new BigIntConstant(paramHelper.getTradeQty()));
			fldVals.put("t_bid_price", marketPriceCon);
			fldVals.put("t_ca_id", new BigIntConstant(paramHelper.getAcctId()));
			fldVals.put("t_trade_price",
					new DoubleConstant(paramHelper.getTradePrice()));
			cm.insert(tradeKey, fldVals, tx);
			cm.flushToLocalStorage(tradeKey, txNum, tx);
			cm.remove(tradeKey, txNum);

			// INSERT INTO trade_history(th_t_id, th_dts)
			fldVals = new HashMap<String, Constant>();
			fldVals.put("th_t_id", new BigIntConstant(paramHelper.getTradeId()));
			fldVals.put("th_dts", new BigIntConstant(currentTime));
			cm.insert(tradeHistoryKey, fldVals, tx);
			cm.flushToLocalStorage(tradeHistoryKey, txNum, tx);
			cm.remove(tradeHistoryKey, txNum);
		}

		// remove cache record
		for (RecordKey k : localReadKeys)
			cm.remove(k, txNum);
		for (RecordKey k : remoteReadKeys)
			cm.remove(k, txNum);

		return true;
	}
}
