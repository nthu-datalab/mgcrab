package netdb.software.benchmark.tpce.procedure.vanilladddb.calvin;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import netdb.software.benchmark.tpce.procedure.TradeResultParamHelper;

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

public class TradeResultProc extends
		CalvinStoredProcedure<TradeResultParamHelper> {

	public TradeResultProc(long txNum) {
		super(txNum, new TradeResultParamHelper());
		this.txNum = txNum;
	}

	private RecordKey accountKey, customerKey, brokerKey, tradeKey,
			tradeHistoryKey;

	@Override
	public void prepareKeys() {

		PartitionMetaMgr parMgr = VanillaDdDb.partitionMetaMgr();
		int myId = VanillaDdDb.serverId();

		// account
		Map<String, Constant> keyEntryMap = new HashMap<String, Constant>();
		keyEntryMap.put("ca_id", new BigIntConstant(paramHelper.getAcctId()));
		RecordKey key = new RecordKey("customer_account", keyEntryMap);
		accountKey = key;
		int targetNodeId = parMgr.getPartition(key);
		if (targetNodeId == myId) {
			localReadKeys.add(key);
			localWriteKeys.add(key);
		} else {
			if (!participantNode.contains(targetNodeId))
				participantNode.add(targetNodeId);
			remoteReadKeys.add(key);
		}

		int masterNode = targetNodeId;

		// customer
		keyEntryMap = new HashMap<String, Constant>();
		keyEntryMap
				.put("c_id", new BigIntConstant(paramHelper.getCustomerId()));
		key = new RecordKey("customer", keyEntryMap);
		customerKey = key;
		if (parMgr.getPartition(key) == myId) {
			localReadKeys.add(key);
		} else {
			remoteReadKeys.add(key);
			if (!participantNode.contains(parMgr.getPartition(key))) {
				participantNode.add(parMgr.getPartition(key));
			}
		}

		// broker
		keyEntryMap = new HashMap<String, Constant>();
		keyEntryMap.put("b_id", new BigIntConstant(paramHelper.getBrokerId()));
		key = new RecordKey("broker", keyEntryMap);
		brokerKey = key;
		if (parMgr.getPartition(key) == myId) {
			localReadKeys.add(key);
		} else {
			remoteReadKeys.add(key);
			if (!participantNode.contains(parMgr.getPartition(key))) {
				participantNode.add(parMgr.getPartition(key));
			}
		}

		// trade
		keyEntryMap = new HashMap<String, Constant>();
		keyEntryMap.put("t_id", new BigIntConstant(paramHelper.getTradeId()));
		key = new RecordKey("trade", keyEntryMap);
		tradeKey = key;
		if (parMgr.getPartition(key) == myId) {
			localReadKeys.add(key);
		} else {
			remoteReadKeys.add(key);
			if (!participantNode.contains(parMgr.getPartition(key))) {
				participantNode.add(parMgr.getPartition(key));
			}
		}

		// insert new history
		keyEntryMap = new HashMap<String, Constant>();
		keyEntryMap
				.put("th_t_id", new BigIntConstant(paramHelper.getTradeId()));
		key = new RecordKey("trade_history", keyEntryMap);
		tradeHistoryKey = key;
		targetNodeId = parMgr.getPartition(key);
		if (targetNodeId == myId) {
			localWriteKeys.add(key);
		} else if (!participantNode.contains(targetNodeId)) {
			participantNode.add(targetNodeId);
		}

		if (masterNode == myId)
			isMaster = true;
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
			// SELECT ca_name, ca_b_id, ca_c_id FROM customer_account WHERE
			// ca_id = acctId
			String accountName = (String) cm
					.read(accountKey, txNum, tx,
							localReadKeys.contains(accountKey))
					.getVal("ca_name").asJavaVal();

			// SELECT c_name FROM customer WHERE c_id = customerId
			paramHelper.setCustomerName((String) cm
					.read(customerKey, txNum, tx,
							localReadKeys.contains(customerKey))
					.getVal("c_name").asJavaVal());

			// SELECT b_name FROM broker WHERE b_id = brokerId
			String brokerName = (String) cm
					.read(brokerKey, txNum, tx,
							localReadKeys.contains(brokerKey)).getVal("b_name")
					.asJavaVal();

			// SELECT trade_infos FROM trade WHERE t_id = tradeId
			CachedRecord rec = cm.read(tradeKey, txNum, tx,
					localReadKeys.contains(tradeKey));
			long tradeTime = (Long) rec.getVal("t_dts").asJavaVal();
			String symbol = (String) rec.getVal("t_s_symb").asJavaVal();

			// INSERT INTO trade_history(th_t_id, th_dts)
			long currentTime = System.currentTimeMillis();
			Map<String, Constant> fldVals = new HashMap<String, Constant>();
			fldVals = new HashMap<String, Constant>();
			fldVals.put("th_t_id", new BigIntConstant(paramHelper.getTradeId()));
			fldVals.put("th_dts", new BigIntConstant(currentTime));
			cm.insert(tradeHistoryKey, fldVals, tx);
			cm.flushToLocalStorage(tradeHistoryKey, txNum, tx);
			cm.remove(tradeHistoryKey, txNum);

			// update customer_account set ca_bal = ca_bal + tradePrice WHERE
			// ca_id = acctId
			fldVals = new HashMap<String, Constant>();
			fldVals.put("ca_bal", new DoubleConstant(1000));
			fldVals.put("ca_name", new VarcharConstant(accountName));
			cm.update(accountKey, new CachedRecord(fldVals), tx);
			cm.flushToLocalStorage(accountKey, txNum, tx);
			cm.remove(accountKey, txNum);
		}

		// remove cache record
		for (RecordKey k : localReadKeys)
			cm.remove(k, txNum);
		for (RecordKey k : remoteReadKeys)
			cm.remove(k, txNum);

		return true;
	}
}
