package netdb.software.benchmark.tpcc.procedure.vanilladddb.tpart;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import netdb.software.benchmark.tpcc.TpccConstants;
import netdb.software.benchmark.tpcc.procedure.NewOrderProcParamHelper;

import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.sql.BigIntConstant;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.DoubleConstant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.sql.storedprocedure.SpResultRecord;
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

/**
 * The stored procedure which executes the new order transaction defined in
 * TPC-C 5.11.
 * 
 */
public class TPartNewOrderProc implements TPartStoredProcedure {
	private static int[] distrOIds;
	static {
		distrOIds = new int[TpccConstants.NUM_WAREHOUSES * TpccConstants.DISTRICTS_PER_WAREHOUSE + 100];
		for (int i = 0; i < distrOIds.length; i++)
			distrOIds[i] = 3001;
	}

	private Transaction tx;
	private long txNum;
	private SunkPlan plan;

	private int wid, did, cid, olCount, oid;
	private int[][] items;
	private boolean allLocal;

	private double wTax, dTax, cDiscount, totalAmount;
	private long oEntryDate;
	private String cLast, cCredit;
	private boolean isCommitted = true, itemNotFound = false;

	private RecordKey[][] repeatingGroupKeys;
	private RecordKey[] readKeys, writeKeys;
	private Constant widCon, didCon, cidCon, oidCon;

	private List<CachedEntryKey> entryKeys = new ArrayList<CachedEntryKey>();
	private RecordKey[] localWriteBackKeys;

	private NewTPartCacheMgr cm = (NewTPartCacheMgr) VanillaDdDb.cacheMgr();

	private NewOrderProcParamHelper paramHelper = new NewOrderProcParamHelper();

	public TPartNewOrderProc(long txNum) {
		this.txNum = txNum;
	}

	@Override
	public void prepare(Object... pars) {
		paramHelper.prepareParameters(pars);

		startTransaction();

		wid = paramHelper.getWid();
		did = paramHelper.getDid();
		cid = paramHelper.getCid();
		olCount = paramHelper.getOlCount();
		items = paramHelper.getItems();

		// hard code the next order id
		int index = (wid - 1) * 10 + did - 1;
		oid = distrOIds[index];
		distrOIds[index] = oid + 1;

		repeatingGroupKeys = new RecordKey[15][3];
		widCon = new IntegerConstant(wid);
		didCon = new IntegerConstant(did);
		cidCon = new IntegerConstant(cid);
		oidCon = new IntegerConstant(oid);

		// keys of read set and write set
		readKeys = new RecordKey[3 + 2 * olCount];
		writeKeys = new RecordKey[3 + 2 * olCount];

		Map<String, Constant> keyEntryMap = new HashMap<String, Constant>();
		keyEntryMap.put("w_id", widCon);
		RecordKey key = new RecordKey("warehouse", keyEntryMap);
		warehouseKey = key;
		readKeys[0] = key;

		keyEntryMap = new HashMap<String, Constant>();
		keyEntryMap.put("d_w_id", widCon);
		keyEntryMap.put("d_id", didCon);
		key = new RecordKey("district", keyEntryMap);
		districtKey = key;
		readKeys[1] = key;
		writeKeys[0] = key;

		keyEntryMap = new HashMap<String, Constant>();
		keyEntryMap.put("c_w_id", widCon);
		keyEntryMap.put("c_d_id", didCon);
		keyEntryMap.put("c_id", cidCon);
		key = new RecordKey("customer", keyEntryMap);
		customerKey = key;
		readKeys[2] = key;

		keyEntryMap = new HashMap<String, Constant>();
		keyEntryMap.put("o_w_id", widCon);
		keyEntryMap.put("o_d_id", didCon);
		keyEntryMap.put("o_id", oidCon);
		key = new RecordKey("orders", keyEntryMap);
		ordersKey = key;
		writeKeys[1] = key;

		keyEntryMap = new HashMap<String, Constant>();
		keyEntryMap.put("no_w_id", widCon);
		keyEntryMap.put("no_d_id", didCon);
		keyEntryMap.put("no_o_id", oidCon);
		key = new RecordKey("new_order", keyEntryMap);
		newOrderKey = key;
		writeKeys[2] = key;

		for (int i = 1; i <= olCount; i++) {
			int olIId = items[i - 1][0];
			int olSupplyWId = items[i - 1][1];
			Constant olIIdCon = new IntegerConstant(olIId);
			Constant supWidCon = new IntegerConstant(olSupplyWId);
			Constant olNumCon = new IntegerConstant(i);

			keyEntryMap = new HashMap<String, Constant>();
			keyEntryMap.put("i_id", olIIdCon);
			key = new RecordKey("item", keyEntryMap);
			repeatingGroupKeys[i - 1][0] = key;
			readKeys[2 + i * 2 - 1] = key;

			keyEntryMap = new HashMap<String, Constant>();
			keyEntryMap.put("ol_o_id", oidCon);
			keyEntryMap.put("ol_d_id", didCon);
			keyEntryMap.put("ol_w_id", widCon);
			keyEntryMap.put("ol_number", olNumCon);
			key = new RecordKey("order_line", keyEntryMap);
			repeatingGroupKeys[i - 1][2] = key;
			writeKeys[2 + i * 2 - 1] = key;

			keyEntryMap = new HashMap<String, Constant>();
			keyEntryMap.put("s_i_id", olIIdCon);
			keyEntryMap.put("s_w_id", supWidCon);
			key = new RecordKey("stock", keyEntryMap);
			repeatingGroupKeys[i - 1][1] = key;
			readKeys[2 + i * 2] = key;
			writeKeys[2 + i * 2] = key;
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
		return 6 + olCount * 4;
	}

	@Override
	public void requestConservativeLocks() {
		// startTransaction();
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

	public Transaction startTransaction() {
		if (tx == null)
			tx = VanillaDdDb.txMgr().transaction(
					Connection.TRANSACTION_SERIALIZABLE, false, txNum);
		return tx;
	}

	private void doUncache() {
		for (CachedEntryKey key : entryKeys) {
			cm.uncache(key.getRecordKey(), key.getSource(),
					key.getDestination());
		}
	}

	@Override
	public SpResultSet execute() {
		try {
			executeJob();

			if (itemNotFound) {
				tx.rollback();
				isCommitted = false;
			} else
				tx.commit();
		} catch (Exception e) {
			tx.rollback();
			isCommitted = false;
			e.printStackTrace();
		} finally {
			doUncache();
		}
		return createResultSet();
	}

	private RecordKey warehouseKey, districtKey, customerKey, ordersKey,
			newOrderKey;

	private void executeJob() {

		int sinkId = plan.sinkProcessId();
		localWriteBackKeys = plan.getLocalWriteBackInfo().toArray(
				new RecordKey[0]);
		if (plan.isLocalTask()) {

			for (RecordKey k : plan.getSinkReadingInfo()) {
				cm.createCacheRecordFromSink(k, plan.sinkProcessId(), tx, txNum);
			}

			Long srcTxNum = plan.getReadSrcTxNum(warehouseKey);
			// SELECT w_tax FROM warehouse WHERE w_id = wid
			Constant wTaxCon = cm.read(warehouseKey, srcTxNum, txNum).getVal(
					"w_tax");
			wTax = (Double) wTaxCon.asJavaVal();
			entryKeys.add(new CachedEntryKey(warehouseKey, srcTxNum, txNum));

			srcTxNum = plan.getReadSrcTxNum(districtKey);
			// SELECT d_tax, d_next_o_id FROM district WHERE d_w_id = wid AND
			// d_id = did
			CachedRecord distRec = cm.read(districtKey, srcTxNum, txNum);
			entryKeys.add(new CachedEntryKey(districtKey, srcTxNum, txNum));

			Constant dTaxCon = distRec.getVal("d_tax");
			dTax = (Double) dTaxCon.asJavaVal();
			distRec.getVal("d_next_o_id");

			srcTxNum = plan.getReadSrcTxNum(customerKey);
			// SELECT c_discount, c_last, c_credit FROM customer WHERE c_w_id =
			// wid AND c_d_id = did AND c_id = cid
			CachedRecord rec = cm.read(customerKey, srcTxNum, txNum);
			Constant cDiscountCon = rec.getVal("c_discount");
			Constant cLastCon = rec.getVal("c_last");
			Constant cCreditCon = rec.getVal("c_credit");
			cDiscount = (Double) cDiscountCon.asJavaVal();
			cLast = (String) cLastCon.asJavaVal();
			cCredit = (String) cCreditCon.asJavaVal();
			entryKeys.add(new CachedEntryKey(customerKey, srcTxNum, txNum));

			// NOTE! Partial update
			// UPDATE district SET d_next_o_id = (nextOId + 1) WHERE d_w_id =
			// wid AND d_id = did

			Map<String, Constant> fldVals = new HashMap<String, Constant>(
					distRec.getFldValMap());
			fldVals.put("d_next_o_id", new IntegerConstant(oid + 1));
			cm.update(districtKey, fldVals, tx,
					plan.getWritingDestOfRecord(districtKey));
			entryKeys.add(new CachedEntryKey(districtKey, srcTxNum, txNum));

			oEntryDate = System.currentTimeMillis();
			int isAllLocal = allLocal ? 1 : 0;
			// INSERT INTO orders(o_id, o_d_id, o_w_id, o_c_id, o_entry_d,
			// o_carrier_id, o_ol_cnt, o_all_local) VALUES ( nextOId, did, 0,
			// olCount, isAllLocal)

			fldVals = new HashMap<String, Constant>();
			fldVals.put("o_id", oidCon);
			fldVals.put("o_d_id", didCon);
			fldVals.put("o_w_id", widCon);
			fldVals.put("o_c_id", cidCon);
			fldVals.put("o_entry_d", new BigIntConstant(oEntryDate));
			fldVals.put("o_carrier_id", new IntegerConstant(0));
			fldVals.put("o_ol_cnt", new IntegerConstant(olCount));
			fldVals.put("o_all_local", new IntegerConstant(isAllLocal));
			cm.insert(ordersKey, fldVals, tx,
					plan.getWritingDestOfRecord(ordersKey));
			entryKeys.add(new CachedEntryKey(ordersKey, srcTxNum, txNum));

			// INSERT INTO new_order (no_o_id, no_d_id, no_w_id) VALUES
			// (nextOId, did, wid)
			fldVals = new HashMap<String, Constant>();
			fldVals.put("no_o_id", oidCon);
			fldVals.put("no_d_id", didCon);
			fldVals.put("no_w_id", widCon);
			cm.insert(newOrderKey, fldVals, tx,
					plan.getWritingDestOfRecord(newOrderKey));
			entryKeys.add(new CachedEntryKey(newOrderKey, srcTxNum, txNum));

			// System.out.println("tx" + txNum + " with ol#:" + olCount +
			// " plan:"
			// + plan.readingInfoMap);
			if (!itemNotFound) {
				for (int i = 1; i <= olCount; i++) {
					int olIId = items[i - 1][0];
					int olSupplyWId = items[i - 1][1];
					int olQuantity = items[i - 1][2];
					Constant olNumCon = new IntegerConstant(i);
					Constant olIIdCon = new IntegerConstant(olIId);
					Constant supWidCon = new IntegerConstant(olSupplyWId);
					Constant iPriceCon, iNameCon, iDataCon;

					srcTxNum = plan
							.getReadSrcTxNum(repeatingGroupKeys[i - 1][0]);
					// SELECT i_price, i_name, i_data FROM item WHERE i_id =
					// olIId
					rec = cm.read(repeatingGroupKeys[i - 1][0], srcTxNum, txNum);
					iPriceCon = rec.getVal("i_price");
					iNameCon = rec.getVal("i_name");
					iDataCon = rec.getVal("i_data");
					entryKeys.add(new CachedEntryKey(
							repeatingGroupKeys[i - 1][0], srcTxNum, txNum));

					String sDistXX;
					if (did == 10)
						sDistXX = "s_dist_10";
					else
						sDistXX = "s_dist_0" + did;

					srcTxNum = plan
							.getReadSrcTxNum(repeatingGroupKeys[i - 1][1]);
					// SELECT s_quantity, s_data, s_ytd, s_order_cnt, sDistXX
					// FROM
					// stock
					// WHERE s_i_id = olIId and s_w_id = olSupplyWId
					Constant sQuantityCon, sYtdCon, sOrderCntCon, sDataCon, sDistInfoCon;
					rec = cm.read(repeatingGroupKeys[i - 1][1], srcTxNum, txNum);
					// if (rec == null)
					// System.out.println("null:" + repeatingGroupKeys[i - 1][1]
					// + " sinkId" + sinkId);
					sQuantityCon = rec.getVal("s_quantity");
					sYtdCon = rec.getVal("s_ytd");
					sOrderCntCon = rec.getVal("s_order_cnt");
					sDataCon = rec.getVal("s_data");
					sDistInfoCon = rec.getVal(sDistXX);
					entryKeys.add(new CachedEntryKey(
							repeatingGroupKeys[i - 1][1], srcTxNum, txNum));

					// update stock
					int q = (Integer) sQuantityCon.asJavaVal() - olQuantity;
					int sytd = (Integer) sYtdCon.asJavaVal();
					int soc = (Integer) sOrderCntCon.asJavaVal();

					fldVals = new HashMap<String, Constant>(rec.getFldValMap());
					if (q >= 10)
						fldVals.put("s_quantity", new IntegerConstant(q));
					else
						fldVals.put("s_quantity", new IntegerConstant(q + 91));
					fldVals.put("s_order_cnt", new IntegerConstant(soc + 1));
					cm.update(
							repeatingGroupKeys[i - 1][1],
							fldVals,
							tx,
							plan.getWritingDestOfRecord(repeatingGroupKeys[i - 1][1]));
					fldVals.put("s_ytd", new IntegerConstant(sytd + olQuantity));

					double olAmount = olQuantity
							* (Double) iPriceCon.asJavaVal();
					String brandGeneric;
					if (((String) iDataCon.asJavaVal()).contains("ORIGINAL")
							&& ((String) sDataCon.asJavaVal())
									.contains("ORIGINAL"))
						brandGeneric = "B";
					else
						brandGeneric = "G";
					// INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id,
					// ol_number,ol_i_id, ol_supply_w_id, ol_delivery_d,
					// ol_quantity,
					// ol_amount, ol_dist_info) VALUES ( nextOId, did, wid, i,
					// olIId,
					// olSupplyWid, NULL, olQuantity,
					// DoublePlainPrinter.toPlainString(olAmount), sDistInfo)

					fldVals = new HashMap<String, Constant>();
					fldVals.put("ol_o_id", oidCon);
					fldVals.put("ol_d_id", didCon);
					fldVals.put("ol_w_id", widCon);
					fldVals.put("ol_number", olNumCon);
					fldVals.put("ol_i_id", olIIdCon);
					fldVals.put("ol_supply_w_id", supWidCon);
					fldVals.put("ol_delivery_d", new BigIntConstant(
							Long.MIN_VALUE));
					fldVals.put("ol_quantity", new IntegerConstant(olQuantity));
					fldVals.put("ol_amount", new DoubleConstant(olAmount));
					fldVals.put("ol_dist_info", sDistInfoCon);
					cm.insert(
							repeatingGroupKeys[i - 1][2],
							fldVals,
							tx,
							plan.getWritingDestOfRecord(repeatingGroupKeys[i - 1][2]));
					entryKeys.add(new CachedEntryKey(
							repeatingGroupKeys[i - 1][2], srcTxNum, txNum));

					totalAmount += olAmount;

				}
				totalAmount = totalAmount * (1 - cDiscount) * (1 + wTax + dTax);
			}
			pushToRemote();

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

			/********** local write back ************/
			// ccMgr.executeSp(null, localWriteBackKeys);
			for (RecordKey wk : localWriteBackKeys) {

				cm.writeBack(wk, txNum, sinkId, tx);
			}
		}
	}

	private void pushToRemote() {
		int sinkId = plan.sinkProcessId();
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
					// + pushInfo.getRecord() + ",src:" + txNum + ",dest:"
					// + pushInfo.getDestTxNum());

					rec = cm.read(pushInfo.getRecord(), txNum,
							pushInfo.getDestTxNum());
					entryKeys.add(new CachedEntryKey(pushInfo.getRecord(),
							txNum, pushInfo.getDestTxNum()));
					rs.addTuple(pushInfo.getRecord(), txNum,
							pushInfo.getDestTxNum(), rec);
				}
				VanillaDdDb.connectionMgr().pushTupleSet(targetServerId, rs);
			}
		}
	}

	// private void undo() {
	// /*
	// * Since the T-graph builds an edge from modifier to following reader
	// * for each record to be modified, if the modifier aborted, the
	// * following reader will still read modifier's version. Undo for
	// * modification here means to create a record with modifier version but
	// * with undo value.
	// */
	// int sinkId = plan.sinkProcessId();
	//
	// // undo UPDATE district
	// long srcTxNum = plan.getReadSrcTxNum(districtKey);
	// CachedRecord rec = cm.read(districtKey, srcTxNum, txNum);
	//
	// Map<String, Constant> fldVals = new HashMap<String, Constant>(
	// rec.getFldValMap());
	// cm.update(districtKey, fldVals, tx,
	// plan.getWritingDestOfRecord(districtKey));
	//
	// // undo INSERT INTO orders
	// cm.delete(ordersKey, txNum);
	//
	// // undo INSERT INTO new_order
	// cm.remove(sinkId, newOrderKey, txNum);
	//
	// // undo UPDATE stock
	// for (int i = 1; i <= olCount; i++) {
	// rec = cm.read(sinkId, repeatingGroupKeys[i - 1][0], txNum, tx);
	// // undo the stock of valid item
	// if (rec != null) {
	// srcTxNum = plan.getSrcTxNum(repeatingGroupKeys[i - 1][1]);
	// rec = cm.read(sinkId, repeatingGroupKeys[i - 1][1], srcTxNum,
	// tx);
	// fldVals = new HashMap<String, Constant>(rec.getFldValMap());
	// cm.update(sinkId, repeatingGroupKeys[i - 1][1], fldVals, tx);
	// }
	// }
	// pushToRemote();
	// }

	private SpResultSet createResultSet() {
		/*
		 * TODO The output information is not strictly followed the TPC-C
		 * definition. See the session 2.4.3.5 in TPC-C 5.11 document.
		 */
		Schema sch = new Schema();
		Type statusType = Type.VARCHAR(10);
		sch.addField("status", statusType);
		SpResultRecord rec;

		Type cLastType = Type.VARCHAR(16);
		Type cCreditType = Type.VARCHAR(2);
		Type statusMsgType = Type.VARCHAR(30);

		sch.addField("w_tax", Type.DOUBLE);
		sch.addField("d_tax", Type.DOUBLE);
		sch.addField("c_discount", Type.DOUBLE);
		sch.addField("c_last", cLastType);
		sch.addField("c_credit", cCreditType);
		sch.addField("total_amount", Type.DOUBLE);
		sch.addField("o_entry_date", Type.BIGINT);
		sch.addField("status_msg", statusMsgType);

		rec = new SpResultRecord();
		String status = isCommitted ? "committed" : "abort";
		rec.setVal("status", new VarcharConstant(status, statusType));
		rec.setVal("w_tax", new DoubleConstant(wTax));
		rec.setVal("d_tax", new DoubleConstant(dTax));
		rec.setVal("c_discount", new DoubleConstant(cDiscount));
		rec.setVal("c_last", new VarcharConstant(cLast, cLastType));
		rec.setVal("c_credit", new VarcharConstant(cCredit, cCreditType));
		rec.setVal("total_amount", new DoubleConstant(totalAmount));
		rec.setVal("o_entry_date", new BigIntConstant(oEntryDate));
		String statusMsg = itemNotFound ? TpccConstants.INVALID_ITEM_MESSAGE
				: (allLocal ? "all local" : "remote");
		rec.setVal("status_msg", new VarcharConstant(statusMsg, statusMsgType));
		// } else {
		// rec = new SpResultRecord();
		// String status = isCommitted ? "committed" : "abort";
		// rec.setVal("status", new VarcharConstant(status, statusType));
		// }
		return new SpResultSet(sch, rec);
	}

	@Override
	public int getProcedureType() {
		return TPartStoredProcedure.KEY_ACCESS;
	}

	@Override
	public SunkPlan getSunkPlan() {
		return plan;
	}

	@Override
	public boolean isMaster() {
		return plan.isLocalTask();
	}

}
