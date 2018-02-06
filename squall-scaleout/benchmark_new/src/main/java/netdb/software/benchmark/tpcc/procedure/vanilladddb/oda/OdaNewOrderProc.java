package netdb.software.benchmark.tpcc.procedure.vanilladddb.oda;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import netdb.software.benchmark.tpcc.procedure.NewOrderProcParamHelper;

import org.vanilladb.core.sql.BigIntConstant;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.DoubleConstant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.dd.cache.CachedRecord;
import org.vanilladb.dd.cache.oda.OdaCacheMgr;
import org.vanilladb.dd.schedule.oda.OdaStoredProcedure;
import org.vanilladb.dd.server.VanillaDdDb;
import org.vanilladb.dd.sql.RecordKey;

/**
 * The stored procedure which executes the new order transaction defined in
 * TPC-C 5.11.
 * 
 */
public class OdaNewOrderProc extends OdaStoredProcedure {
	private static int[] distrOIds;
	static {
		distrOIds = new int[100];
		for (int i = 0; i < distrOIds.length; i++)
			distrOIds[i] = 3001;
	}

	private Transaction tx;
	private long txNum;
	private Object[] pars;

	private Map<RecordKey, Long> readLinks;
	private Map<RecordKey, Map<String, Constant>> writings = new HashMap<RecordKey, Map<String, Constant>>();
	private Map<RecordKey, Map<String, Constant>> inserts = new HashMap<RecordKey, Map<String, Constant>>();

	private int oid;

	private RecordKey[][] repeatingGroupKeys;
	private RecordKey[] readKeys, writeKeys;
	private RecordKey warehouseKey, districtKey, customerKey, ordersKey,
			newOrderKey;
	private Constant widCon, didCon, cidCon, oidCon;

	private NewOrderProcParamHelper noParamHelper = new NewOrderProcParamHelper();

	public OdaNewOrderProc(long txNum) {
		this.txNum = txNum;
		this.paramHelper = noParamHelper;
	}

	@Override
	public void prepareKeys() {
		int index = (noParamHelper.getWid() - 1) * 10 + noParamHelper.getDid()
				- 1;
		oid = distrOIds[index];
		distrOIds[index] = oid + 1;

		repeatingGroupKeys = new RecordKey[15][3];
		widCon = new IntegerConstant(noParamHelper.getWid());
		didCon = new IntegerConstant(noParamHelper.getDid());
		cidCon = new IntegerConstant(noParamHelper.getCid());
		oidCon = new IntegerConstant(oid);

		// keys of read set and write set

		List<Object> readKeyList = new LinkedList<Object>();
		List<Object> writeKeyList = new LinkedList<Object>();

		Map<String, Constant> keyEntryMap = new HashMap<String, Constant>();
		keyEntryMap.put("w_id", widCon);
		RecordKey key = new RecordKey("warehouse", keyEntryMap);
		warehouseKey = key;
		readKeyList.add(key);
		addReadKey(key);

		keyEntryMap = new HashMap<String, Constant>();
		keyEntryMap.put("d_w_id", widCon);
		keyEntryMap.put("d_id", didCon);
		key = new RecordKey("district", keyEntryMap);
		districtKey = key;
		readKeyList.add(key);
		writeKeyList.add(key);
		addReadKey(key);
		addWriteKey(key);

		keyEntryMap = new HashMap<String, Constant>();
		keyEntryMap.put("c_w_id", widCon);
		keyEntryMap.put("c_d_id", didCon);
		keyEntryMap.put("c_id", cidCon);
		key = new RecordKey("customer", keyEntryMap);
		customerKey = key;
		readKeyList.add(key);
		addReadKey(key);

		keyEntryMap = new HashMap<String, Constant>();
		keyEntryMap.put("o_w_id", widCon);
		keyEntryMap.put("o_d_id", didCon);
		keyEntryMap.put("o_id", oidCon);
		key = new RecordKey("orders", keyEntryMap);
		ordersKey = key;
		writeKeyList.add(key);
		addWriteKey(key);

		keyEntryMap = new HashMap<String, Constant>();
		keyEntryMap.put("no_w_id", widCon);
		keyEntryMap.put("no_d_id", didCon);
		keyEntryMap.put("no_o_id", oidCon);
		key = new RecordKey("new_order", keyEntryMap);
		newOrderKey = key;
		writeKeyList.add(key);
		addWriteKey(key);

		for (int i = 0; i < noParamHelper.getOlCount(); i++) {
			int olIId = noParamHelper.getItems()[i][0];
			int olSupplyWId = noParamHelper.getItems()[i][1];
			Constant olIIdCon = new IntegerConstant(olIId);
			Constant supWidCon = new IntegerConstant(olSupplyWId);
			Constant olNumCon = new IntegerConstant(i + 1);

			keyEntryMap = new HashMap<String, Constant>();
			keyEntryMap.put("i_id", olIIdCon);
			key = new RecordKey("item", keyEntryMap);
			repeatingGroupKeys[i][0] = key;
			readKeyList.add(key);
			addReadKey(key);

			keyEntryMap = new HashMap<String, Constant>();
			keyEntryMap.put("ol_o_id", oidCon);
			keyEntryMap.put("ol_d_id", didCon);
			keyEntryMap.put("ol_w_id", widCon);
			keyEntryMap.put("ol_number", olNumCon);
			key = new RecordKey("order_line", keyEntryMap);
			repeatingGroupKeys[i][2] = key;
			writeKeyList.add(key);
			addWriteKey(key);

			keyEntryMap = new HashMap<String, Constant>();
			keyEntryMap.put("s_i_id", olIIdCon);
			keyEntryMap.put("s_w_id", supWidCon);
			key = new RecordKey("stock", keyEntryMap);
			repeatingGroupKeys[i][1] = key;
			readKeyList.add(key);
			writeKeyList.add(key);
			addReadKey(key);
			addWriteKey(key);
		}

		readKeys = readKeyList.toArray(new RecordKey[0]);
		writeKeys = writeKeyList.toArray(new RecordKey[0]);
	}

	@Override
	public void executeSql() {
		OdaCacheMgr cm = (OdaCacheMgr) VanillaDdDb.cacheMgr();

		HashMap<RecordKey, CachedRecord> readings = new HashMap<RecordKey, CachedRecord>();

		// SELECT w_tax FROM warehouse WHERE w_id = wid
		CachedRecord rec = cm.read(warehouseKey, readLinks.get(warehouseKey),
				tx);
		Constant wTaxCon = rec.getVal("w_tax");
		noParamHelper.setwTax((Double) wTaxCon.asJavaVal());
		readings.put(warehouseKey, rec);

		// SELECT d_tax, d_next_o_id FROM district WHERE d_w_id = wid AND
		// d_id = did
		rec = cm.read(districtKey, readLinks.get(districtKey), tx);
		Constant dTaxCon = rec.getVal("d_tax");
		noParamHelper.setdTax((Double) dTaxCon.asJavaVal());
		rec.getVal("d_next_o_id");
		readings.put(districtKey, rec);

		// SELECT c_discount, c_last, c_credit FROM customer WHERE c_w_id =
		// wid AND c_d_id = did AND c_id = cid
		rec = cm.read(customerKey, readLinks.get(customerKey), tx);
		Constant cDiscountCon = rec.getVal("c_discount");
		Constant cLastCon = rec.getVal("c_last");
		Constant cCreditCon = rec.getVal("c_credit");
		noParamHelper.setcDiscount((Double) cDiscountCon.asJavaVal());
		noParamHelper.setcLast((String) cLastCon.asJavaVal());
		noParamHelper.setcCredit((String) cCreditCon.asJavaVal());
		readings.put(customerKey, rec);

		/********** perform tx logic and write local ************/
		// UPDATE district SET d_next_o_id = (nextOId + 1) WHERE d_w_id =
		// wid AND d_id = did
		Map<String, Constant> fldVals = new HashMap<String, Constant>(readings
				.get(districtKey).getFldValMap());
		fldVals.put("d_next_o_id", new IntegerConstant(oid + 1));
		writings.put(districtKey, fldVals);

		noParamHelper.setoEntryDate(System.currentTimeMillis());
		int isAllLocal = noParamHelper.isAllLocal() ? 1 : 0;
		// INSERT INTO orders(o_id, o_d_id, o_w_id, o_c_id, o_entry_d,
		// o_carrier_id, o_ol_cnt, o_all_local) VALUES ( nextOId, did, 0,
		// olCount, isAllLocal)

		fldVals = new HashMap<String, Constant>();
		fldVals.put("o_id", oidCon);
		fldVals.put("o_d_id", didCon);
		fldVals.put("o_w_id", widCon);
		fldVals.put("o_c_id", cidCon);
		fldVals.put("o_entry_d",
				new BigIntConstant(noParamHelper.getoEntryDate()));
		fldVals.put("o_carrier_id", new IntegerConstant(0));
		fldVals.put("o_ol_cnt", new IntegerConstant(noParamHelper.getOlCount()));
		fldVals.put("o_all_local", new IntegerConstant(isAllLocal));
		inserts.put(ordersKey, fldVals);

		// INSERT INTO new_order (no_o_id, no_d_id, no_w_id) VALUES
		// (nextOId, did, wid)
		fldVals = new HashMap<String, Constant>();
		fldVals.put("no_o_id", oidCon);
		fldVals.put("no_d_id", didCon);
		fldVals.put("no_w_id", widCon);
		inserts.put(newOrderKey, fldVals);

		for (int i = 0; i < noParamHelper.getOlCount(); i++) {
			rec = cm.read(repeatingGroupKeys[i][0],
					readLinks.get(repeatingGroupKeys[i][0]), tx);

			if (rec == null) {
				noParamHelper.setItemNotFound(true);
				noParamHelper.setCommitted(false);
				return;
			}
		}

		for (int i = 0; i < noParamHelper.getOlCount(); i++) {
			int olIId = noParamHelper.getItems()[i][0];
			int olSupplyWId = noParamHelper.getItems()[i][1];
			int olQuantity = noParamHelper.getItems()[i][2];
			Constant olNumCon = new IntegerConstant(i + 1);
			Constant olIIdCon = new IntegerConstant(olIId);
			Constant supWidCon = new IntegerConstant(olSupplyWId);
			Constant iPriceCon, iNameCon, iDataCon;

			// SELECT i_price, i_name, i_data FROM item WHERE i_id = olIId
			rec = cm.read(repeatingGroupKeys[i][0],
					readLinks.get(repeatingGroupKeys[i][0]), tx);
			iPriceCon = rec.getVal("i_price");
			iNameCon = rec.getVal("i_name");
			iDataCon = rec.getVal("i_data");

			String sDistXX;
			if (noParamHelper.getDid() == 10)
				sDistXX = "s_dist_10";
			else
				sDistXX = "s_dist_0" + noParamHelper.getDid();
			// SELECT s_quantity, s_data, s_ytd, s_order_cnt, sDistXX FROM
			// stock
			// WHERE s_i_id = olIId and s_w_id = olSupplyWId
			Constant sQuantityCon, sYtdCon, sOrderCntCon, sDataCon, sDistInfoCon;
			rec = cm.read(repeatingGroupKeys[i][1],
					readLinks.get(repeatingGroupKeys[i][1]), tx);

			sQuantityCon = rec.getVal("s_quantity");
			sYtdCon = rec.getVal("s_ytd");
			sOrderCntCon = rec.getVal("s_order_cnt");
			sDataCon = rec.getVal("s_data");
			sDistInfoCon = rec.getVal(sDistXX);

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
			fldVals.put("s_ytd", new IntegerConstant(sytd + olQuantity));
			writings.put(repeatingGroupKeys[i][1], fldVals);

			double olAmount = olQuantity * (Double) iPriceCon.asJavaVal();
			String brandGeneric;
			if (((String) iDataCon.asJavaVal()).contains("ORIGINAL")
					&& ((String) sDataCon.asJavaVal()).contains("ORIGINAL"))
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
			fldVals.put("ol_delivery_d", new BigIntConstant(Long.MIN_VALUE));
			fldVals.put("ol_quantity", new IntegerConstant(olQuantity));
			fldVals.put("ol_amount", new DoubleConstant(olAmount));
			fldVals.put("ol_dist_info", sDistInfoCon);
			inserts.put(repeatingGroupKeys[i][2], fldVals);
			noParamHelper.setTotalAmount(noParamHelper.getTotalAmount()
					+ olAmount);

			noParamHelper.setTotalAmount(noParamHelper.getTotalAmount()
					* (1 - noParamHelper.getcDiscount())
					* (1 + noParamHelper.getwTax() + noParamHelper.getdTax()));
		}
	}
}
