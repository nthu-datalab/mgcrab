package netdb.software.benchmark.tpcc.procedure.vanilladddb.calvin;

import java.util.HashMap;
import java.util.Map;

import org.vanilladb.core.sql.BigIntConstant;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.DoubleConstant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.dd.cache.CachedRecord;
import org.vanilladb.dd.schedule.calvin.CalvinStoredProcedure;
import org.vanilladb.dd.server.VanillaDdDb;
import org.vanilladb.dd.sql.RecordKey;

import netdb.software.benchmark.tpcc.TpccConstants;
import netdb.software.benchmark.tpcc.procedure.NewOrderProcParamHelper;

/**
 * Entering a new order is done in a single database transaction with the
 * following steps:<br />
 * 1. Create an order header, comprised of: <br />
 * - 2 row selections with data retrieval <br />
 * - 1 row selections with data retrieval and update<br />
 * - 2 row insertions <br />
 * 2. Order a variable number of items (average ol_cnt = 10), comprised of: <br />
 * - (1 * ol_cnt) row selections with data retrieval <br />
 * - (1 * ol_cnt) row selections with data retrieval and update <br />
 * - (1 * ol_cnt) row insertions <br />
 * 
 * @author yslin
 *
 */
public class NewOrderProc extends
		CalvinStoredProcedure<NewOrderProcParamHelper> {

	// hard code the next order id
	// TODO: This should be retrieve from district table
	// TODO: Is this thread-safe ?
	private static int[] distrOIds;
	static {
		distrOIds = new int[TpccConstants.NUM_WAREHOUSES
				* TpccConstants.DISTRICTS_PER_WAREHOUSE + 100];
		for (int i = 0; i < distrOIds.length; i++)
			distrOIds[i] = 3001;
	}
	private int fakeOid;
	
	/**
	 * This method should be accessed by the thread of the scheduler.
	 * 
	 * @param wid
	 * @param did
	 * @return
	 */
	public static int getNextOrderId(int wid, int did)  {
		return distrOIds[(wid - 1) * 10 + did - 1];
	}

	// Record keys for retrieving data
	private RecordKey warehouseKey, districtKey, customerKey;
	private RecordKey orderKey, newOrderKey;
	// a {itemKey, stockKey, orderLineKey} per order line
	private RecordKey[][] orderLineKeys = new RecordKey[15][3];

	// SQL Constants
	Constant widCon, didCon, cidCon, oidCon;

	public NewOrderProc(long txNum) {
		super(txNum, new NewOrderProcParamHelper());
	}

	@Override
	protected void prepareKeys() {
		Map<String, Constant> keyEntryMap = null;

		// Construct constant from parameters
		widCon = new IntegerConstant(paramHelper.getWid());
		didCon = new IntegerConstant(paramHelper.getDid());
		cidCon = new IntegerConstant(paramHelper.getCid());

		// hard code the next order id
		// TODO: This should be retrieve from district table
		int index = (paramHelper.getWid() - 1) * 10 + paramHelper.getDid() - 1;
		fakeOid = distrOIds[index];
		distrOIds[index] = fakeOid + 1;
		oidCon = new IntegerConstant(fakeOid);

		// =================== Keys for steps 1 ===================

		// SELECT ... FROM warehouse WHERE w_id = wid
		keyEntryMap = new HashMap<String, Constant>();
		keyEntryMap.put("w_id", widCon);
		warehouseKey = new RecordKey("warehouse", keyEntryMap);
		addReadKey(warehouseKey);

		// SELECT ... FROM district WHERE d_w_id = wid AND d_id = did
		keyEntryMap = new HashMap<String, Constant>();
		keyEntryMap.put("d_w_id", widCon);
		keyEntryMap.put("d_id", didCon);
		districtKey = new RecordKey("district", keyEntryMap);
		addReadKey(districtKey);

		// UPDATE ... WHERE d_w_id = wid AND d_id = did
		addWriteKey(districtKey);

		// SELECT ... WHERE c_w_id = wid AND c_d_id = did AND c_id = cid
		keyEntryMap = new HashMap<String, Constant>();
		keyEntryMap.put("c_w_id", widCon);
		keyEntryMap.put("c_d_id", didCon);
		keyEntryMap.put("c_id", cidCon);
		customerKey = new RecordKey("customer", keyEntryMap);
		addReadKey(customerKey);

		// INSERT INTO orders (o_id, o_w_id, o_d_id, ...) VALUES (nextOId, wid,
		// did, ...)
		keyEntryMap = new HashMap<String, Constant>();
		keyEntryMap.put("o_w_id", widCon);
		keyEntryMap.put("o_d_id", didCon);
		keyEntryMap.put("o_id", oidCon);
		orderKey = new RecordKey("orders", keyEntryMap);
		addInsertKey(orderKey);

		// INSERT INTO new_order (no_o_id, no_w_id, no_d_id) VALUES
		// (nextOId, wid, did)
		keyEntryMap = new HashMap<String, Constant>();
		keyEntryMap.put("no_w_id", widCon);
		keyEntryMap.put("no_d_id", didCon);
		keyEntryMap.put("no_o_id", oidCon);
		newOrderKey = new RecordKey("new_order", keyEntryMap);
		addInsertKey(newOrderKey);

		// =================== Keys for steps 2 ===================
		int orderLineCount = paramHelper.getOlCount();
		int[][] items = paramHelper.getItems();

		// For each order line
		for (int i = 0; i < orderLineCount; i++) {
			// initialize variables
			int olIId = items[i][0];
			int olSupplyWId = items[i][1];
			Constant olIIdCon = new IntegerConstant(olIId);
			Constant supWidCon = new IntegerConstant(olSupplyWId);
			Constant olNumCon = new IntegerConstant(i + 1);

			// SELECT ... FROM item WHERE i_id = olIId
			keyEntryMap = new HashMap<String, Constant>();
			keyEntryMap.put("i_id", olIIdCon);
			orderLineKeys[i][0] = new RecordKey("item", keyEntryMap);
			addReadKey(orderLineKeys[i][0]);

			// SELECT ... FROM stock WHERE s_i_id = olIId AND s_w_id =
			// olSupplyWId
			keyEntryMap = new HashMap<String, Constant>();
			keyEntryMap.put("s_i_id", olIIdCon);
			keyEntryMap.put("s_w_id", supWidCon);
			orderLineKeys[i][1] = new RecordKey("stock", keyEntryMap);
			addReadKey(orderLineKeys[i][1]);

			// UPDATE ... WHERE s_i_id = olIId AND s_w_id = olSupplyWId
			addWriteKey(orderLineKeys[i][1]);

			// INSERT INTO order_line (ol_o_id, ol_w_id, ol_d_id, ol_number,
			// ...)
			// VALUES (nextOId, wid, did, i, ...)
			keyEntryMap = new HashMap<String, Constant>();
			keyEntryMap.put("ol_o_id", oidCon);
			keyEntryMap.put("ol_d_id", didCon);
			keyEntryMap.put("ol_w_id", widCon);
			keyEntryMap.put("ol_number", olNumCon);
			orderLineKeys[i][2] = new RecordKey("order_line", keyEntryMap);
			addInsertKey(orderLineKeys[i][2]);
		}
	}

	/**
	 * A new order transaction chooses the node containing the main warehouse be
	 * the master node.
	 * 
	 * @return the master node number
	 */
	@Override
	protected int decideMaster() {
		return VanillaDdDb.partitionMetaMgr().getPartition(warehouseKey);
	}

	@Override
	protected void onLocalReadCollected(
			Map<RecordKey, CachedRecord> localReadings) {
		// Do nothing
	}

	@Override
	protected void onRemoteReadCollected(
			Map<RecordKey, CachedRecord> remoteReadings) {
		// Do nothing
	}

	@Override
	protected void writeRecords(Map<RecordKey, CachedRecord> readings) {
		CachedRecord rec = null;
		Map<String, Constant> fldVals = null;

		// UPDATE district SET d_next_o_id = (nextOId + 1) WHERE d_w_id = wid
		// AND d_id = did
		rec = readings.get(districtKey);
		rec.setVal("d_next_o_id", new IntegerConstant(fakeOid + 1));
		update(districtKey, rec);

		// INSERT INTO orders (o_id, o_w_id, o_d_id, o_c_id, o_entry_d,
		// o_carrier_id, o_ol_cnt, o_all_local) VALUES ( nextOId, wid,
		// did, cid, currentTime, 0, olCount, isAllLocal)
		paramHelper.setoEntryDate(System.currentTimeMillis());
		int isAllLocal = paramHelper.isAllLocal() ? 1 : 0;
		long oEntryDate = paramHelper.getoEntryDate();
		int olCount = paramHelper.getOlCount();

		fldVals = new HashMap<String, Constant>();
		fldVals.put("o_id", oidCon);
		fldVals.put("o_d_id", didCon);
		fldVals.put("o_w_id", widCon);
		fldVals.put("o_c_id", cidCon);
		fldVals.put("o_entry_d", new BigIntConstant(oEntryDate));
		fldVals.put("o_carrier_id", new IntegerConstant(0));
		fldVals.put("o_ol_cnt", new IntegerConstant(olCount));
		fldVals.put("o_all_local", new IntegerConstant(isAllLocal));
		insert(orderKey, fldVals);

		// INSERT INTO new_order (no_o_id, no_d_id, no_w_id) VALUES
		// (nextOId, did, wid)
		fldVals = new HashMap<String, Constant>();
		fldVals.put("no_o_id", oidCon);
		fldVals.put("no_d_id", didCon);
		fldVals.put("no_w_id", widCon);
		insert(newOrderKey, fldVals);

		// For each order line
		int totalAmount = 0;
		int[][] items = paramHelper.getItems();
		int orderLineCount = paramHelper.getOlCount();
		for (int i = 0; i < orderLineCount; i++) {
			
			// SELECT ... FROM item WHERE ...
			rec = readings.get(orderLineKeys[i][0]);
			double iPrice = (Double) rec.getVal("i_price").asJavaVal();

			// UPDATE stock SET s_quantity = ..., s_ytd = s_ytd + ol_quantitity,
			// s_order_cnt = s_order_cnt + 1 WHERE s_i_id = olIId AND
			// s_w_id = olSupplyWId
			rec = readings.get(orderLineKeys[i][1]);

			int olQuantity = items[i][2];
			int sQuantity = (Integer) rec.getVal("s_quantity").asJavaVal();
			int sYtd = (Integer) rec.getVal("s_ytd").asJavaVal();
			int sOrderCnt = (Integer) rec.getVal("s_order_cnt").asJavaVal();

			sQuantity -= olQuantity;
			if (sQuantity < 10)
				sQuantity += 91;
			sYtd += olQuantity;
			sOrderCnt++;
			String sDistXX;
			if (paramHelper.getDid() == 10)
				sDistXX = "s_dist_10";
			else
				sDistXX = "s_dist_0" + paramHelper.getDid();
			Constant sDistInfoCon = rec.getVal(sDistXX);

			rec.setVal("s_quantity", new IntegerConstant(sQuantity));
			rec.setVal("s_ytd", new IntegerConstant(sYtd));
			rec.setVal("s_order_cnt", new IntegerConstant(sOrderCnt));

			update(orderLineKeys[i][1], rec);

			// INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id,
			// ol_number,ol_i_id, ol_supply_w_id, ol_delivery_d,
			// ol_quantity, ol_amount, ol_dist_info) VALUES (
			// nextOId, did, wid, i, olIId, olSupplyWid, NULL, olQuantity,
			// DoublePlainPrinter.toPlainString(olAmount), sDistInfo)
			int olIId = items[i][0];
			int supWid = items[i][1];
			double olAmount = olQuantity * iPrice;
			
			
			fldVals = new HashMap<String, Constant>();
			fldVals.put("ol_o_id", oidCon);
			fldVals.put("ol_d_id", didCon);
			fldVals.put("ol_w_id", widCon);
			fldVals.put("ol_number", new IntegerConstant(i + 1));
			fldVals.put("ol_i_id", new IntegerConstant(olIId));
			fldVals.put("ol_supply_w_id", new IntegerConstant(supWid));
			fldVals.put("ol_delivery_d", new BigIntConstant(Long.MIN_VALUE));
			fldVals.put("ol_quantity", new IntegerConstant(olQuantity));
			fldVals.put("ol_amount", new DoubleConstant(olAmount));
			fldVals.put("ol_dist_info", sDistInfoCon);
			insert(orderLineKeys[i][2], fldVals);
			
			// record amounts
			totalAmount += olAmount;
		}
		paramHelper.setTotalAmount(totalAmount
				* (1 - paramHelper.getcDiscount())
				* (1 + paramHelper.getwTax() + paramHelper.getdTax()));
	}

	@Override
	protected void masterCollectResults(Map<RecordKey, CachedRecord> readings) {
		CachedRecord rec = null;

		// SELECT w_tax FROM warehouse WHERE w_id = wid
		rec = readings.get(warehouseKey);
		paramHelper.setwTax((Double) rec.getVal("w_tax").asJavaVal());

		// SELECT d_tax, d_next_o_id FROM district WHERE d_w_id = wid AND d_id =
		// did
		rec = readings.get(districtKey);
		paramHelper.setdTax((Double) rec.getVal("d_tax").asJavaVal());
		// XXX: This should be used for next order id
		rec.getVal("d_next_o_id").asJavaVal();

		// SELECT c_discount, c_last, c_credit FROM customer WHERE c_w_id = wid
		// AND
		// c_d_id = did AND c_id = cid
		rec = readings.get(customerKey);
		paramHelper.setcDiscount((Double) rec.getVal("c_discount").asJavaVal());
		paramHelper.setcLast((String) rec.getVal("c_last").asJavaVal());
		paramHelper.setcCredit((String) rec.getVal("c_credit").asJavaVal());

		// For each order line
		int orderLineCount = paramHelper.getOlCount();
		for (int i = 0; i < orderLineCount; i++) {

			// SELECT i_price, i_name, i_data FROM item WHERE i_id = olIId
			rec = readings.get(orderLineKeys[i][0]);
			rec.getVal("i_price").asJavaVal();
			rec.getVal("i_name").asJavaVal();
			rec.getVal("i_data").asJavaVal();

			// SELECT s_quantity, sDistXX, s_data, s_ytd, s_order_cnt FROM
			// stock WHERE s_i_id = olIId AND s_w_id = olSupplyWId
			String sDistXX;
			if (paramHelper.getDid() == 10)
				sDistXX = "s_dist_10";
			else
				sDistXX = "s_dist_0" + paramHelper.getDid();
			
			rec = readings.get(orderLineKeys[i][1]);
			rec.getVal("s_quantity").asJavaVal();
			rec.getVal(sDistXX).asJavaVal();
			rec.getVal("s_data").asJavaVal();
			rec.getVal("s_ytd").asJavaVal();
			rec.getVal("s_order_cnt").asJavaVal();
		}
	}

}
