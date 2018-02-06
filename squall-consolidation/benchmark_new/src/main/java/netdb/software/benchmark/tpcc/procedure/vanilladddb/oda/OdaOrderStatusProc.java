package netdb.software.benchmark.tpcc.procedure.vanilladddb.oda;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import netdb.software.benchmark.tpcc.procedure.OrderStatusProcParamHelper;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.dd.cache.CachedRecord;
import org.vanilladb.dd.cache.oda.OdaCacheMgr;
import org.vanilladb.dd.schedule.oda.OdaStoredProcedure;
import org.vanilladb.dd.server.VanillaDdDb;
import org.vanilladb.dd.sql.RecordKey;

public class OdaOrderStatusProc extends OdaStoredProcedure {

	private Transaction tx;
	private long txNum;

	private Map<RecordKey, Long> readLinks;

	private RecordKey[] readKeys, writeKeys;
	private RecordKey customerKey, orderKey, orderLineKey;
	private Constant widCon, didCon, cidCon, oidCon;

	private OrderStatusProcParamHelper osParamHelper = new OrderStatusProcParamHelper();

	public OdaOrderStatusProc(long txNum) {
		this.txNum = txNum;
		this.paramHelper = osParamHelper;

	}

	@Override
	public void prepareKeys() {
		widCon = new IntegerConstant(osParamHelper.getCwid());
		didCon = new IntegerConstant(osParamHelper.getCdid());
		cidCon = new IntegerConstant(osParamHelper.getCid());
		oidCon = new IntegerConstant(
				CustomerOrderHelper.getCustomerLatestOrder(osParamHelper
						.getCid()));

		List<Object> readKeyList = new LinkedList<Object>();
		List<Object> writeKeyList = new LinkedList<Object>();

		// add customer record key
		Map<String, Constant> keyEntryMap = new HashMap<String, Constant>();
		keyEntryMap.put("c_w_id", widCon);
		keyEntryMap.put("c_d_id", didCon);
		keyEntryMap.put("c_id", cidCon);
		RecordKey key = new RecordKey("customer", keyEntryMap);
		customerKey = key;
		readKeyList.add(key);

		// add order record key
		keyEntryMap = new HashMap<String, Constant>();
		keyEntryMap.put("o_w_id", widCon);
		keyEntryMap.put("o_d_id", didCon);
		keyEntryMap.put("o_c_id", cidCon);
		key = new RecordKey("orders", keyEntryMap);
		orderKey = key;
		readKeyList.add(key);

		// add order line record key
		keyEntryMap = new HashMap<String, Constant>();
		keyEntryMap.put("ol_o_id", oidCon);
		keyEntryMap.put("ol_d_id", didCon);
		keyEntryMap.put("ol_w_id", widCon);
		key = new RecordKey("order_line", keyEntryMap);
		orderLineKey = key;
		readKeyList.add(key);

		readKeys = readKeyList.toArray(new RecordKey[0]);
		writeKeys = writeKeyList.toArray(new RecordKey[0]);
	}

	public void executeSql() {
		OdaCacheMgr cm = (OdaCacheMgr) VanillaDdDb.cacheMgr();

		// SELECT c_balance, c_first, c_middle, c_last FROM customer WHERE
		// c_w_id = cwid AND c_d_id = cdid AND c_id = cid
		CachedRecord rec = cm.read(customerKey, readLinks.get(customerKey), tx);
		osParamHelper.setcFirst((String) rec.getVal("c_first").asJavaVal());
		osParamHelper.setcMiddle((String) rec.getVal("c_middle").asJavaVal());
		osParamHelper.setcLast((String) rec.getVal("c_last").asJavaVal());
		osParamHelper.setcBalance((Double) rec.getVal("c_balance").asJavaVal());

		// SELECT o_id, o_carrier_id, o_entry_d FROM orders WHERE o_w_id = cwid
		// AND o_d_id = cdid AND o_c_id = cid ORDER BY o_id desc
		rec = cm.read(orderKey, readLinks.get(orderKey), tx);
		osParamHelper.setOid((Integer) rec.getVal("o_id").asJavaVal());
		osParamHelper.setCarrierId((Integer) rec.getVal("o_carrier_id")
				.asJavaVal());
		osParamHelper.setoEntryDate((Long) rec.getVal("o_entry_d").asJavaVal());

		int olIId, olSupplyWid, olQuantity;
		double olAmount;
		long olDeliveryDate;
		// SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d
		// FROM order_line WHERE ol_o_id = oid AND ol_d_id = cdid AND ol_w_id =
		// cwid
		rec = cm.read(orderLineKey, readLinks.get(orderLineKey), tx);
		olIId = (Integer) rec.getVal("ol_i_id").asJavaVal();
		olSupplyWid = (Integer) rec.getVal("ol_supply_w_id").asJavaVal();
		olQuantity = (Integer) rec.getVal("ol_quantity").asJavaVal();
		olAmount = (Double) rec.getVal("ol_amount").asJavaVal();
		olDeliveryDate = (Long) rec.getVal("ol_delivery_d").asJavaVal();
	}

}
