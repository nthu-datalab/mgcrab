package netdb.software.benchmark.tpcc.procedure.vanilladddb;

import java.sql.Connection;

import netdb.software.benchmark.tpcc.procedure.NewOrderProcedure;
import netdb.software.benchmark.tpcc.util.DoublePlainPrinter;

import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.dd.schedule.DdStoredProcedure;
import org.vanilladb.dd.server.VanillaDdDb;
import org.vanilladb.dd.storage.tx.concurrency.ConservativeOrderedCcMgr;

/**
 * The stored procedure which executes the new order transaction defined in
 * TPC-C 5.11.
 * 
 */
public class NewOrderProc extends NewOrderProcedure implements
		DdStoredProcedure {

	private long txNum;

	public NewOrderProc(long txNum) {
		this.txNum = txNum;
	}

	public Transaction startTransaction() {
		if (tx == null)
			tx = VanillaDdDb.txMgr().transaction(
					Connection.TRANSACTION_SERIALIZABLE, false, txNum);
		return tx;
	}

	public String[] getReadTables() {
		String rt[] = { "warehouse", "district", "customer", "item", "stock" };
		return rt;
	}

	public String[] getWriteTables() {
		String wt[] = { "district", "orders", "new_order", "stock",
				"order_line" };
		return wt;
	}

	@Override
	public void prepare(Object... pars) {
		prepareParameters(pars);
	}

	@Override
	public SpResultSet execute() {
		try {
			ConservativeOrderedCcMgr ccMgr = (ConservativeOrderedCcMgr) tx
					.concurrencyMgr();
			ccMgr.executeSp(getReadTables(), getWriteTables());
			executeSql();
			if (itemNotFound) {
				tx.rollback();
				isCommitted = false;
			} else
				tx.commit();
		} catch (Exception e) {
			tx.rollback();
			isCommitted = false;
			e.printStackTrace();
		}
		return createResultSet();
	}

	@Override
	public void requestConservativeLocks() {
		startTransaction();
		ConservativeOrderedCcMgr ccMgr = (ConservativeOrderedCcMgr) tx
				.concurrencyMgr();
		ccMgr.prepareSp(getReadTables(), getWriteTables());
	}

	protected void executeSql() {
		String sql = "SELECT w_tax FROM warehouse WHERE w_id = " + wid;
		Plan p = VanillaDb.planner().createQueryPlan(sql, tx);
		Scan s = p.open();
		s.beforeFirst();
		if (s.next())
			wTax = (Double) s.getVal("w_tax").asJavaVal();
		else
			throw new RuntimeException();
		s.close();

		int nextOId = 0;
		sql = "SELECT d_tax, d_next_o_id FROM district WHERE d_w_id = " + wid
				+ " AND d_id = " + did;
		p = VanillaDb.planner().createQueryPlan(sql, tx);
		s = p.open();
		s.beforeFirst();
		if (s.next()) {
			dTax = (Double) s.getVal("d_tax").asJavaVal();
			nextOId = (Integer) s.getVal("d_next_o_id").asJavaVal();
		} else
			throw new RuntimeException();
		s.close();

		sql = "SELECT c_discount, c_last, c_credit FROM customer WHERE c_w_id = "
				+ wid + " AND c_d_id = " + did + " AND c_id = " + cid;
		p = VanillaDb.planner().createQueryPlan(sql, tx);
		s = p.open();
		s.beforeFirst();
		if (s.next()) {
			cDiscount = (Double) s.getVal("c_discount").asJavaVal();
			cLast = (String) s.getVal("c_last").asJavaVal();
			cCredit = (String) s.getVal("c_credit").asJavaVal();
		} else
			throw new RuntimeException();
		s.close();

		sql = "UPDATE district SET d_next_o_id = " + (nextOId + 1)
				+ " WHERE d_w_id = " + wid + " AND d_id = " + did;
		int result = VanillaDb.planner().executeUpdate(sql, tx);
		if (result <= 0)
			throw new RuntimeException();

		oEntryDate = System.currentTimeMillis();
		int isAllLocal = allLocal ? 1 : 0;
		sql = "INSERT INTO orders(o_id, o_d_id, o_w_id, o_c_id, o_entry_d,"
				+ "o_carrier_id, o_ol_cnt, o_all_local) VALUES (" + nextOId
				+ ", " + did + ", " + wid + ", " + cid + ", " + oEntryDate
				+ ", 0, " + olCount + ", " + isAllLocal + ")";

		result = VanillaDb.planner().executeUpdate(sql, tx);
		if (result <= 0)
			throw new RuntimeException();

		sql = "INSERT INTO new_order (no_o_id, no_d_id, no_w_id)" + "VALUES ("
				+ nextOId + ", " + did + ", " + wid + ")";
		result = VanillaDb.planner().executeUpdate(sql, tx);
		if (result <= 0)
			throw new RuntimeException();

		for (int i = 1; i <= olCount; i++) {
			int olIId = items[i - 1][0];
			int olSupplyWId = items[i - 1][1];
			int olQuantity = items[i - 1][2];
			String iName, iData;
			double iPrice;
			sql = "SELECT i_price, i_name, i_data FROM item WHERE i_id = "
					+ olIId;
			p = VanillaDb.planner().createQueryPlan(sql, tx);
			s = p.open();
			s.beforeFirst();
			if (s.next()) {
				iPrice = (Double) s.getVal("i_price").asJavaVal();
				iName = (String) s.getVal("i_name").asJavaVal();
				iData = (String) s.getVal("i_data").asJavaVal();
			} else {
				itemNotFound = true;
				break;
			}
			s.close();

			int sQuantity = 0, sYtd, sOrderCnt;
			String sData, sDistXX, sDistInfo;
			if (did == 10)
				sDistXX = "s_dist_10";
			else
				sDistXX = "s_dist_0" + did;

			sql = "SELECT S_quantity, s_data, s_ytd, s_order_cnt, " + sDistXX
					+ " FROM stock WHERE s_i_id = " + olIId + " and s_w_id ="
					+ olSupplyWId;
			p = VanillaDb.planner().createQueryPlan(sql, tx);
			s = p.open();
			s.beforeFirst();
			if (s.next()) {
				sQuantity = (Integer) s.getVal("s_quantity").asJavaVal();
				sYtd = (Integer) s.getVal("s_ytd").asJavaVal();
				sOrderCnt = (Integer) s.getVal("s_order_cnt").asJavaVal();
				sData = (String) s.getVal("s_data").asJavaVal();
				sDistInfo = (String) s.getVal(sDistXX).asJavaVal();
			} else
				throw new RuntimeException("error select from stock...");

			s.close();

			int q = sQuantity - olQuantity;
			if (q >= 10)
				sql = "UPDATE stock SET s_quantity =  " + q + ", s_ytd ="
						+ (sYtd + olQuantity) + ", s_order_cnt =  "
						+ (sOrderCnt + 1) + ", WHERE s_i_id =" + olIId
						+ " AND s_w_id =" + olSupplyWId;
			else
				sql = "UPDATE stock SET s_quantity =  " + (q + 91)
						+ ", s_ytd =" + (sYtd + olQuantity)
						+ ", s_order_cnt =  " + (sOrderCnt + 1)
						+ ", WHERE s_i_id =" + olIId + " AND s_w_id ="
						+ olSupplyWId;
			result = VanillaDb.planner().executeUpdate(sql, tx);
			if (result <= 0)
				throw new RuntimeException();

			double olAmount = olQuantity * iPrice;
			String brandGeneric;
			if (iData.contains("ORIGINAL") && sData.contains("ORIGINAL"))
				brandGeneric = "B";
			else
				brandGeneric = "G";
			long NULL = Long.MIN_VALUE;
			sql = "INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number,"
					+ "ol_i_id, ol_supply_w_id, ol_delivery_d, ol_quantity, ol_amount, "
					+ "ol_dist_info) VALUES ("
					+ nextOId
					+ ", "
					+ did
					+ ", "
					+ wid
					+ ", "
					+ i
					+ ", "
					+ olIId
					+ ", "
					+ olSupplyWId
					+ ", "
					+ NULL
					+ ", "
					+ olQuantity
					+ ", "
					+ DoublePlainPrinter.toPlainString(olAmount)
					+ ", '"
					+ sDistInfo + "')";
			result = VanillaDb.planner().executeUpdate(sql, tx);
			if (result <= 0)
				throw new RuntimeException();

			i++;
			totalAmount = totalAmount + olAmount;
		}
	}
}
