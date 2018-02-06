package netdb.software.benchmark.tpcc.procedure.vanilladb;

import java.sql.Connection;

import netdb.software.benchmark.tpcc.procedure.NewOrderProcParamHelper;
import netdb.software.benchmark.tpcc.util.DoublePlainPrinter;

import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.storedprocedure.StoredProcedure;
import org.vanilladb.core.storage.tx.Transaction;

/**
 * The stored procedure which executes the new order transaction defined in
 * TPC-C 5.11.
 * 
 */
public class NewOrderProc implements StoredProcedure {

	private NewOrderProcParamHelper paramHelper = new NewOrderProcParamHelper();
	private Transaction tx;

	public NewOrderProc() {

	}

	@Override
	public void prepare(Object... pars) {
		paramHelper.prepareParameters(pars);
	}

	public SpResultSet execute() {
		tx = VanillaDb.txMgr().transaction(Connection.TRANSACTION_SERIALIZABLE,
				false);
		try {
			executeSql();

			if (paramHelper.isItemNotFound()) {
				tx.rollback();
				paramHelper.setCommitted(false);
			} else {
				tx.commit();
			}
		} catch (Exception e) {
			tx.rollback();
			paramHelper.setCommitted(false);
			e.printStackTrace();
		}
		return paramHelper.createResultSet();
	}

	protected void executeSql() {
		String sql = "SELECT w_tax FROM warehouse WHERE w_id = "
				+ paramHelper.getWid();
		Plan p = VanillaDb.planner().createQueryPlan(sql, tx);
		Scan s = p.open();
		s.beforeFirst();
		if (s.next())
			paramHelper.setwTax((Double) s.getVal("w_tax").asJavaVal());
		else
			throw new RuntimeException();
		s.close();

		int nextOId = 0;
		sql = "SELECT d_tax, d_next_o_id FROM district WHERE d_w_id = "
				+ paramHelper.getWid() + " AND d_id = " + paramHelper.getDid();
		p = VanillaDb.planner().createQueryPlan(sql, tx);
		s = p.open();
		s.beforeFirst();
		if (s.next()) {
			paramHelper.setdTax((Double) s.getVal("d_tax").asJavaVal());
			nextOId = (Integer) s.getVal("d_next_o_id").asJavaVal();
		} else
			throw new RuntimeException();
		s.close();

		sql = "SELECT c_discount, c_last, c_credit FROM customer WHERE c_w_id = "
				+ paramHelper.getWid()
				+ " AND c_d_id = "
				+ paramHelper.getDid() + " AND c_id = " + paramHelper.getCid();
		p = VanillaDb.planner().createQueryPlan(sql, tx);
		s = p.open();
		s.beforeFirst();
		if (s.next()) {
			paramHelper.setcDiscount((Double) s.getVal("c_discount")
					.asJavaVal());
			paramHelper.setcLast((String) s.getVal("c_last").asJavaVal());
			paramHelper.setcCredit((String) s.getVal("c_credit").asJavaVal());
		} else
			throw new RuntimeException();
		s.close();

		sql = "UPDATE district SET d_next_o_id = " + (nextOId + 1)
				+ " WHERE d_w_id = " + paramHelper.getWid() + " AND d_id = "
				+ paramHelper.getDid();
		int result = VanillaDb.planner().executeUpdate(sql, tx);
		if (result <= 0)
			throw new RuntimeException();

		paramHelper.setoEntryDate(System.currentTimeMillis());
		int isAllLocal = paramHelper.isAllLocal() ? 1 : 0;
		sql = "INSERT INTO orders(o_id, o_d_id, o_w_id, o_c_id, o_entry_d,"
				+ "o_carrier_id, o_ol_cnt, o_all_local) VALUES (" + nextOId
				+ ", " + paramHelper.getDid() + ", " + paramHelper.getWid()
				+ ", " + paramHelper.getCid() + ", "
				+ paramHelper.getoEntryDate() + ", 0, "
				+ paramHelper.getOlCount() + ", " + isAllLocal + ")";

		result = VanillaDb.planner().executeUpdate(sql, tx);
		if (result <= 0)
			throw new RuntimeException();

		sql = "INSERT INTO new_order (no_o_id, no_d_id, no_w_id)" + "VALUES ("
				+ nextOId + ", " + paramHelper.getDid() + ", "
				+ paramHelper.getWid() + ")";
		result = VanillaDb.planner().executeUpdate(sql, tx);
		if (result <= 0)
			throw new RuntimeException();

		for (int i = 1; i <= paramHelper.getOlCount(); i++) {
			int olIId = paramHelper.getItems()[i - 1][0];
			int olSupplyWId = paramHelper.getItems()[i - 1][1];
			int olQuantity = paramHelper.getItems()[i - 1][2];
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
				paramHelper.setItemNotFound(true);
				break;
			}
			s.close();

			int sQuantity = 0, sYtd, sOrderCnt;
			String sData, sDistXX, sDistInfo;
			if (paramHelper.getDid() == 10)
				sDistXX = "s_dist_10";
			else
				sDistXX = "s_dist_0" + paramHelper.getDid();

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
					+ paramHelper.getDid()
					+ ", "
					+ paramHelper.getWid()
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
			paramHelper.setTotalAmount(paramHelper.getTotalAmount() + olAmount);
		}
	}
}