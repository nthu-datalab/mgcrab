package netdb.software.benchmark.tpcc.procedure.vanilladb;

import java.sql.Connection;
import java.sql.SQLException;

import netdb.software.benchmark.tpcc.procedure.OrderStatusProcParamHelper;

import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.storedprocedure.StoredProcedure;
import org.vanilladb.core.storage.tx.Transaction;

/**
 * The stored procedure which executes the order status transaction defined in
 * TPC-C 5.11. It is a read-only transaction.
 * 
 */
public class OrderStatusProc implements StoredProcedure {

	private Transaction tx;
	private OrderStatusProcParamHelper paramHelper = new OrderStatusProcParamHelper();

	public OrderStatusProc() {

	}

	@Override
	public void prepare(Object... pars) {
		paramHelper.prepareParameters(pars);
	}

	@Override
	public SpResultSet execute() {
		this.tx = VanillaDb.txMgr().transaction(
				Connection.TRANSACTION_SERIALIZABLE, true);
		try {
			if (paramHelper.isSelectByCLast())
				executeSqlByCLast();
			else
				executeSqlByCid();
			tx.commit();
		} catch (Exception e) {
			tx.rollback();
			paramHelper.setCommitted(false);
			e.printStackTrace();
		}
		return paramHelper.createResultSet();
	}

	protected void executeSqlByCid() throws SQLException {

		Plan p;
		Scan s;

		String sql = "SELECT c_balance, c_first, c_middle, c_last "
				+ "FROM customer WHERE c_w_id = " + paramHelper.getCwid()
				+ " AND c_d_id = " + paramHelper.getCdid() + " AND c_id = "
				+ paramHelper.getCid();
		p = VanillaDb.planner().createQueryPlan(sql, tx);
		s = p.open();
		s.beforeFirst();
		if (s.next()) {
			paramHelper.setcFirst((String) s.getVal("c_first").asJavaVal());
			paramHelper.setcMiddle((String) s.getVal("c_middle").asJavaVal());
			paramHelper.setcLast((String) s.getVal("c_last").asJavaVal());
			paramHelper.setcBalance((Double) s.getVal("c_balance").asJavaVal());
		} else
			throw new RuntimeException();
		s.close();

		sql = "SELECT o_id, o_carrier_id, o_entry_d FROM orders WHERE o_w_id ="
				+ paramHelper.getCwid() + " AND o_d_id = "
				+ paramHelper.getCdid() + " AND o_c_id = "
				+ paramHelper.getCid() + " ORDER BY o_id desc";
		p = VanillaDb.planner().createQueryPlan(sql, tx);
		s = p.open();
		s.beforeFirst();
		if (s.next()) {
			paramHelper.setOid((Integer) s.getVal("o_id").asJavaVal());
			paramHelper.setCarrierId((Integer) s.getVal("o_carrier_id")
					.asJavaVal());
			paramHelper.setoEntryDate((Long) s.getVal("o_entry_d").asJavaVal());
		} else
			throw new RuntimeException();
		s.close();

		int olIId, olSupplyWid, olQuantity;
		double olAmount;
		long olDeliveryDate;
		sql = "SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, "
				+ "ol_delivery_d FROM order_line WHERE ol_o_id ="
				+ paramHelper.getOid() + " AND ol_d_id = "
				+ paramHelper.getCdid() + " AND ol_w_id = "
				+ paramHelper.getCwid();
		p = VanillaDb.planner().createQueryPlan(sql, tx);
		s = p.open();
		s.beforeFirst();
		while (s.next()) {
			olIId = (Integer) s.getVal("ol_i_id").asJavaVal();
			olSupplyWid = (Integer) s.getVal("ol_supply_w_id").asJavaVal();
			olQuantity = (Integer) s.getVal("ol_quantity").asJavaVal();
			olAmount = (Double) s.getVal("ol_amount").asJavaVal();
			olDeliveryDate = (Long) s.getVal("ol_delivery_d").asJavaVal();
		}
		s.close();

	}

	protected void executeSqlByCLast() throws SQLException {

		Plan p;
		Scan s;

		long cidCount;
		String sql = "SELECT COUNT(c_id) as \"countofc_id\" FROM customer WHERE c_w_id = "
				+ paramHelper.getCwid()
				+ " AND c_d_id = "
				+ paramHelper.getCdid()
				+ " AND c_last = '"
				+ paramHelper.getcLast() + "'";
		p = VanillaDb.planner().createQueryPlan(sql, tx);
		s = p.open();
		s.beforeFirst();
		if (s.next())
			cidCount = (Long) s.getVal("countofc_id").asJavaVal();
		else
			throw new RuntimeException();
		s.close();
		if (cidCount % 2 == 1)
			cidCount++;

		sql = "SELECT c_balance, c_first, c_middle, c_id "
				+ "FROM customer WHERE c_w_id = " + paramHelper.getCwid()
				+ " AND c_d_id = " + paramHelper.getCdid() + " AND c_last = '"
				+ paramHelper.getcLast() + "'";
		p = VanillaDb.planner().createQueryPlan(sql, tx);
		s = p.open();
		s.beforeFirst();
		int t = 1;
		boolean isSelected = false;
		while (s.next()) {
			if (t == cidCount / 2) {
				paramHelper.setcFirst((String) s.getVal("c_first").asJavaVal());
				paramHelper.setcMiddle((String) s.getVal("c_middle")
						.asJavaVal());
				paramHelper.setCid((Integer) s.getVal("c_id").asJavaVal());
				paramHelper.setcBalance((Double) s.getVal("c_balance")
						.asJavaVal());
				isSelected = true;
			}
			t++;
		}
		s.close();
		if (!isSelected)
			throw new RuntimeException();

		sql = "SELECT o_id, o_carrier_id, o_entry_d FROM orders WHERE o_w_id ="
				+ paramHelper.getCwid() + " AND o_d_id = "
				+ paramHelper.getCdid() + " AND o_c_id = "
				+ paramHelper.getCid() + " ORDER BY o_id desc";
		p = VanillaDb.planner().createQueryPlan(sql, tx);
		s = p.open();
		s.beforeFirst();
		if (s.next()) {
			paramHelper.setOid((Integer) s.getVal("o_id").asJavaVal());
			paramHelper.setCarrierId((Integer) s.getVal("o_carrier_id")
					.asJavaVal());
			paramHelper.setoEntryDate((Long) s.getVal("o_entry_d").asJavaVal());
		} else
			throw new RuntimeException();
		s.close();

		int olIId, olSupplyWid, olQuantity;
		double olAmount;
		long olDeliveryDate;
		sql = "SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, "
				+ "ol_delivery_d FROM order_line WHERE ol_o_id ="
				+ paramHelper.getOid() + " AND ol_d_id = "
				+ paramHelper.getCdid() + " AND ol_w_id = "
				+ paramHelper.getCwid();
		p = VanillaDb.planner().createQueryPlan(sql, tx);
		s = p.open();
		s.beforeFirst();
		while (s.next()) {
			olIId = (Integer) s.getVal("ol_i_id").asJavaVal();
			olSupplyWid = (Integer) s.getVal("ol_supply_w_id").asJavaVal();
			olQuantity = (Integer) s.getVal("ol_quantity").asJavaVal();
			olAmount = (Double) s.getVal("ol_amount").asJavaVal();
			olDeliveryDate = (Long) s.getVal("ol_delivery_d").asJavaVal();
		}
		s.close();

	}

}
