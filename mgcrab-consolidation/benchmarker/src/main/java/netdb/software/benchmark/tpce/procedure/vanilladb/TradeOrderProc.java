package netdb.software.benchmark.tpce.procedure.vanilladb;

import java.sql.Connection;

import netdb.software.benchmark.tpce.TpceWorkload;
import netdb.software.benchmark.tpce.procedure.TradeOrderProcedure;

import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.storedprocedure.StoredProcedure;

public class TradeOrderProc extends TradeOrderProcedure implements
		StoredProcedure {

	@Override
	public void prepare(Object... pars) {
		prepareParameters(pars);
	}

	@Override
	public SpResultSet execute() {
		this.tx = VanillaDb.txMgr().transaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		try {
			executeSql();

			if (rollback)
				tx.rollback();
			else
				tx.commit();
		} catch (Exception e) {
			tx.rollback();
			isCommitted = false;
			e.printStackTrace();
		}
		return createResultSet();
	}

	private void executeSql() {
		String sql;
		Plan p;
		Scan s;

		// Get account information by ca_id (customer, broker association)
		String accountName;
		sql = "SELECT ca_name, ca_b_id, ca_c_id FROM customer_account WHERE ca_id = "
				+ acctId;
		p = VanillaDb.planner().createQueryPlan(sql, tx);
		s = p.open();
		s.beforeFirst();
		if (s.next()) {
			accountName = (String) s.getVal("ca_name").asJavaVal();
		} else
			throw new RuntimeException();
		s.close();

		// Get customer information
		sql = "SELECT c_name FROM customer WHERE c_id = " + customerId;
		p = VanillaDb.planner().createQueryPlan(sql, tx);
		s = p.open();
		s.beforeFirst();
		if (s.next()) {
			customerName = (String) s.getVal("c_name").asJavaVal();
		} else
			throw new RuntimeException();
		s.close();

		// Get broker information
		String brokerName;
		sql = "SELECT b_name FROM broker WHERE b_id = " + brokerId;
		p = VanillaDb.planner().createQueryPlan(sql, tx);
		s = p.open();
		s.beforeFirst();
		if (s.next()) {
			brokerName = (String) s.getVal("b_name").asJavaVal();
		} else
			throw new RuntimeException();
		s.close();

		// Get security information
		long companyId;
		String securityName;
		sql = "SELECT s_co_id, s_name FROM security WHERE s_symb = " + sSymb;
		p = VanillaDb.planner().createQueryPlan(sql, tx);
		s = p.open();
		s.beforeFirst();
		if (s.next()) {
			companyId = (Long) s.getVal("s_co_id").asJavaVal();
			securityName = (String) s.getVal("s_name").asJavaVal();
		} else
			throw new RuntimeException();
		s.close();

		// Get current pricing information for the security
		sql = "SELECT lt_price FROM last_trade WHERE lt_s_symb = " + sSymb;
		p = VanillaDb.planner().createQueryPlan(sql, tx);
		s = p.open();
		s.beforeFirst();
		if (s.next()) {
			marketPrice = (Double) s.getVal("lt_price").asJavaVal();
		} else
			throw new RuntimeException();
		s.close();

		// Get trade characteristics based on the type of trade
		int typeIsMarket, typeIsSell;
		sql = "SELECT tt_is_mrkt, tt_is_sell FROM trade_type WHERE tt_id = "
				+ tradeTypeId;
		p = VanillaDb.planner().createQueryPlan(sql, tx);
		s = p.open();
		s.beforeFirst();
		if (s.next()) {
			typeIsMarket = (Integer) s.getVal("tt_is_mrkt").asJavaVal();
			typeIsSell = (Integer) s.getVal("tt_is_sell").asJavaVal();
		} else
			throw new RuntimeException();
		s.close();

		/*
		 * // Get holding quantity int hasQty; sql =
		 * "SELECT sum(hs_qty) as total_qty FROM holding WHERE h_ca_id = " +
		 * acctId + "AND h_s_symb = " + s_symb; p =
		 * VanillaDb.planner().createQueryPlan(sql, tx); s = p.open();
		 * s.beforeFirst(); if (s.next()) { hasQty = (Integer)
		 * s.getVal("total_qty").asJavaVal(); } else { hasQty = 0; throw new
		 * RuntimeException(); } s.close();
		 */

		// XXX The estimation of the overall impact of executing the requested
		// trade is skip in this simplified workload

		// Record the trade information in Trade table
		tradePrice = requestedPrice;
		long currentTime = System.currentTimeMillis();
		sql = String.format(
				"INSERT INTO trade(t_id, t_dts, t_tt_id, t_s_symb, "
						+ "t_qty, t_bid_price, t_ca_id, t_trade_price) "
						+ "VALUES(%d, %d, '%s', '%s', %d, %f, %d, %f)",
				tradeId, currentTime, tradeTypeId, sSymb, tradeQty,
				marketPrice, acctId, tradePrice);
		int result = VanillaDb.planner().executeUpdate(sql, tx);
		if (result <= 0)
			throw new RuntimeException();

		// Record trade information in Trade_History table
		sql = String.format("INSERT INTO trade_history(th_t_id, th_dts) "
				+ "VALUES(%d, %d)", tradeId, currentTime);
		result = VanillaDb.planner().executeUpdate(sql, tx);
		if (result <= 0)
			throw new RuntimeException();
	}
}
