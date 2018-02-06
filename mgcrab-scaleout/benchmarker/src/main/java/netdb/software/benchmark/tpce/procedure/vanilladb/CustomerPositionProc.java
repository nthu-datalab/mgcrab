package netdb.software.benchmark.tpce.procedure.vanilladb;

import java.sql.Connection;

import netdb.software.benchmark.tpce.TpceConstants;
import netdb.software.benchmark.tpce.procedure.CustomerPositionProcedure;

import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.storedprocedure.StoredProcedure;

public class CustomerPositionProc extends CustomerPositionProcedure implements
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
		// Get customer information
		String customerName = null;
		sql = "SELECT c_name FROM customer WHERE c_id = " + customerId;
		p = VanillaDb.planner().createQueryPlan(sql, tx);
		s = p.open();
		s.beforeFirst();
		if (s.next()) {
			customerName = (String) s.getVal("c_name").asJavaVal();
		} else
			throw new RuntimeException();
		s.close();

		// Get all accounts of this customer
		// for each account, get its ID, balance
		// TODO need holding summary to get the assets value
		long[] accountIdArray = new long[(int) TpceConstants.ACCOUNT_CUSTOMER_FACTOR];
		double[] balanceArray = new double[(int) TpceConstants.ACCOUNT_CUSTOMER_FACTOR];
		Double[] assetValueArray = new Double[(int) TpceConstants.ACCOUNT_CUSTOMER_FACTOR];
		sql = "SELECT ca_id, ca_bal FROM customer_account WHERE ca_c_id = "
				+ customerId;
		p = VanillaDb.planner().createQueryPlan(sql, tx);
		s = p.open();
		s.beforeFirst();
		int idx = 0;
		while (s.next()) {
			accountIdArray[idx] = (Long) s.getVal("ca_id").asJavaVal();
			balanceArray[idx] = (Double) s.getVal("ca_bal").asJavaVal();
		}
		s.close();

		if (needHistory) {
			// Get 10 recent history transactions of this customer from one of
			// his account (randomly selected)
			// currently always select the first account id
			// TODO need cascade query
			Long acctId = accountIdArray[0];
			sql = "SELECT t_id, t_qty, t_s_symb, th_dts FROM trade, trade_history WHERE th_t_id = t_id";

		}

	}

}
