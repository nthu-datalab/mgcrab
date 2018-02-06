package netdb.software.benchmark.tpcc.procedure.vanilladb;

import java.sql.Connection;

import netdb.software.benchmark.tpcc.procedure.MicroBenchmarkProcParamHelper;

import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.storedprocedure.StoredProcedure;
import org.vanilladb.core.storage.tx.Transaction;

public class MicroBenchmarkProc implements StoredProcedure {

	private MicroBenchmarkProcParamHelper paramHelper = new MicroBenchmarkProcParamHelper();
	private Transaction tx;

	public MicroBenchmarkProc() {

	}

	@Override
	public void prepare(Object... pars) {
		paramHelper.prepareParameters(pars);
	}

	@Override
	public SpResultSet execute() {
		tx = VanillaDb.txMgr().transaction(Connection.TRANSACTION_SERIALIZABLE,
				false);
		try {
			executeSql();
			tx.commit();
		} catch (Exception e) {
			tx.rollback();
			paramHelper.setCommitted(false);
			e.printStackTrace();
		}
		return paramHelper.createResultSet();
	}

	protected void executeSql() {

		// select the items
		for (int i = 0; i < paramHelper.getReadCount(); i++) {

			String name = "";
			double price = 0;

			String sql = "SELECT i_name, i_price FROM item WHERE i_id = "
					+ paramHelper.getReadItemId()[i];
			Plan p = VanillaDb.planner().createQueryPlan(sql, tx);
			Scan s = p.open();
			s.beforeFirst();
			if (s.next()) {
				name = (String) s.getVal("i_name").asJavaVal();
				price = (Double) s.getVal("i_price").asJavaVal();
			} else
				throw new RuntimeException();
			s.close();
		}

		// update the items
		for (int i = 0; i < paramHelper.getWriteCount(); i++) {
			String sql = "UPDATE item SET i_price = "
					+ paramHelper.getNewItemPrice()[i] + " WHERE i_id = "
					+ paramHelper.getWriteItemId()[i];
			int result = VanillaDb.planner().executeUpdate(sql, tx);
			if (result <= 0)
				throw new RuntimeException();
		}
	}
}
