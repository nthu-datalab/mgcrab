package netdb.software.benchmark.tpcc.procedure.vanilladddb;

import java.sql.Connection;

import netdb.software.benchmark.tpcc.procedure.MicroBenchmarkProcedure;

import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.dd.schedule.DdStoredProcedure;
import org.vanilladb.dd.server.VanillaDdDb;
import org.vanilladb.dd.storage.tx.concurrency.ConservativeOrderedCcMgr;

public class MicroBenchmarkProc extends MicroBenchmarkProcedure implements
		DdStoredProcedure {

	private long txNum;
	private String[] accesstable = { "item" };

	public MicroBenchmarkProc(long txNum) {
		this.txNum = txNum;
	}

	public Transaction startTransaction() {
		if (tx == null)
			tx = VanillaDdDb.txMgr().transaction(
					Connection.TRANSACTION_SERIALIZABLE, false, txNum);
		return tx;
	}

	@Override
	public void requestConservativeLocks() {
		startTransaction();
		ConservativeOrderedCcMgr ccMgr = (ConservativeOrderedCcMgr) tx
				.concurrencyMgr();
		ccMgr.prepareSp(accesstable, accesstable);
	}

	@Override
	public void prepare(Object... pars) {
		prepareParameters(pars);
	}

	@Override
	public SpResultSet execute() {
		ConservativeOrderedCcMgr ccMgr = (ConservativeOrderedCcMgr) tx
				.concurrencyMgr();
		ccMgr.executeSp(accesstable, accesstable);
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

	protected void executeSql() {

		// select the items
		for (int i = 0; i < readCount; i++) {
			String name;
			double price;
			String sql = "SELECT i_name, i_price FROM item WHERE i_id = "
					+ readItemId[i];
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
		for (int i = 0; i < writeCount; i++) {
			String sql = "UPDATE item SET i_price = " + newItemPrice[i]
					+ " WHERE i_id = " + writeItemId[i];
			int result = VanillaDb.planner().executeUpdate(sql, tx);
			if (result <= 0)
				throw new RuntimeException();
		}
	}

}
