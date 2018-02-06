package netdb.software.benchmark.tpcc.procedure.vanilladddb;

import java.sql.Connection;

import netdb.software.benchmark.tpcc.procedure.OrderStatusProcedure;

import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.dd.schedule.DdStoredProcedure;
import org.vanilladb.dd.server.VanillaDdDb;
import org.vanilladb.dd.storage.tx.concurrency.ConservativeOrderedCcMgr;

/**
 * The stored procedure which executes the order status transaction defined in
 * TPC-C 5.11. It is a read-only transaction.
 * 
 */
public class OrderStatusProc extends OrderStatusProcedure implements
		DdStoredProcedure {

	private long txNum;

	public OrderStatusProc(long txNum) {
		this.txNum = txNum;
	}

	public Transaction startTransaction() {
		if (tx == null)
			tx = VanillaDdDb.txMgr().transaction(
					Connection.TRANSACTION_SERIALIZABLE, true, txNum);
		return tx;
	}

	public String[] getReadTables() {
		String[] rt = { "customer", "orders", "order_line" };
		return rt;
	}

	public String[] getWriteTables() {
		String[] wt = {};
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

			if (selectByCLast)
				executeSqlByCLast();
			else
				executeSqlByCid();
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
}
