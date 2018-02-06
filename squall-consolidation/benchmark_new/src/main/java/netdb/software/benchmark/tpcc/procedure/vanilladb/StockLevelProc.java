package netdb.software.benchmark.tpcc.procedure.vanilladb;

import java.sql.Connection;

import netdb.software.benchmark.tpcc.procedure.StockLevelProcedure;

import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.storedprocedure.StoredProcedure;

/**
 * The stored procedure which executes the stock level transaction defined in
 * TPC-C 5.11. It is a read-only transaction.
 * 
 */
public class StockLevelProc extends StockLevelProcedure implements
		StoredProcedure {

	public StockLevelProc() {
	}

	@Override
	public void prepare(Object... pars) {
		prepareParameters(pars);
	}

	@Override
	public SpResultSet execute() {
		this.tx = VanillaDb.txMgr().transaction(
				Connection.TRANSACTION_SERIALIZABLE, true);
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
}
