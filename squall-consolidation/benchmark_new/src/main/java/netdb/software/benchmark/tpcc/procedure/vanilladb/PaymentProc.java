package netdb.software.benchmark.tpcc.procedure.vanilladb;

import java.sql.Connection;

import netdb.software.benchmark.tpcc.procedure.PaymentProcedure;

import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.storedprocedure.StoredProcedure;

/**
 * The stored procedure which executes the customer payment transaction defined
 * in TPC-C 5.11.
 * 
 */
public class PaymentProc extends PaymentProcedure implements StoredProcedure {

	public PaymentProc() {

	}

	@Override
	public void prepare(Object... pars) {
		prepareParameters(pars);
	}

	@Override
	public SpResultSet execute() {
		this.tx = VanillaDb.txMgr().transaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		try {
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
}
