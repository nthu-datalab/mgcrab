package netdb.software.benchmark.tpcc.procedure.vanilladb;

import java.sql.Connection;
import java.util.logging.Level;
import java.util.logging.Logger;

import netdb.software.benchmark.tpcc.procedure.SchemaBuilderProcedure;

import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.storedprocedure.StoredProcedure;
import org.vanilladb.core.storage.tx.Transaction;

public class SchemaBuilderProc extends SchemaBuilderProcedure implements
		StoredProcedure {
	private static Logger logger = Logger.getLogger(SchemaBuilderProc.class
			.getName());

	public SchemaBuilderProc() {
	}

	@Override
	public void prepare(Object... pars) {
		// do nothing
	}

	@Override
	public SpResultSet execute() {
		if (logger.isLoggable(Level.FINE))
			logger.info("Create schema for tpcc testbed...");
		createSchema();
		return createResultSet();
	}

	private void createSchema() {
		Transaction tx = VanillaDb.txMgr().transaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		isCommitted = true;
		try {
			for (String cmd : TPCC_TABLES_DDL)
				VanillaDb.planner().executeUpdate(cmd, tx);
			for (String cmd : TPCC_INDEXES_DDL)
				VanillaDb.planner().executeUpdate(cmd, tx);
		} catch (Exception e) {
			e.printStackTrace();
			tx.rollback();
			isCommitted = false;
		}
		tx.commit();
	}
}
