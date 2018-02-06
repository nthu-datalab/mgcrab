package netdb.software.benchmark.tpcc.procedure.mysql;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;

import netdb.software.benchmark.tpcc.procedure.SchemaBuilderProcedure;
import netdb.software.benchmark.tpcc.util.MysqlService;

import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.storedprocedure.StoredProcedure;
import org.vanilladb.core.storage.tx.Transaction;

public class MysqlSchemaBuilderProc extends SchemaBuilderProcedure implements
		StoredProcedure {
	private static Logger logger = Logger
			.getLogger(MysqlSchemaBuilderProc.class.getName());

	public MysqlSchemaBuilderProc() {
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

		Connection conn = MysqlService.connect();
		Statement stm = MysqlService.createStatement(conn);
		ResultSet rs = null;

		try {
			for (String cmd : TPCC_TABLES_DDL)
				MysqlService.executeUpdateQuery(cmd, stm);
			for (String cmd : TPCC_INDEXES_DDL)
				MysqlService.executeUpdateQuery(cmd, stm);
		} catch (Exception e) {
			e.printStackTrace();
			tx.rollback();
			isCommitted = false;
		}
		tx.commit();

		MysqlService.closeStatement(stm);
		MysqlService.disconnect(conn);
	}
}
