package netdb.software.benchmark.tpcc.procedure.vanilladddb.tpart;

import java.sql.Connection;
import java.util.logging.Level;
import java.util.logging.Logger;

import netdb.software.benchmark.tpcc.procedure.SchemaBuilderProcedure;

import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.dd.schedule.tpart.TPartStoredProcedure;
import org.vanilladb.dd.schedule.tpart.sink.SunkPlan;
import org.vanilladb.dd.server.VanillaDdDb;
import org.vanilladb.dd.sql.RecordKey;
import org.vanilladb.dd.storage.tx.concurrency.ConservativeOrderedCcMgr;

public class SchemaBuilderProc extends SchemaBuilderProcedure implements
		TPartStoredProcedure {
	private static Logger logger = Logger.getLogger(SchemaBuilderProc.class
			.getName());
	private Transaction tx;
	private long txNum;

	public SchemaBuilderProc(long txNum) {
		this.txNum = txNum;
	}

	public Transaction startTransaction() {
		if (tx == null)
			tx = VanillaDdDb.txMgr().transaction(
					Connection.TRANSACTION_SERIALIZABLE, false, txNum);
		return tx;
	}

	public String[] getReadTables() {
		String[] rt = {};
		return rt;
	}

	public String[] getWriteTables() {
		String wt[] = { "warehouse", "district", "customer", "history",
				"orders", "new_order", "item", "stock", "order_line" };
		return wt;
	}

	@Override
	public boolean isReadOnly() {
		return false;
	}

	@Override
	public void prepare(Object... pars) {
		// do nothing
	}

	@Override
	public SpResultSet execute() {
		if (logger.isLoggable(Level.FINE))
			logger.info("Create schema for tpcc testbed...");

		ConservativeOrderedCcMgr ccMgr = (ConservativeOrderedCcMgr) tx
				.concurrencyMgr();
		ccMgr.executeSp(getReadTables(), getWriteTables());
		createSchema();
		return createResultSet();
	}

	@Override
	public void requestConservativeLocks() {
		startTransaction();
		ConservativeOrderedCcMgr ccMgr = (ConservativeOrderedCcMgr) tx
				.concurrencyMgr();
		ccMgr.prepareSp(getReadTables(), getWriteTables());
	}

	private void createSchema() {
		startTransaction();
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

	@Override
	public RecordKey[] getReadSet() {
		return null;
	}

	@Override
	public RecordKey[] getWriteSet() {
		return null;
	}

	@Override
	public void setSunkPlan(SunkPlan plan) {
		// do nothing
	}

	@Override
	public double getWeight() {
		return 0;
	}

	@Override
	public int getProcedureType() {
		return TPartStoredProcedure.POPULATE;
	}

	@Override
	public boolean isMaster() {
		// TODO Auto-generated method stub
		return true;
	}
}