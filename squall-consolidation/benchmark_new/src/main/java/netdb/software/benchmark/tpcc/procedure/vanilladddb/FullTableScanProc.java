package netdb.software.benchmark.tpcc.procedure.vanilladddb;

import java.sql.Connection;
import java.util.Map;

import netdb.software.benchmark.tpcc.procedure.FullTableScanProcedure;

import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.storage.index.Index;
import org.vanilladb.core.storage.metadata.CatalogMgr;
import org.vanilladb.core.storage.metadata.index.IndexInfo;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.dd.schedule.DdStoredProcedure;
import org.vanilladb.dd.server.VanillaDdDb;
import org.vanilladb.dd.storage.tx.concurrency.ConservativeOrderedCcMgr;

public class FullTableScanProc extends FullTableScanProcedure implements
		DdStoredProcedure {

	private Transaction tx;
	private long txNum;

	public FullTableScanProc(long txNum) {
		this.txNum = txNum;
	}

	public Transaction startTransaction() {
		if (tx == null)
			tx = VanillaDdDb.txMgr().transaction(
					Connection.TRANSACTION_SERIALIZABLE, false, txNum);
		return tx;
	}

	public String[] getReadTables() {
		return READ_TABLES;
	}

	public String[] getWriteTables() {
		String[] wt = {};
		return wt;
	}

	@Override
	public void prepare(Object... pars) {
		// do nothing
	}

	@Override
	public SpResultSet execute() {
		isCommitted = true;
		try {
			ConservativeOrderedCcMgr ccMgr = (ConservativeOrderedCcMgr) tx
					.concurrencyMgr();
			ccMgr.executeSp(getReadTables(), getWriteTables());

			// scan all tables once
			for (String sql : TPCC_TABLES_DML) {
				Plan p = VanillaDb.planner().createQueryPlan(sql, tx);
				Scan s = p.open();
				s.beforeFirst();
				while (s.next()) {
					// do nothing
				}
				s.close();
			}

			// load all the index block to memory
			CatalogMgr md = VanillaDb.catalogMgr();
			for (String tbl : READ_TABLES) {
				Map<String, IndexInfo> iiMap = md.getIndexInfo(tbl, tx);
				for (IndexInfo ii : iiMap.values()) {
					Index i = ii.open(tx);
					i.preLoadToMemory();
					i.close();
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
			tx.rollback();
			isCommitted = false;
		}
		tx.commit();

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
