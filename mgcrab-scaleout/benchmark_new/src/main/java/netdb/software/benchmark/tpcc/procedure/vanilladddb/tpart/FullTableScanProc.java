package netdb.software.benchmark.tpcc.procedure.vanilladddb.tpart;

import java.sql.Connection;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import netdb.software.benchmark.tpcc.procedure.FullTableScanProcedure;

import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.sql.storedprocedure.SpResultRecord;
import org.vanilladb.core.storage.index.Index;
import org.vanilladb.core.storage.metadata.CatalogMgr;
import org.vanilladb.core.storage.metadata.index.IndexInfo;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.dd.schedule.tpart.TPartStoredProcedure;
import org.vanilladb.dd.schedule.tpart.sink.SunkPlan;
import org.vanilladb.dd.server.VanillaDdDb;
import org.vanilladb.dd.sql.RecordKey;
import org.vanilladb.dd.storage.tx.concurrency.ConservativeOrderedCcMgr;

public class FullTableScanProc extends FullTableScanProcedure implements
		TPartStoredProcedure {
	private static Logger logger = Logger.getLogger(FullTableScanProc.class
			.getName());
	private Transaction tx;
	private long txNum;

	public FullTableScanProc(long txNum) {
		this.txNum = txNum;
	}

	@Override
	public void requestConservativeLocks() {
		startTransaction();
		ConservativeOrderedCcMgr ccMgr = (ConservativeOrderedCcMgr) tx
				.concurrencyMgr();

		ccMgr.prepareSp(READ_TABLES, null);
	}

	@Override
	public boolean isReadOnly() {
		return true;
	}

	@Override
	public void prepare(Object... pars) {
		// do nothing
	}

	private Transaction startTransaction() {
		if (tx == null) {
			tx = VanillaDdDb.txMgr().transaction(
					Connection.TRANSACTION_SERIALIZABLE, false, txNum);
			if (logger.isLoggable(Level.INFO))
				logger.warning("start preloading data procedure...");
		}
		return tx;
	}

	@Override
	public SpResultSet execute() {
		boolean isCommitted;
		startTransaction();

		// C2PL
		ConservativeOrderedCcMgr ccMgr = (ConservativeOrderedCcMgr) tx
				.concurrencyMgr();
		ccMgr.executeSp(READ_TABLES, null);

		try {
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

			tx.commit();
			isCommitted = true;
		} catch (Exception e) {
			e.printStackTrace();
			tx.rollback();
			isCommitted = false;
		}

		Schema sch = new Schema();
		Type t = Type.VARCHAR(10);
		sch.addField("status", t);
		SpResultRecord rec = new SpResultRecord();
		String status = isCommitted ? "committed" : "abort";
		rec.setVal("status", new VarcharConstant(status, t));
		return new SpResultSet(sch, rec);
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
	}

	@Override
	public double getWeight() {
		return 0;
	}

	@Override
	public int getProcedureType() {
		return TPartStoredProcedure.PRE_LOAD;
	}
}
