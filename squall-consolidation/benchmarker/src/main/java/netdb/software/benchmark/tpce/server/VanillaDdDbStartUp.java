package netdb.software.benchmark.tpce.server;

import java.sql.Connection;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import netdb.software.benchmark.App;

import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.storage.index.Index;
import org.vanilladb.core.storage.metadata.CatalogMgr;
import org.vanilladb.core.storage.metadata.index.IndexInfo;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.dd.server.VanillaDdDb;

public class VanillaDdDbStartUp implements SutStartUp {
	private static Logger logger = Logger.getLogger(VanillaDdDbStartUp.class
			.getName());

	public void startup(String[] args) {
		if (logger.isLoggable(Level.INFO))
			logger.info("initing...");

		VanillaDdDb.init(args[0], Integer.parseInt(args[1]));
		
//		String prop = System.getProperty(App.class.getName() + ".LOAD_TESTBED");
//		boolean LOAD_TESTBED = (prop == null) ? false : Boolean
//				.parseBoolean(prop.trim());
//		if (!LOAD_TESTBED)
//			preload();

		if (logger.isLoggable(Level.INFO))
			logger.info("tpce benchmark vanilladb-dd server ready");
	}

	private static final String TPCC_TABLES_DML[] = {
			"SELECT c_id FROM customer", "SELECT ca_id FROM customer_account",
			"SELECT h_t_id FROM holding",
			"SELECT hh_h_t_id FROM holding_history", "SELECT b_id FROM broker",
			"SELECT t_id FROM trade", "SELECT th_t_id FROM trade_history",
			"SELECT tt_id FROM trade_type", "SELECT co_id FROM company",
			"SELECT lt_s_symb FROM last_trade", "SELECT s_symb FROM security" };

	private static final String READ_TABLES[] = { "customer",
			"customer_account", "broker", "security", "last_trade",
			"trade_type" };

	private void preload() {
		Transaction tx = VanillaDdDb.txMgr().transaction(
				Connection.TRANSACTION_SERIALIZABLE, true);
		if (logger.isLoggable(Level.INFO))
			logger.warning("start preloading data procedure...");
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
	}
}
