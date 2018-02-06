package netdb.software.benchmark.tpcc.server;

import java.sql.Connection;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import netdb.software.benchmark.tpcc.App;

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

		String prop = System.getProperty(App.class.getName() + ".LOAD_TESTBED");
		boolean LOAD_TESTBED = (prop == null) ? false : Boolean
				.parseBoolean(prop.trim());
		// if (!LOAD_TESTBED)
		// preload();

		if (logger.isLoggable(Level.INFO))
			logger.info("tpcc benchmark vanilladb-dd server ready");
	}

	private static final String TPCC_TABLES_DML[] = { "SELECT i_id FROM item" };
	/*
	 * { "SELECT w_id FROM warehouse", "SELECT d_id FROM district",
	 * "SELECT c_id FROM customer", "SELECT h_c_id FROM history",
	 * "SELECT no_o_id FROM new_order", "SELECT o_id FROM orders",
	 * "SELECT ol_o_id FROM order_line", "SELECT i_id FROM item",
	 * "SELECT s_i_id FROM stock" };
	 */

	private static final String READ_TABLES[] = { "warehouse", "district",
			"customer", "new_order", "history", "orders", "order_line",
			"stock", "item" };

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
