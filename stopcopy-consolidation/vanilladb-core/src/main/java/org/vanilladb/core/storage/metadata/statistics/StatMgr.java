package org.vanilladb.core.storage.metadata.statistics;

import static org.vanilladb.core.storage.metadata.TableMgr.TCAT;
import static org.vanilladb.core.storage.metadata.TableMgr.TCAT_TBLNAME;

import java.util.HashMap;
import java.util.Map;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.storage.metadata.TableInfo;
import org.vanilladb.core.storage.record.RecordFile;
import org.vanilladb.core.storage.tx.Transaction;

/**
 * The statistics manager, which is responsible for keeping statistical
 * information about each table. The manager does not store this information in
 * catalogs in database. Instead, it calculates this information on system
 * startup, keeps the information in memory, and periodically refreshes it.
 */
public class StatMgr {
	// if REFRESH_THRESHOLD == 0, the refresh process will be turned off
	private static final int REFRESH_THRESHOLD;
	private static final int NUM_BUCKETS;
	private static final int NUM_PERCENTILES;
	private static final int REFRESH_STAT_OFF = 0;

	private boolean isRefreshStatOn;
	private Map<String, TableStatInfo> tableStats;
	private Map<String, Integer> updateCounts;

	static {
		String prop = System.getProperty(StatMgr.class.getName()
				+ ".REFRESH_THRESHOLD");
		REFRESH_THRESHOLD = (prop == null ? 100 : Integer.parseInt(prop.trim()));
		prop = System.getProperty(StatMgr.class.getName() + ".NUM_BUCKETS");
		NUM_BUCKETS = (prop == null ? 20 : Integer.parseInt(prop.trim()));
		prop = System.getProperty(StatMgr.class.getName() + ".NUM_PERCENTILES");
		NUM_PERCENTILES = (prop == null ? 5 : Integer.parseInt(prop.trim()));
	}

	/**
	 * Creates the statistics manager. The initial statistics are calculated by
	 * traversing the entire database.
	 * 
	 * @param tblMgr
	 *            the table manager
	 * @param tx
	 *            the startup transaction
	 */
	public StatMgr(Transaction tx) {
		initStatistics(tx);
		// Check refresh_threshold value to turn on/off the statistics
		isRefreshStatOn = !(REFRESH_THRESHOLD == REFRESH_STAT_OFF);
	}

	/**
	 * Returns the statistical information about the specified table.
	 * 
	 * @param ti
	 *            the table's metadata
	 * @param tx
	 *            the calling transaction
	 * @return the statistical information about the table
	 */
	public synchronized TableStatInfo getTableStatInfo(TableInfo ti,
			Transaction tx) {
		if (isRefreshStatOn) {
			Integer c = updateCounts.get(ti.tableName());
			if (c != null && c > REFRESH_THRESHOLD)
				VanillaDb.taskMgr().runTask(
						new StatisticsRefreshTask(tx, ti.tableName()));
		}

		TableStatInfo tsi = tableStats.get(ti.tableName());
		if (tsi == null) {
			tsi = calcTableStats(ti, tx);
			tableStats.put(ti.tableName(), tsi);
		}
		return tsi;
	}

	public synchronized void countRecordUpdates(String tblName, int count) {
		if (!isRefreshStatOn)
			return;
		Integer pre = updateCounts.get(tblName);
		if (pre == null) {
			pre = 0;
		}
		updateCounts.put(tblName, pre + count);
	}

	protected boolean isRefreshStatOn() {
		return this.isRefreshStatOn;
	}

	protected synchronized void refreshStatistics(String tblName, Transaction tx) {
		updateCounts.put(tblName, 0);

		TableInfo ti = VanillaDb.catalogMgr().getTableInfo(tblName, tx);
		TableStatInfo si = calcTableStats(ti, tx);
		tableStats.put(tblName, si);
	}

	private synchronized void initStatistics(Transaction tx) {
		updateCounts = new HashMap<String, Integer>();
		tableStats = new HashMap<String, TableStatInfo>();
		TableInfo tcatmd = VanillaDb.catalogMgr().getTableInfo(TCAT, tx);
		RecordFile tcatfile = tcatmd.open(tx, true);
		tcatfile.beforeFirst();
		while (tcatfile.next()) {
			String tblName = (String) tcatfile.getVal(TCAT_TBLNAME).asJavaVal();
			refreshStatistics(tblName, tx);
		}
		tcatfile.close();
	}

	private synchronized TableStatInfo calcTableStats(TableInfo ti,
			Transaction tx) {

		// delete this ==begin
		long numblocks = 0;
		Schema schema = ti.schema();
		SampledHistogramBuilder hb = new SampledHistogramBuilder(schema);
		Histogram h = hb.newMaxDiffHistogram(NUM_BUCKETS, NUM_PERCENTILES);
		return new TableStatInfo(numblocks, h);
		// delete above end==

		// long numblocks = 0;
		// Schema schema = ti.schema();
		// SampledHistogramBuilder hb = new SampledHistogramBuilder(schema);
		//
		// RecordFile rf = ti.open(tx, true);
		// rf.beforeFirst();
		// while (rf.next()) {
		// numblocks = rf.currentRecordId().block().number() + 1;
		// hb.sample(rf);
		// }
		// rf.close();

		// Histogram h = hb.newMaxDiffHistogram(NUM_BUCKETS, NUM_PERCENTILES);
		// return new TableStatInfo(numblocks, h);
	}
}