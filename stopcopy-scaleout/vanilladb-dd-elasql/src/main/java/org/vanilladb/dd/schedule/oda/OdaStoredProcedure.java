package org.vanilladb.dd.schedule.oda;

import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.storedprocedure.StoredProcedureParamHelper;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.dd.cache.CachedRecord;
import org.vanilladb.dd.cache.oda.OdaCacheMgr;
import org.vanilladb.dd.schedule.DdStoredProcedure;
import org.vanilladb.dd.server.VanillaDdDb;
import org.vanilladb.dd.sql.RecordKey;
import org.vanilladb.dd.storage.tx.recovery.DdRecoveryMgr;

public abstract class OdaStoredProcedure implements DdStoredProcedure {

	protected Map<RecordKey, CachedRecord> readMap = new HashMap<RecordKey, CachedRecord>();
	protected Map<RecordKey, Map<String, Constant>> writeMap = new HashMap<RecordKey, Map<String, Constant>>();
	protected StoredProcedureParamHelper paramHelper;
	protected long txNum;

	private Map<RecordKey, Long> readLinks;
	private OdaCacheMgr cacheMgr = (OdaCacheMgr) VanillaDdDb.cacheMgr();
	private Transaction tx;

	/*******************
	 * Abstract methods
	 *******************/

	/**
	 * Prepare the RecordKey for each record to be used in this stored
	 * procedure. Use the {@link #addReadKey(RecordKey)},
	 * {@link #addWriteKey(RecordKey)} method to add keys.
	 */
	public abstract void prepareKeys();

	/**
	 * Execute the transaction logic.
	 */
	public abstract void executeSql();

	/*****************
	 * Implementation
	 *****************/

	/**
	 * Prepare parameters for this stored procedure.
	 * 
	 * @param param
	 *            An object array contains all parameter for this stored
	 *            procedure.
	 */
	public final void prepare(Object... param) {

		if (paramHelper == null)
			return;

		paramHelper.prepareParameters(param);

		if (tx == null)
			tx = VanillaDdDb.txMgr().transaction(
					Connection.TRANSACTION_SERIALIZABLE,
					paramHelper.isReadOnly(), txNum);
		this.tx.addLifecycleListener(new DdRecoveryMgr(tx
				.getTransactionNumber()));
	}

	/**
	 * Add read record key.
	 * 
	 * @param key
	 */
	public final void addReadKey(RecordKey key) {
		readMap.put(key, null);
	}

	/**
	 * Add write record key.
	 * 
	 * @param key
	 */
	public final void addWriteKey(RecordKey key) {
		writeMap.put(key, null);
	}

	/**
	 * Read cached record from reading map.
	 * 
	 * @param key
	 * @return
	 */
	public final CachedRecord read(RecordKey key) {
		return readMap.get(key);
	}

	/**
	 * Update a field to a record key.
	 * 
	 * @param key
	 * @param fieldName
	 * @param value
	 */
	public final void write(RecordKey key, String fieldName, Constant value) {
		CachedRecord record = read(key);
		Map<String, Constant> map = new HashMap<String, Constant>(
				record.getFldValMap());
		map.put(fieldName, value);
		writeMap.put(key, map);
	}

	/**
	 * Read all records for the defined readMap's keys. The retrieved cached
	 * records will be stored into the readMap.
	 */
	public final void prepareRecords() {
		for (RecordKey key : readMap.keySet()) {
			CachedRecord record = cacheMgr.read(key, readLinks.get(key), tx);
			readMap.put(key, record);
		}
	}

	/**
	 * Execute this stored procedure.
	 */
	public final SpResultSet execute() {

		try {
			executeSql();
		} catch (Exception e) {
			paramHelper.setCommitted(false);
			e.printStackTrace();
		}

		if (paramHelper.isCommitted()) {
			tx.commit();
		} else {
			tx.rollback();
		}

		return paramHelper.createResultSet();
	}

	/**
	 * Update the dirty, inserted, deleted records into cache manager.
	 */
	public final void updateCacheMgr() {
		for (Entry<RecordKey, Map<String, Constant>> e : writeMap.entrySet()) {
			cacheMgr.update(e.getKey(), e.getValue(), tx);
		}
	}

	public final void releaseCachedRecords() {

		for (RecordKey k : readMap.keySet()) {
			cacheMgr.releaseCachedRecord(k, readLinks.get(k));
		}

		for (RecordKey k : writeMap.keySet()) {
			cacheMgr.removeCachedRecord(k, readLinks.get(k), tx);
		}

		cacheMgr.removeOldestTxRecords(tx);

		writeMap = null;
		readMap = null;
	}

	public void setReadLinks(Map<RecordKey, Long> link) {
		this.readLinks = link;
	}

	@Override
	public final RecordKey[] getReadSet() {
		return readMap.keySet().toArray(new RecordKey[0]);
	}

	@Override
	public final RecordKey[] getWriteSet() {
		return writeMap.keySet().toArray(new RecordKey[0]);
	}

	@Override
	public boolean isReadOnly() {
		return paramHelper.isReadOnly();
	}
}
