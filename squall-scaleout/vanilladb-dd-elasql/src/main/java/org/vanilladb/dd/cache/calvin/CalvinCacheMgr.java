package org.vanilladb.dd.cache.calvin;

import java.util.Map;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.dd.cache.CacheMgr;
import org.vanilladb.dd.cache.CachedRecord;
import org.vanilladb.dd.sql.RecordKey;

/**
 * The class that deal with remote wait
 * 
 */

// TODO remove unnecessary code
public class CalvinCacheMgr implements CacheMgr {
	private static long MAX_WAITTING_TIME = 120000;
	private BasicCacheMgr cacheMgr;

	private final Object[] anchors = new Object[1009];

	public CalvinCacheMgr() {
		cacheMgr = new BasicCacheMgr();
		for (int i = 0; i < anchors.length; ++i) {
			anchors[i] = new Object();
		}
	}

	private Object prepareAnchor(Object o) {
		int hash = o.hashCode() % anchors.length;
		if (hash < 0) {
			hash += anchors.length;
		}
		return anchors[hash];
	}

	public void cacheRemoteRecord(RecordKey key, CachedRecord rec) {
		synchronized (prepareAnchor(key)) {
			cacheMgr.cacheRecord(key, rec);
			prepareAnchor(key).notifyAll();
		}
	}

	public boolean flushToLocalStorage(RecordKey key, long srcTxNum, Transaction tx) {
		return cacheMgr.flushToLocalStorage(key, srcTxNum, tx);
	}

	public void remove(RecordKey key, long srcTxNum) {
		// System.out.println("remove "+ key+" src:"+srcTxNum);
		cacheMgr.remove(key, srcTxNum);
	}

	public CachedRecord read(RecordKey key, long srcTxNum, Transaction tx, boolean srcIsLocal) {
		CachedRecord rec;

		if (!srcIsLocal) { // not received the remote record yet
			try {
				synchronized (prepareAnchor(key)) {
					long startTime = System.currentTimeMillis();

					rec = cacheMgr.read(key, srcTxNum, tx, srcIsLocal);
					while (rec == null && System.currentTimeMillis() - startTime < MAX_WAITTING_TIME) {
						prepareAnchor(key).wait(MAX_WAITTING_TIME);
						rec = cacheMgr.read(key, srcTxNum, tx, srcIsLocal);
					}

					if (rec == null)
						throw new RuntimeException("Tx." + srcTxNum + " timeout for waiting remote record: " + key);

				}
			} catch (InterruptedException e) {
				throw new RuntimeException();
			}
		} else {
			rec = cacheMgr.read(key, srcTxNum, tx, srcIsLocal);
		}

		return rec;
	}

	/*
	 * TODO: Deprecated public void update(RecordKey key, Map<String, Constant>
	 * fldVals, Transaction tx) { // need to update the whole flds of a record
	 * cacheMgr.update(key, fldVals, tx); }
	 */

	public void update(RecordKey key, CachedRecord rec, Transaction tx) {
		// need to update the whole flds of a record
		cacheMgr.update(key, rec, tx);
	}

	public void insert(RecordKey key, Map<String, Constant> fldVals, Transaction tx) {
		cacheMgr.insert(key, fldVals, tx);
	}

	public void delete(RecordKey key, Transaction tx) {
		cacheMgr.delete(key, tx);
	}

	public void markWriteback(RecordKey key, Transaction tx) {
		cacheMgr.markWriteback(key, tx);
	}
}
