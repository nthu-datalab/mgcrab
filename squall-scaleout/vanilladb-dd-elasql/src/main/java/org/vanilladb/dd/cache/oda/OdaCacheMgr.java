package org.vanilladb.dd.cache.oda;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.dd.cache.CacheMgr;
import org.vanilladb.dd.cache.CachedRecord;
import org.vanilladb.dd.cache.LocalRecordMgr;
import org.vanilladb.dd.sql.RecordKey;
import org.vanilladb.dd.sql.RecordVersion;

/**
 * The class that deal with remote wait
 * 
 * 
 */

// TODO remove unnecessary code
public class OdaCacheMgr implements CacheMgr {
	// private static long MAX_TIME = 30000;

	private static final int MAX_RECORD_COUNT;
	private static final double SHADOW_COPY_THRESHOLD;

	private final Object[] cacheMgrAnchors = new Object[1009];
	private final Object[] latestVersionTableAnchors = new Object[1009];
	private final Object[] writebackLockAnchors = new Object[1009];
	private AtomicLong removeCounter = new AtomicLong(0);
	private AtomicInteger cmSize = new AtomicInteger(0);

	private Map<RecordKey, Map<Long, RetainObject>> retainTable = new ConcurrentHashMap<RecordKey, Map<Long, RetainObject>>();

	private Map<RecordKey, Long> latestVersionTable = new ConcurrentHashMap<RecordKey, Long>();

	private Map<RecordKey, Boolean> latestWritebackLockTable = new ConcurrentHashMap<RecordKey, Boolean>();

	private Map<Long, TxRecordSet> txRecordMap = new ConcurrentHashMap<Long, TxRecordSet>();

	private Map<RecordVersion, CachedRecord> cacheRecordMap = new ConcurrentHashMap<RecordVersion, CachedRecord>();

	private static int addCnt = 0;
	private static int removedCnt = 0;

	static {
		String prop = System.getProperty(OdaCacheMgr.class.getName()
				+ ".THRESHOLD");

		SHADOW_COPY_THRESHOLD = (prop == null) ? 2 : Double.parseDouble(prop);
		// the ratio 2 makes the threshold larger than TTL, thus the threshold
		// is never reached

		prop = System.getProperty(OdaCacheMgr.class.getName() + ".MAX_RECORD");

		MAX_RECORD_COUNT = (prop == null) ? 5000 : Integer.parseInt(prop);

		// System.out.println("Max Record Count: "+ MAX_RECORD_COUNT);
	}

	public OdaCacheMgr() {
		for (int i = 0; i < cacheMgrAnchors.length; ++i) {
			cacheMgrAnchors[i] = new Object();
		}
		for (int i = 0; i < latestVersionTableAnchors.length; ++i) {
			latestVersionTableAnchors[i] = new Object();
		}
		for (int i = 0; i < writebackLockAnchors.length; ++i) {
			writebackLockAnchors[i] = new Object();
		}
	}

	private Object prepareCacheMgrAnchor(Object o) {
		int hash = o.hashCode() % cacheMgrAnchors.length;
		if (hash < 0) {
			hash += cacheMgrAnchors.length;
		}
		return cacheMgrAnchors[hash];
	}

	private Object prepareLatestVersionTableAnchor(Object o) {
		int hash = o.hashCode() % latestVersionTableAnchors.length;
		if (hash < 0) {
			hash += latestVersionTableAnchors.length;
		}
		return latestVersionTableAnchors[hash];
	}

	private Object prepareWritebackLockAnchor(Object o) {
		int hash = o.hashCode() % writebackLockAnchors.length;
		if (hash < 0) {
			hash += writebackLockAnchors.length;
		}
		return writebackLockAnchors[hash];
	}

	public void cacheRemoteRecord(RecordKey key, CachedRecord rec) {
		// System.out.println("cache " + key + " src:" + rec.getSrcTxNum());
		cmSize.incrementAndGet();
		synchronized (prepareCacheMgrAnchor(key)) {
			RecordVersion rv = new RecordVersion(key, rec.getSrcTxNum());
			cacheRecordMap.put(rv, rec);
			// cacheMgr.cacheRecord(key, rec);
			prepareCacheMgrAnchor(key).notifyAll();
		}
	}

	public void flushToLocalStorage(RecordKey key, long srcTxNum, Transaction tx) {
		// cacheMgr.flushToLocalStorage(key, srcTxNum, tx);
		RecordVersion rv = new RecordVersion(key, srcTxNum);
		CachedRecord rec;
		rec = cacheRecordMap.get(rv);
		if (rec != null)
			flush(key, rec, tx);
	}

	private void flush(RecordKey key, CachedRecord rec, Transaction tx) {

		if (rec.isDeleted())
			LocalRecordMgr.delete(key, tx);
		else if (rec.isNewInserted())
			LocalRecordMgr.insert(key, rec, tx);
		else if (rec.isDirty())
			LocalRecordMgr.update(key, rec, tx);
	}

	public void remove(RecordKey key, long srcTxNum) {
		// System.out.println("remove "+ key+" src:"+srcTxNum);
		// cacheMgr.remove(key, srcTxNum);
		CachedRecord rec = cacheRecordMap.remove(new RecordVersion(key,
				srcTxNum));
		if (rec != null) {
			cmSize.decrementAndGet();
		} else {
		}
	}

	public CachedRecord read(RecordKey key, long srcTxNum, Transaction tx) {

		CachedRecord rec;
		boolean isSelf = (Math.abs(srcTxNum) == tx.getTransactionNumber());

		if (!isSelf) { // not received the remote record yet
			try {
				synchronized (prepareCacheMgrAnchor(key)) {
					RecordVersion rv = new RecordVersion(key, srcTxNum);
					rec = cacheRecordMap.get(rv);
					// rec = cacheMgr.read(key, srcTxNum, tx, isReadFromSink);
					while (rec == null) {
						// System.out.println(tx.getTransactionNumber() +
						// " wait "
						// + srcTxNum + "'s " + key);
						prepareCacheMgrAnchor(key).wait();
						rec = cacheRecordMap.get(rv);
						// rec = cacheMgr.read(key, srcTxNum, tx,
						// isReadFromSink);
					}
				}
				// System.out.println(tx.getTransactionNumber() + " get "
				// + key);
			} catch (InterruptedException e) {
				throw new RuntimeException();
			}
		} else {
			RecordVersion rv = new RecordVersion(key, srcTxNum);
			rec = cacheRecordMap.get(rv);
			// return if the the cache has this record
			if (rec == null) {
				rec = LocalRecordMgr.read(key, tx);
				if (rec == null) {
					rec = LocalRecordMgr.read(key, tx);
					if (rec == null) {
						System.out.println("read record " + key);
						throw new RuntimeException(
								"record read from sink is null");
					}
				}
				cmSize.incrementAndGet();
				synchronized (prepareCacheMgrAnchor(key)) {
					rv.srcTxNum = tx.getTransactionNumber() * -1;
					rec.setSrcTxNum(tx.getTransactionNumber() * -1);
					cacheRecordMap.put(rv, rec);
					prepareCacheMgrAnchor(key).notifyAll();
				}
			}

			// rec = cacheMgr.read(key, srcTxNum, tx, isReadFromSink);

		}

		return rec;
	}

	public void update(RecordKey key, Map<String, Constant> fldVals,
			Transaction tx) {
		// need to update the whole flds of a record
		cmSize.incrementAndGet();
		synchronized (prepareCacheMgrAnchor(key)) {
			RecordVersion rv = new RecordVersion(key, tx.getTransactionNumber());
			CachedRecord rec = new CachedRecord();
			rec.setVals(fldVals);
			rec.setSrcTxNum(tx.getTransactionNumber());
			cacheRecordMap.put(rv, rec);
			// cacheMgr.update(key, fldVals, tx);
			// System.out.println(tx.getTransactionNumber() + " update " + key);
			prepareCacheMgrAnchor(key).notifyAll();
		}

	}

	public void insert(RecordKey key, Map<String, Constant> fldVals,
			Transaction tx) {
		cmSize.incrementAndGet();
		synchronized (prepareCacheMgrAnchor(key)) {
			// cacheMgr.insert(key, fldVals, tx);
			RecordVersion rv = new RecordVersion(key, tx.getTransactionNumber());
			CachedRecord rec = new CachedRecord(fldVals);
			// System.out.println(tx.getTransactionNumber() + " insert " + key);
			rec.setSrcTxNum(tx.getTransactionNumber());
			rec.setNewInserted(true);
			cacheRecordMap.put(rv, rec);
			prepareCacheMgrAnchor(key).notifyAll();
		}
	}

	public void delete(RecordKey key, Transaction tx) {
		// cacheMgr.delete(key, tx);
		cmSize.incrementAndGet();
		RecordVersion rv = new RecordVersion(key, tx.getTransactionNumber());
		CachedRecord dummyRec = new CachedRecord();
		dummyRec.setSrcTxNum(tx.getTransactionNumber());
		dummyRec.delete();
		cacheRecordMap.put(rv, dummyRec);
	}

	private Long getLatestVersionOfRecord(RecordKey key) {
		return latestVersionTable.get(key);
	}

	public void setLatestVersionOfRecord(RecordKey key, Long txn) {
		synchronized (prepareLatestVersionTableAnchor(key)) {

			latestVersionTable.put(key, txn);

		}
		addTxRecord(txn, key);
	}

	// TODO concurrency issue
	public void retainCachedRecord(RecordKey key, long srcTxn) {

		Long version = (Long) srcTxn;
		Map<Long, RetainObject> map = retainTable.get(key);

		if (map == null) {
			if (retainTable.get(key) == null) {
				map = new ConcurrentHashMap<Long, RetainObject>();
				retainTable.put(key, map);
			}
		}

		RetainObject retainObject = map.get(version);
		if (retainObject == null) {
			if (map.get(version) == null) {
				retainObject = new RetainObject();
				map.put(version, retainObject);
			}
		}

		synchronized (retainObject) {
			retainObject.retain();
		}
		// System.out.println(retainObject.getRetainCount() + " retain " +
		// srcTxn + " key: " + key);
	}

	public void releaseCachedRecord(RecordKey key, Long srcTxn) {

		RetainObject retainObject = retainTable.get(key).get(srcTxn);
		// System.out.println(retainObject.getRetainCount() + " beforer " +
		// srcTxn + " key: " + key);
		// System.out.println("Release cached record " + key + " of version "
		// + srcTxn);
		synchronized (retainObject) {
			retainObject.release();
			if (retainObject.getRetainCount() == 0) {
				retainObject.notifyAll();
			}
		}
		// System.out.println(retainObject.getRetainCount() + " release " +
		// srcTxn + " key: " + key);
	}

	public void removeCachedRecord(RecordKey key, Long srcTxn, Transaction tx) {

		// System.out.println("Remove cached record" + key + ", srcTxn: " +
		// srcTxn
		// + ". Latest is " + latestVersionTable.get(key));
		Long latestVersion;
		latestVersion = latestVersionTable.get(key);

		if (srcTxn == null)
			System.out.println("Record: " + key + " is null");

		if (latestVersion != null && srcTxn >= 0
				&& latestVersion.equals(srcTxn)) {
			synchronized (prepareWritebackLockAnchor(key)) {
				// while (latestWritebackLockTable.containsKey(key)) {
				// try {
				// prepareLatestVersionTableAnchor(key).wait();
				// } catch (InterruptedException e) {
				// e.printStackTrace();
				// }
				// }
				// latestWritebackLockTable.put(key, Boolean.TRUE);
				latestVersion = latestVersionTable.get(key);
				if (latestVersion != null && latestVersion.equals(srcTxn)) {
					flushToLocalStorage(key, srcTxn, tx);
				}
			}
		}

		synchronized (prepareLatestVersionTableAnchor(key)) {
			latestVersion = latestVersionTable.get(key);
			if (latestVersion != null && latestVersion.equals(srcTxn)) {
				latestVersionTable.remove(key);
			}
			// latestWritebackLockTable.remove(key);
			// prepareLatestVersionTableAnchor(key).notifyAll();
		}

		// System.out.println("latest version table size: "
		// + latestVersionTable.size());

		RetainObject retainObject = (retainTable.get(key) == null) ? null
				: retainTable.get(key).get(srcTxn);

		if (retainObject != null) {
			synchronized (retainObject) {
				while (retainObject.getRetainCount() > 0) {
					try {
						// System.out.println(retainObject.getRetainCount() +
						// " waiting " + srcTxn + " Key: "+ key);
						retainObject.wait();
						// System.out.println(retainObject.getRetainCount() +
						// " waited " + srcTxn + " Key: "+ key);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}

				// remove version info from retain map
				retainTable.get(key).remove(srcTxn);
			}
		}

		// remove cached record
		remove(key, srcTxn);

		// remove record from txRecordSet
		long txNum = Math.abs(srcTxn);
		boolean isReadFromSink = (srcTxn < 0) ? true : false;
		TxRecordSet recordSet = txRecordMap.get(txNum);
		if (recordSet != null) {
			if (isReadFromSink)
				recordSet.removeReadFromSinkRecord(key);
			else
				recordSet.removeWriteRecord(key);
		}

		// System.out.println("retain table size: " + retainTable.size());

	}

	public long prepareLatestVersionForTransaction(final RecordKey key,
			final long txNum) {
		Long latestVersion;
		synchronized (prepareLatestVersionTableAnchor(key)) {
			latestVersion = getLatestVersionOfRecord(key);
			// if there are no latest version, read from sink by self
			if (latestVersion == null) {
				setLatestVersionOfRecord(key, -txNum);
				latestVersion = -txNum;
				retainCachedRecord(key, latestVersion);
			} else {
				retainCachedRecord(key, latestVersion);
				// retain object should not be null
				// RetainObject ro = retainTable.get(key).get(latestVersion);
				// synchronized (ro) {
				// long oldestTx = removeCounter.get();
				// if (oldestTx > 0
				// && (txNum - oldestTx) * SHADOW_COPY_THRESHOLD >
				// (latestVersion - oldestTx)) {
				// ro.retain();
				// final long srcVersion = latestVersion;
				//
				// final int coef = (latestVersion > 0) ? 1 : -1;
				// final long newVersionNum = txNum * coef;
				// VanillaDdDb.taskMgr().runTask(new Task() {
				// @Override
				// public void run() {
				// synchronized (prepareCacheMgrAnchor(key)) {
				//
				// // System.out.println("FUCK!");
				// RecordVersion rvr = new RecordVersion(key,
				// srcVersion);
				// CachedRecord rec = cacheRecordMap.get(rvr);
				// try {
				// while (rec == null) {
				// prepareCacheMgrAnchor(key).wait();
				// rec = cacheRecordMap.get(rvr);
				// }
				// } catch (InterruptedException e) {
				// e.printStackTrace();
				// }
				// RecordVersion rvu = new RecordVersion(key,
				// newVersionNum);
				// CachedRecord rec2 = new CachedRecord();
				// rec2.setVals(rec.getFldValMap());
				// rec2.setSrcTxNum(newVersionNum);
				// cacheRecordMap.put(rvu, rec2);
				// prepareCacheMgrAnchor(key).notifyAll();
				// }
				// releaseCachedRecord(key, srcVersion);
				// }
				// });
				//
				// setLatestVersionOfRecord(key, newVersionNum);
				// }
				//
				// }

			}
		}

		return latestVersion;

	}

	private void addTxRecord(long txNum, RecordKey key) {

		boolean isReadFromSink = (txNum < 0) ? true : false;
		txNum = Math.abs(txNum);

		TxRecordSet txRecordSet = txRecordMap.get(txNum);

		if (txRecordSet == null) {
			txRecordSet = new TxRecordSet(txNum);
			txRecordMap.put(txNum, txRecordSet);
			// System.out.println("Add record set, txn: " + txNum);
		}

		if (isReadFromSink)
			txRecordSet.addReadFromSinkRecord(key);
		else
			txRecordSet.addWriteRecord(key);
	}

	public void removeOldestTxRecords(Transaction tx) {
		// if (tx.getTransactionNumber() % 1000 == 0) {
		// System.out.println("cachePoolSize: " + cacheRecordMap.size());
		// System.out.println("txRecordMapSize: " + txRecordMap.size());
		// System.out.println("retain table size: " + retainTable.size());
		// System.out.println("latest version table size: "
		// + latestVersionTable.size());
		// System.out.println("Max record: " + MAX_RECORD_COUNT);
		// System.out.println("cmSize: " + cmSize.get());
		// }
		while (cmSize.get() > MAX_RECORD_COUNT) {
			removeTxRecord(removeCounter.getAndIncrement(), tx);
		}

	}

	public void removeTxRecord(long txNum, Transaction tx) {
		TxRecordSet recordSet = txRecordMap.get(txNum);
		if (recordSet != null) {
			for (RecordKey key : recordSet.getWriteRecordSet()) {
				removeCachedRecord(key, txNum, tx);
			}
			for (RecordKey key : recordSet.getReadFromSinkRecordSet()) {
				removeCachedRecord(key, -txNum, tx);
			}
		}

		// System.out.println(txNum + " removed txRecordMap");
		// if (txNum % 1000 == 0) {
		// System.out.println("txRecordMap size: " + txRecordMap.size());
		// }
	}

	class RetainObject {
		private int retainCount = 0;
		private int needCount = 0;

		public RetainObject() {
			retainCount = 0;
		}

		public void retain() {
			retainCount++;
			needCount++;
		}

		public void release() {
			retainCount--;
		}

		public int getRetainCount() {
			return retainCount;
		}

		public int getNeedCount() {
			return needCount;
		}

	}

	class TxRecordSet {
		private long txNum;
		private Set<RecordKey> writeRecordSet = new HashSet<RecordKey>();
		private Set<RecordKey> readFromSinkRecordSet = new HashSet<RecordKey>();
		private int activeRecordCount = 0;

		public TxRecordSet(long txNum) {
			this.txNum = txNum;
		}

		public synchronized void addWriteRecord(RecordKey record) {
			if (writeRecordSet.add(record))
				activeRecordCount++;
		}

		public synchronized void addReadFromSinkRecord(RecordKey record) {
			if (readFromSinkRecordSet.add(record))
				activeRecordCount++;
		}

		public synchronized void removeWriteRecord(RecordKey record) {
			if (writeRecordSet.remove(record))
				activeRecordCount--;
			if (activeRecordCount <= 0)
				removeThisRecordSet();
		}

		public synchronized void removeReadFromSinkRecord(RecordKey record) {
			if (readFromSinkRecordSet.remove(record))
				activeRecordCount--;
			if (activeRecordCount <= 0)
				removeThisRecordSet();
		}

		private void removeThisRecordSet() {
			// System.out.println("Remove record set, txn: " + txNum);
			txRecordMap.remove(this.txNum);
		}

		public synchronized Set<RecordKey> getWriteRecordSet() {
			return new HashSet<RecordKey>(writeRecordSet);
		}

		public synchronized Set<RecordKey> getReadFromSinkRecordSet() {
			return new HashSet<RecordKey>(readFromSinkRecordSet);
		}

		public int getActiveRecordCount() {
			return activeRecordCount;
		}

	}
}
