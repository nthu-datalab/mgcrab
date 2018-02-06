package org.vanilladb.dd.cache.calvin;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.dd.cache.CachedRecord;
import org.vanilladb.dd.cache.LocalRecordMgr;
import org.vanilladb.dd.sql.RecordKey;
import org.vanilladb.dd.sql.RecordVersion;

public class BasicCacheMgr {
	private static final int INITIAL_CACHE_CAPACITY = 256;

	private Map<RecordVersion, CachedRecord> cacheRecordMap;

	// <RecordKey, srcTxNum> -> Record Value

	public BasicCacheMgr() {
		this.cacheRecordMap = new ConcurrentHashMap<RecordVersion, CachedRecord>(
				INITIAL_CACHE_CAPACITY);
		
//		new PeriodicalJob(1000, 1400000, new Runnable() {
//
//			@Override
//			public void run() {
//				System.out.println("Map Size: " + cacheRecordMap.size());
//				// Check first 5 records
//				int i = 0;
//				for (RecordVersion rv : cacheRecordMap.keySet()) {
//					System.out.println(rv);
//					
//					i++;
//					if (i > 5)
//						break;
//				}
//			}
//			
//		}).start();
	}

	public void cacheRecord(RecordKey key, CachedRecord rec) {
		RecordVersion rv = new RecordVersion(key, rec.getSrcTxNum());
		cacheRecordMap.put(rv, rec);
	}

	public boolean flushToLocalStorage(RecordKey key, long srcTxNum,
			Transaction tx) {
		RecordVersion rv = new RecordVersion(key, srcTxNum);
		CachedRecord rec;
		rec = cacheRecordMap.get(rv);
		if (rec == null)
			return false;
		else
			flush(key, rec, tx);
		return true;
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
		cacheRecordMap.remove(new RecordVersion(key, srcTxNum));

	}

	public CachedRecord readCache(RecordKey key, long srcTxNum) {
		RecordVersion rv = new RecordVersion(key, srcTxNum);
		return cacheRecordMap.get(rv);
	}

	public CachedRecord read(RecordKey key, long srcTxNum, Transaction tx,
			boolean isReadFromSink) {
		RecordVersion rv = new RecordVersion(key, srcTxNum);
		CachedRecord rec;
		rec = cacheRecordMap.get(rv);

		// return if the the cache has this record
		if (rec != null || !isReadFromSink)
			return rec;

		rec = LocalRecordMgr.read(key, tx);

		if (rec != null) {
			rv.srcTxNum = tx.getTransactionNumber();
			rec.setSrcTxNum(tx.getTransactionNumber());
			cacheRecordMap.put(rv, rec);
		}
		// System.out
		// .println(tx.getTransactionNumber() + " read from sink " + key);
		return rec;
	}

	/* TODO: Deprecated
	public void update(RecordKey key, Map<String, Constant> fldVals,
			Transaction tx) {
		RecordVersion rv = new RecordVersion(key, tx.getTransactionNumber());
		CachedRecord rec = new CachedRecord();
		rec.setVals(fldVals);
		rec.setSrcTxNum(tx.getTransactionNumber());
		cacheRecordMap.put(rv, rec);
	}
	*/
	
	public void update(RecordKey key, CachedRecord rec,	Transaction tx) {
		RecordVersion rv = new RecordVersion(key, tx.getTransactionNumber());
		rec.setSrcTxNum(tx.getTransactionNumber());
		cacheRecordMap.put(rv, rec);
	}

	public void insert(RecordKey key, Map<String, Constant> fldVals,
			Transaction tx) {
		RecordVersion rv = new RecordVersion(key, tx.getTransactionNumber());
		CachedRecord rec = new CachedRecord(fldVals);
		rec.setSrcTxNum(tx.getTransactionNumber());
		rec.setNewInserted(true);
		cacheRecordMap.put(rv, rec);
	}

	public void delete(RecordKey key, Transaction tx) {
		RecordVersion rv = new RecordVersion(key, tx.getTransactionNumber());
		CachedRecord dummyRec = new CachedRecord();
		dummyRec.setSrcTxNum(tx.getTransactionNumber());
		dummyRec.delete();
		cacheRecordMap.put(rv, dummyRec);
	}
	
	public void markWriteback(RecordKey key, Transaction tx) {
		RecordVersion rv = new RecordVersion(key, tx.getTransactionNumber());
		CachedRecord rec = cacheRecordMap.get(rv);
//		if (key.getTableName().equals("district"))
//			System.out.println("Tx." + tx.getTransactionNumber() + " mark record " + key + " as inserted.");
		rec.setNewInserted(true);
		cacheRecordMap.put(rv, rec);
	}
}
