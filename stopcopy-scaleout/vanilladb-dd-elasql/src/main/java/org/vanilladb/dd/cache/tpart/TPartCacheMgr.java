package org.vanilladb.dd.cache.tpart;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.dd.cache.CacheMgr;
import org.vanilladb.dd.cache.CachedRecord;
import org.vanilladb.dd.cache.calvin.BasicCacheMgr;
import org.vanilladb.dd.sql.RecordKey;
import org.vanilladb.dd.sql.RecordVersion;

public class TPartCacheMgr implements CacheMgr {
	private Map<Integer, SinkCachePool> sinkCachePoolMap;
	private int lastRemovedSinkId;

	public TPartCacheMgr() {
		sinkCachePoolMap = new HashMap<Integer, SinkCachePool>();
	}

	/**
	 * Create a new sink cache pool that contains the flags. A sink cache pool
	 * will be created when some nodes were sunk.
	 */
	public void createSinkCachePool(int sinkId,
			List<RecordVersion> remoteFlags,
			Map<RecordKey, Long> writeBackFlags, List<Long> readingTxs) {
		SinkCachePool scp = new SinkCachePool(remoteFlags, writeBackFlags,
				readingTxs);

		synchronized (sinkCachePoolMap) {
			if (!sinkCachePoolMap.containsKey(sinkId))
				sinkCachePoolMap.put(sinkId, scp);
			else {
				scp = sinkCachePoolMap.get(sinkId);
				scp.reInit(remoteFlags, writeBackFlags, readingTxs);
			}
			sinkCachePoolMap.notify();
		}
	}

	/**
	 * Check if a sink cache pool can be removed. If true, the
	 * SinkFlushProcedure can remove this sink cache pool.
	 */
	public boolean canRemoveSinkPool(int sinkId) {
		/*
		 * Test 2 conditions: 1) The previous sink pool has been removed, which
		 * guarantees that reading from previous sink will get right version. 2)
		 * The reading txs of this sink have finished their reads.
		 */
		SinkCachePool pscp = getSinkCachePool(sinkId - 1);
		if (pscp != null)
			return false;
		SinkCachePool scp = getSinkCachePool(sinkId);
		// System.out.println("pending reader in sink" + sinkId + ": "
		// + scp.readingTxs);
		scp.allReaderFinished();// blocked method
		return true;
	}

	public void canWriteBack(int sinkId) {
		// SinkCachePool scp = getSinkCachePool(sinkId);
		// scp.allReaderFinished();// blocked method

		// do nothing
	}

	public void finishRead(int sinkId, long txNum) {
		SinkCachePool scp = getSinkCachePool(sinkId);
		if (scp != null)
			scp.removeReader(txNum);
	}

	public void removeSinkCachePool(int sinkId) {
		synchronized (sinkCachePoolMap) {
			sinkCachePoolMap.remove(sinkId);
			if (sinkId > lastRemovedSinkId)
				lastRemovedSinkId = sinkId;
		}
	}

	public void cacheRemoteRecord(int sinkId, RecordKey key, long srcTxNum,
			CachedRecord rec) {
		// if that sink already finished, drop this record
		if (sinkId < lastRemovedSinkId)
			return;
		if (rec == null)
			throw new RuntimeException("cache null " + key + " src:" + srcTxNum
					+ " sink:" + sinkId);

		SinkCachePool scp = getAndCreateSinkPoolIfNeed(sinkId);
		scp.cacheMgr.cacheRecord(key, rec);
		synchronized (scp) {
			if (scp.remoteFlags != null)
				scp.remoteFlags.remove(new RecordVersion(key, srcTxNum));
			else
				scp.addEarlyPush(new RecordVersion(key, srcTxNum));
			scp.notifyAll(); // notify all threads waiting on this sink pool
		}
	}

	public void remove(int sinkId, RecordKey key, long srcTxNum) {
		SinkCachePool scp = getSinkCachePool(sinkId);
		scp.cacheMgr.remove(key, srcTxNum);
	}

	public boolean flushToLocalStorage(int sinkId, RecordKey key,
			long srcTxNum, Transaction tx) {
		SinkCachePool scp = getSinkCachePool(sinkId);
		RecordVersion rv = new RecordVersion(key, srcTxNum);
		synchronized (scp) {
			boolean waitRemote = scp.remoteFlags.contains(rv);
			try {
				while (waitRemote) {
					scp.wait();
					// System.out.println("wait remote to flush "
					// + tx.getTransactionNumber());
					waitRemote = scp.remoteFlags.contains(rv);
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
				throw new RuntimeException();
			}
		}
		return scp.cacheMgr.flushToLocalStorage(key, srcTxNum, tx);
	}

	public CachedRecord read(int sinkId, RecordKey key, long srcTxNum,
			Transaction tx) {
		SinkCachePool scp;
		synchronized (sinkCachePoolMap) {
			scp = sinkCachePoolMap.get(sinkId);
			try {
				while (scp == null) {
					sinkCachePoolMap.wait();
					scp = sinkCachePoolMap.get(sinkId);
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
				throw new RuntimeException();
			}
		}

		// hit in local sink pool
		CachedRecord rec = scp.cacheMgr.readCache(key, srcTxNum);
		if (rec != null)
			return rec;

		RecordVersion rv = new RecordVersion(key, srcTxNum);

		// wait if rec is from remote
		synchronized (scp) {
			boolean waitRemote = scp.remoteFlags.contains(rv);
			if (waitRemote)
				try {
					while (waitRemote) {
						scp.wait();
						waitRemote = scp.remoteFlags.contains(rv);
					}
					return scp.cacheMgr.readCache(key, srcTxNum);
				} catch (InterruptedException e) {
					throw new RuntimeException();
				}
		}

		// if is from sink
		if (srcTxNum == -1) {
			rec = readFromPreviousSinkPool(sinkId, key, srcTxNum, tx);
			if (rec != null) {
				CachedRecord recCopy = new CachedRecord(rec.getFldValMap());
				recCopy.setSrcTxNum(srcTxNum);
				scp.cacheMgr.cacheRecord(key, recCopy);
				return recCopy;
			}
		}

		// if no rec can read from cache and is not remote,
		// read the record from
		if (rec == null)
			rec = scp.cacheMgr.read(key, srcTxNum, tx, true);

		// if rec is still null, it may have some bugs
		if (rec == null)
			System.out.println("local read null:" + key + " src:" + srcTxNum
					+ " tx:" + tx.getTransactionNumber());
		return rec;

	}

	private CachedRecord readFromPreviousSinkPool(int sinkId, RecordKey key,
			long srcTxNum, Transaction tx) {
		Integer[] sinkIdList;
		synchronized (sinkCachePoolMap) {
			sinkIdList = sinkCachePoolMap.keySet().toArray(new Integer[0]);
		}
		if (sinkIdList.length <= 1)
			return null;
		Arrays.sort(sinkIdList);

		for (int i = sinkIdList.length - 1; i > 0; i--) {
			if (sinkIdList[i] >= sinkId)
				continue;
			SinkCachePool pscp = getSinkCachePool(sinkIdList[i]);
			if (pscp == null)
				break;

			if (pscp.writeBackFlags.containsKey(key)) {
				long psrcTx = pscp.writeBackFlags.get(key);
				synchronized (pscp) {
					RecordVersion rv = new RecordVersion(key, psrcTx);
					boolean waitRemote = pscp.remoteFlags.contains(rv);
					if (waitRemote)
						try {
							while (waitRemote) {
								// System.out.println("WAIT in old remote " +
								// key
								// + " tx:" + tx.getTransactionNumber()
								// + " srcTx:" + psrcTx);
								pscp.wait();
								waitRemote = pscp.remoteFlags.contains(rv);
							}
						} catch (InterruptedException e) {
							throw new RuntimeException();
						}
				}
				return pscp.cacheMgr.readCache(key, psrcTx);
			}
		}
		return null;
	}

	public void update(int sinkId, RecordKey key,
			Map<String, Constant> fldVals, Transaction tx) {
		// need to update the whole flds of a record
		SinkCachePool scp = getSinkCachePool(sinkId);
		scp.cacheMgr.update(key, fldVals, tx);
	}

	public void insert(int sinkId, RecordKey key,
			Map<String, Constant> fldVals, Transaction tx) {
		SinkCachePool scp = getSinkCachePool(sinkId);
		scp.cacheMgr.insert(key, fldVals, tx);
	}

	public void delete(int sinkId, RecordKey key, Transaction tx) {
		SinkCachePool scp = getSinkCachePool(sinkId);
		scp.cacheMgr.delete(key, tx);
	}

	public void doWriteback(int sinkId, Transaction tx) {
		SinkCachePool scp = getSinkCachePool(sinkId);
		long txNum = tx.getTransactionNumber();

	}

	private SinkCachePool getSinkCachePool(int sinkId) {
		synchronized (sinkCachePoolMap) {
			return sinkCachePoolMap.get(sinkId);
		}
	}

	private SinkCachePool getAndCreateSinkPoolIfNeed(int sinkId) {
		synchronized (sinkCachePoolMap) {
			SinkCachePool scp = sinkCachePoolMap.get(sinkId);
			if (scp == null) {
				scp = new SinkCachePool();
				sinkCachePoolMap.put(sinkId, scp);
			}
			return scp;
		}
	}

	class SinkCachePool {
		private List<RecordVersion> remoteFlags;
		private Map<RecordKey, Long> writeBackFlags;
		List<Long> readingTxs;
		BasicCacheMgr cacheMgr = new BasicCacheMgr();
		private List<RecordVersion> earlyPush;

		SinkCachePool() {
			earlyPush = new ArrayList<RecordVersion>();
		}

		SinkCachePool(List<RecordVersion> remoteFlags,
				Map<RecordKey, Long> writeBackFlags, List<Long> readingTxs) {
			this.remoteFlags = remoteFlags;
			this.writeBackFlags = writeBackFlags;
			this.readingTxs = readingTxs;
		}

		synchronized void reInit(List<RecordVersion> remoteFlags,
				Map<RecordKey, Long> writeBackFlags, List<Long> readingTxs) {
			this.writeBackFlags = writeBackFlags;
			this.readingTxs = readingTxs;
			if (earlyPush != null)
				remoteFlags.removeAll(earlyPush);
			this.remoteFlags = remoteFlags;
		}

		synchronized void addEarlyPush(RecordVersion rv) {
			earlyPush.add(rv);
		}

		synchronized boolean hasNoMoreReader() {
			return readingTxs == null || readingTxs.size() == 0;
		}

		synchronized void removeReader(long txNum) {
			readingTxs.remove(txNum);
			this.notifyAll();
		}

		synchronized void allReaderFinished() {
			if (readingTxs == null)
				return;
			try {
				while (readingTxs.size() > 0) {

					this.wait();

				}
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
