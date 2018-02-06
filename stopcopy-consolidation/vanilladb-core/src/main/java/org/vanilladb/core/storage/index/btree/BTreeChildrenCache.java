package org.vanilladb.core.storage.index.btree;

import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.vanilladb.core.storage.buffer.Buffer;
import org.vanilladb.core.storage.buffer.PageMetadata;
import org.vanilladb.core.storage.file.BlockId;

public class BTreeChildrenCache implements PageMetadata {

	// Root page cache
	private static class RootCacheRecord {
		String fileName = null;
		Buffer cachedBuffer = null;
	}

	// TODO: The size of array might be adjusted
	private static RootCacheRecord[] cachedRootBuffers = new RootCacheRecord[100];
	private static ReentrantReadWriteLock cachedRootLock = new ReentrantReadWriteLock();

	public static Buffer searchRootCache(String fileName) {
		Buffer buf = null;

		cachedRootLock.readLock().lock();
		for (int i = 0; i < cachedRootBuffers.length; i++) {
			if (cachedRootBuffers[i] == null)
				break;

			if (cachedRootBuffers[i].fileName.equals(fileName)) {
				buf = cachedRootBuffers[i].cachedBuffer;
				break;
			}
		}
		cachedRootLock.readLock().unlock();

		return buf;
	}

	public static void addRootCache(BlockId rootBlk, Buffer buffer) {
		cachedRootLock.writeLock().lock();
		boolean hit = false;
		int lastEmptyPos;

		for (lastEmptyPos = 0; lastEmptyPos < cachedRootBuffers.length; lastEmptyPos++) {
			if (cachedRootBuffers[lastEmptyPos] == null)
				break;

			if (cachedRootBuffers[lastEmptyPos].fileName.equals(rootBlk
					.fileName())) {
				cachedRootBuffers[lastEmptyPos].cachedBuffer = buffer;
				hit = true;
				break;
			}
		}

		if (lastEmptyPos >= cachedRootBuffers.length)
			throw new RuntimeException("index root buffer cahce is too small");

		if (!hit) {
			RootCacheRecord newRecord = new RootCacheRecord();
			newRecord.fileName = rootBlk.fileName();
			newRecord.cachedBuffer = buffer;
			cachedRootBuffers[lastEmptyPos] = newRecord;
		}
		cachedRootLock.writeLock().unlock();
	}

	// ==========================================================

	private Buffer parent;
	private Buffer ownerBuf;
	private Buffer[] cache;

	private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();

	public BTreeChildrenCache(Buffer ownerBuf, int cacheSize) {
		this.ownerBuf = ownerBuf;
		this.cache = new Buffer[cacheSize];
	}

	public Buffer getChild(int slot) {
		readWriteLock.readLock().lock();
		try {
			return cache[slot];
		} finally {
			readWriteLock.readLock().unlock();
		}
	}

	public void setChild(int slot, Buffer childBuf) {
		readWriteLock.writeLock().lock();
		try {
			// Set the pointer which points to the child
			cache[slot] = childBuf;
			
			// Set the pointer which points to this buffer from the child
			PageMetadata childMeta = childBuf.getPageMetadata();
			if (childMeta != null) {
				BTreeChildrenCache childCache = (BTreeChildrenCache) childMeta;
				childCache.deleteChild(ownerBuf);
			}
		} finally {
			readWriteLock.writeLock().unlock();
		}
	}

	public Buffer deleteChild(int slot) {
		readWriteLock.writeLock().lock();
		try {
			Buffer deletedBuffer = cache[slot];
			cache[slot] = null;
			return deletedBuffer;
		} finally {
			readWriteLock.writeLock().unlock();
		}
	}

	public void deleteChild(Buffer child) {
		readWriteLock.writeLock().lock();
		try {
			for (int i = 0; i < cache.length; i++)
				if (cache[i] == child)
					cache[i] = null;
		} finally {
			readWriteLock.writeLock().unlock();
		}
	}

	public void copyChild(int from, int to) {
		readWriteLock.writeLock().lock();
		try {
			cache[to] = cache[from];
		} finally {
			readWriteLock.writeLock().unlock();
		}
	}

	@Override
	public void clean(Buffer ownerBuffer) {
		readWriteLock.writeLock().lock();
		try {
			if (parent != null) {
				// Get the meta data of the parent
				PageMetadata parentMeta = parent.getPageMetadata();
				if (parentMeta != null) {
					BTreeChildrenCache parentCache = (BTreeChildrenCache) parentMeta;
					parentCache.deleteChild(ownerBuffer);
				}

				// Get the meta data of the children
				for (int i = 0; i < cache.length; i++) {
					if (cache[i] != null && cache[i].getPageMetadata() != null) {
						BTreeChildrenCache childCache = (BTreeChildrenCache) cache[i]
								.getPageMetadata();
						childCache.setParent(null);
					}
				}
			}

		} finally {
			readWriteLock.writeLock().unlock();
		}
	}
	
	private void setParent(Buffer parent) {
		readWriteLock.writeLock().lock();
		try {
			this.parent = parent;
		} finally {
			readWriteLock.writeLock().unlock();
		}
	}
}
