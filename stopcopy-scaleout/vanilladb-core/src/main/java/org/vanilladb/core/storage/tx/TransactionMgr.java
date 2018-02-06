package org.vanilladb.core.storage.tx;

import java.lang.reflect.Constructor;
import java.sql.Connection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.storage.tx.concurrency.ConcurrencyMgr;
import org.vanilladb.core.storage.tx.concurrency.ReadCommittedConcurrencyMgr;
import org.vanilladb.core.storage.tx.concurrency.RepeatableReadConcurrencyMgr;
import org.vanilladb.core.storage.tx.concurrency.SerializableConcurrencyMgr;
import org.vanilladb.core.storage.tx.recovery.RecoveryMgr;

/**
 * The publicly-accessible transaction manager in VanillaDb. This transaction
 * manager is responsible for creating new transaction and maintaining the
 * active transaction list.
 */
public class TransactionMgr implements TransactionLifecycleListener {
	private static Logger logger = Logger.getLogger(TransactionMgr.class
			.getName());
	public static Class<?> serialConcurMgrCls, rrConcurMgrCls, rcConcurMgrCls,
			recoveryMgrCls;
	static {
		String prop = System.getProperty(TransactionMgr.class.getName()
				+ ".SERIALIZABLE_CONCUR_MGR");
		if (prop != null)
			try {
				serialConcurMgrCls = Class.forName(prop.trim());
			} catch (ClassNotFoundException e) {
				// keep cls null
			}

		prop = System.getProperty(TransactionMgr.class.getName()
				+ ".REPEATABLE_READ_CONCUR_MGR");
		if (prop != null)
			try {
				rrConcurMgrCls = Class.forName(prop.trim());
			} catch (ClassNotFoundException e) {
				// keep cls null
			}

		prop = System.getProperty(TransactionMgr.class.getName()
				+ ".READ_COMMITTED_CONCUR_MGR");
		if (prop != null)
			try {
				rcConcurMgrCls = Class.forName(prop.trim());
			} catch (ClassNotFoundException e) {
				// keep cls null
			}

		prop = System.getProperty(TransactionMgr.class.getName()
				+ ".RECOVERY_MGR");
		if (prop != null)
			try {
				recoveryMgrCls = Class.forName(prop.trim());
			} catch (ClassNotFoundException e) {
				// keep cls null
			}
	}
	private Long[] threadTxNums = new Long[10000];
	private ReentrantReadWriteLock activeTxsLock = new ReentrantReadWriteLock();
	private static long nextTxNum = 0;
	private static Object txNumLock = new Object(); // Optimization: Use
													// separate lock for
													// nextTxNum

	public TransactionMgr() {
		for (int i = 0; i < 10000; i++)
			threadTxNums[i] = -1L;

	}

	// public synchronized ArrayList<Transaction> getActiveTransactions() {
	// return activeTxs;
	// }

	@Override
	public void onTxCommit(Transaction tx) {
		activeTxsLock.readLock().lock();
		try {
			threadTxNums[(int) Thread.currentThread().getId()] = -1L;
		} finally {
			activeTxsLock.readLock().unlock();
		}

	}

	@Override
	public void onTxRollback(Transaction tx) {
		activeTxsLock.readLock().lock();
		try {
			threadTxNums[(int) Thread.currentThread().getId()] = -1L;
		} finally {
			activeTxsLock.readLock().unlock();
		}
	}

	@Override
	public void onTxEndStatement(Transaction tx) {
		// do nothing
	}

	/**
	 * Creates non-quiescent checkpoint record.
	 * 
	 * @param checkpointTx
	 *            the transaction that performs checkpointing
	 */
	public void createCheckpoint(Transaction checkpointTx) {
		// stop access new tx request and find out active txs
		List<Long> txNums = new LinkedList<Long>();
		// for (Transaction tx : activeTxs)
		// if (tx.getTransactionNumber() != checkpointTx
		// .getTransactionNumber())
		// txNums.add(tx.getTransactionNumber());

		// XXX critical section unchecked
		activeTxsLock.writeLock().lock();
		try {
			for (Long l : threadTxNums) {
				if (l >= 0) {
					txNums.add(l);
				}
			}

			// flush all buffers
			VanillaDb.bufferMgr().flushAll();
			// wrtie a checkpoint record and flush to disk
			long lsn = checkpointTx.recoveryMgr().checkpoint(txNums);
			VanillaDb.logMgr().flush(lsn);
		} finally {
			activeTxsLock.writeLock().unlock();
		}
	}

	public Transaction transaction(int isolationLevel, boolean readOnly) {
		// Dispatch new transaction number
		long txNum = -1;
		synchronized (txNumLock) {
			txNum = nextTxNum;
			nextTxNum++;
		}
		return transaction(isolationLevel, readOnly, txNum);
	}

	public Transaction transaction(int isolationLevel, boolean readOnly,
			long txNum) {
		// Update next transaction number
		synchronized (txNumLock) {
			if (txNum >= nextTxNum)
				nextTxNum = txNum + 1;
		}
		return newTransaction(isolationLevel, readOnly, txNum);
	}

	public long getNextTxNum() {
		synchronized (txNumLock) {
			return nextTxNum;
		}
	}

	private Transaction newTransaction(int isolationLevel, boolean readOnly,
			long txNum) {
		if (logger.isLoggable(Level.FINE))
			logger.fine("new transaction: " + txNum);

		RecoveryMgr recoveryMgr = null;
		if (recoveryMgrCls != null)
			try {
				Class<?> partypes[] = new Class[2];
				partypes[0] = Long.TYPE;
				partypes[1] = Boolean.TYPE;
				Constructor<?> ct = recoveryMgrCls.getConstructor(partypes);
				recoveryMgr = (RecoveryMgr) ct.newInstance(new Long(txNum),
						new Boolean(readOnly));
			} catch (Exception e) {
				// do nothing
			}
		if (recoveryMgr == null) {
			if (logger.isLoggable(Level.WARNING))
				logger.warning("no recovery mgr found, using default");
			recoveryMgr = new RecoveryMgr(txNum, readOnly);
		}

		ConcurrencyMgr concurMgr = null;
		switch (isolationLevel) {
		case Connection.TRANSACTION_SERIALIZABLE:
			if (serialConcurMgrCls != null)
				try {
					Class<?> partypes[] = new Class[1];
					partypes[0] = Long.TYPE;
					Constructor<?> ct = serialConcurMgrCls
							.getConstructor(partypes);
					concurMgr = (ConcurrencyMgr) ct
							.newInstance(new Long(txNum));
				} catch (Exception e) {
					// do nothing
				}
			if (concurMgr == null) {
				if (logger.isLoggable(Level.WARNING))
					logger.warning("no serializable concurrency mgr found, using default");
				concurMgr = new SerializableConcurrencyMgr(txNum);
			}
			break;
		case Connection.TRANSACTION_REPEATABLE_READ:
			if (rrConcurMgrCls != null)
				try {
					Class<?> partypes[] = new Class[1];
					partypes[0] = Long.TYPE;
					Constructor<?> ct = rrConcurMgrCls.getConstructor(partypes);
					concurMgr = (ConcurrencyMgr) ct
							.newInstance(new Long(txNum));
				} catch (Exception e) {
					// do nothing
				}
			if (concurMgr == null) {
				if (logger.isLoggable(Level.WARNING))
					logger.warning("no repeatable read concurrency mgr found, using default");
				concurMgr = new RepeatableReadConcurrencyMgr(txNum);
			}
			break;
		case Connection.TRANSACTION_READ_COMMITTED:
			if (rcConcurMgrCls != null)
				try {
					Class<?> partypes[] = new Class[1];
					partypes[0] = Long.TYPE;
					Constructor<?> ct = rcConcurMgrCls.getConstructor(partypes);
					concurMgr = (ConcurrencyMgr) ct
							.newInstance(new Long(txNum));
				} catch (Exception e) {
					// do nothing
				}
			if (concurMgr == null) {
				if (logger.isLoggable(Level.WARNING))
					logger.warning("no read committed concurrency mgr found, using default");
				concurMgr = new ReadCommittedConcurrencyMgr(txNum);
			}
			break;
		default:
			throw new UnsupportedOperationException(
					"unsupported isolation level");
		}

		Transaction tx = new Transaction(concurMgr, recoveryMgr, readOnly,
				txNum);
		tx.addLifecycleListener(this);

		activeTxsLock.readLock().lock();
		try {
			threadTxNums[(int) Thread.currentThread().getId()] = txNum;
		} finally {
			activeTxsLock.readLock().unlock();
		}

		// synchronized (this) {
		// activeTxs.add(tx);
		// }
		return tx;
	}
}
