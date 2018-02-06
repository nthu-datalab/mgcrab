package org.vanilladb.core.storage.tx.recovery;

import java.sql.Connection;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.server.task.Task;
import org.vanilladb.core.storage.tx.Transaction;

/**
 * The task performs non-quiescent checkpointing.
 */
public class CheckpointTask extends Task {
	private static Logger logger = Logger.getLogger(CheckpointTask.class
			.getName());

	private static final int TX_COUNT_TO_CHECKPOINT;
	private static final int METHOD_PERIODIC = 0, METHOD_MONITOR = 1;
	private static final int MY_METHOD;
	private static final long PERIOD;
	private long lastTxNum;

	static {
		String prop = System.getProperty(CheckpointTask.class.getName()
				+ ".TX_COUNT_TO_CHECKPOINT");
		TX_COUNT_TO_CHECKPOINT = (prop == null ? 100 : Integer.parseInt(prop
				.trim()));
		prop = System
				.getProperty(CheckpointTask.class.getName() + ".MY_METHOD");
		MY_METHOD = (prop == null ? METHOD_PERIODIC : Integer.parseInt(prop
				.trim()));
		prop = System.getProperty(CheckpointTask.class.getName() + ".PERIOD");
		PERIOD = (prop == null ? 300000 : Long.parseLong(prop.trim()));
	}

	public CheckpointTask() {

	}

	/**
	 * Create a non-quiescent checkpoint.
	 */
	public void createCheckpoint() {
		if (logger.isLoggable(Level.FINE))
			logger.info("Start creating checkpoint");
		if (MY_METHOD == METHOD_MONITOR) {
			if (VanillaDb.txMgr().getNextTxNum() - lastTxNum > TX_COUNT_TO_CHECKPOINT) {
				Transaction tx = VanillaDb.txMgr().transaction(
						Connection.TRANSACTION_SERIALIZABLE, false);
				VanillaDb.txMgr().createCheckpoint(tx);
				tx.commit();
				lastTxNum = VanillaDb.txMgr().getNextTxNum();
			}
		} else if (MY_METHOD == METHOD_PERIODIC) {
			Transaction tx = VanillaDb.txMgr().transaction(
					Connection.TRANSACTION_SERIALIZABLE, false);
			VanillaDb.txMgr().createCheckpoint(tx);
			tx.commit();
		}
	}

	@Override
	public void run() {
		while (true) {
			createCheckpoint();
			try {
				Thread.sleep(PERIOD);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}
