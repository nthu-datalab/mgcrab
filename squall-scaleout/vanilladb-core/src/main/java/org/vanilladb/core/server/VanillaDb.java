package org.vanilladb.core.server;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.vanilladb.core.query.planner.Planner;
import org.vanilladb.core.query.planner.QueryPlanner;
import org.vanilladb.core.query.planner.UpdatePlanner;
import org.vanilladb.core.query.planner.index.IndexUpdatePlanner;
import org.vanilladb.core.query.planner.opt.HeuristicQueryPlanner;
import org.vanilladb.core.server.task.TaskMgr;
import org.vanilladb.core.storage.buffer.BufferMgr;
import org.vanilladb.core.storage.buffer.BufferMgrImpl;
import org.vanilladb.core.storage.buffer.DummyBufferMgr;
import org.vanilladb.core.storage.file.FileMgr;
import org.vanilladb.core.storage.log.LogMgr;
import org.vanilladb.core.storage.metadata.CatalogMgr;
import org.vanilladb.core.storage.metadata.statistics.StatMgr;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.storage.tx.TransactionMgr;
import org.vanilladb.core.storage.tx.recovery.CheckpointTask;
import org.vanilladb.core.storage.tx.recovery.RecoveryMgr;
import org.vanilladb.core.util.Profiler;

/**
 * The class that provides system-wide static global values. These values must
 * be initialized by the method {@link #init(String) init} before use. The
 * methods {@link #initFileMgr(String) initFileMgr},
 * {@link #initFileAndLogMgr(String) initFileAndLogMgr},
 * {@link #initFileLogAndBufferMgr(String) initFileLogAndBufferMgr}, and
 * {@link #initCatalogMgr(boolean, Transaction) initCatalogMgr} provide limited
 * initialization, and are useful for debugging purposes.
 */
public class VanillaDb {
	/**
	 * The class of stored procedure factory.
	 */
	public static Class<?> spFactoryCls;

	private static Logger logger = Logger.getLogger(VanillaDb.class.getName());
	private static Class<?> queryPlannerCls, updatePlannerCls;

	private static FileMgr fileMgr;
	private static BufferMgr buffMgr;
	private static LogMgr logMgr;
	private static CatalogMgr catalogMgr;
	private static StatMgr statMgr;
	private static TaskMgr taskMgr;
	private static TransactionMgr txMgr;
	private static Profiler profiler;

	private static boolean inited;

	/**
	 * Initializes the system. This method is called during system startup.
	 * 
	 * @param dirName
	 *            the name of the database directory
	 */
	public static void init(String dirName, int bufferType) {
		if (inited) {
			if (logger.isLoggable(Level.WARNING))
				logger.warning("discarding duplicated init request");
			return;
		}

		// read config file
		boolean config = false;
		String path = System.getProperty("org.vanilladb.core.config.file");
		if (path != null) {
			FileInputStream fis = null;
			try {
				fis = new FileInputStream(path);
				System.getProperties().load(fis);
				config = true;
			} catch (IOException e) {
				// do nothing
			} finally {
				try {
					if (fis != null)
						fis.close();
				} catch (IOException e) {
					// do nothing
				}
			}
		}
		if (!config && logger.isLoggable(Level.WARNING))
			logger.warning("error reading the config file, using defaults");

		// read properties
		String prop = System.getProperty(VanillaDb.class.getName()
				+ ".QUERYPLANNER");

		if (prop != null)
			try {
				queryPlannerCls = Class.forName(prop.trim());
			} catch (ClassNotFoundException e) {
				// keep cls null
			}

		prop = System.getProperty(VanillaDb.class.getName() + ".UPDATEPLANNER");

		if (prop != null)
			try {
				updatePlannerCls = Class.forName(prop.trim());
			} catch (ClassNotFoundException e) {
				// keep cls null
			}

		prop = System.getProperty(VanillaDb.class.getName() + ".SP_FACTORY");

		if (prop != null)
			try {
				spFactoryCls = Class.forName(prop.trim());
			} catch (ClassNotFoundException e) {
				// keep cls null
			}
		if (spFactoryCls == null)
			try {
				spFactoryCls = Class
						.forName("org.vanilladb.core.sql.storedprocedure.StoredProcedureFactory");
			} catch (ClassNotFoundException e) {
				if (logger.isLoggable(Level.SEVERE))
					logger.severe("cannot find stored proc. factory class");
			}

		initFileLogAndBufferMgr(dirName, bufferType);
		initTaskMgr();
		initTxMgr();
		// the first transaction for initialize the system
		Transaction initTx = txMgr.transaction(
				Connection.TRANSACTION_SERIALIZABLE, false);

		boolean isnew = fileMgr.isNew();

		/*
		 * initialize the catalog manager to ensure the recovery process can get
		 * the index info (required for index logical recovery)
		 */
		initCatalogMgr(isnew, initTx);
		if (isnew) {
			if (logger.isLoggable(Level.INFO))
				logger.info("creating new database");
		} else {
			if (logger.isLoggable(Level.INFO))
				logger.info("recovering existing database");
			// add a checkpoint record to limit rollback
			RecoveryMgr.recover(initTx);
			logMgr.removeAndCreateNewLog();
		}

		// initialize the statistics manager to build the histogram
		initStatMgr(initTx);

		initTx.commit();

		prop = System.getProperty(VanillaDb.class.getName() + ".DO_CHECKPOINT");
		boolean doCheckpointing = (prop == null) ? true : Boolean
				.parseBoolean(prop.trim());
		if (doCheckpointing)
			initCheckpointingTask();
		inited = true;
	}

	public static boolean isInited() {
		return inited;
	}

	/*
	 * The following initialization methods are useful for testing the
	 * lower-level components of the system without having to initialize
	 * everything.
	 */

	/**
	 * Initializes only the file manager.
	 * 
	 * @param dirName
	 *            the name of the database directory
	 */
	public static void initFileMgr(String dirName) {
		fileMgr = new FileMgr(dirName);
	}

	/**
	 * Initializes the file and log managers.
	 * 
	 * @param dirName
	 *            the name of the database directory
	 */
	public static void initFileAndLogMgr(String dirName) {
		initFileMgr(dirName);
		logMgr = new LogMgr();
	}

	/**
	 * Initializes the file, log, and buffer managers.
	 * 
	 * @param dirName
	 *            the name of the database directory
	 */
	public static void initFileLogAndBufferMgr(String dirName, int bufferMgrType) {
		initFileAndLogMgr(dirName);

		if (bufferMgrType == 1)
			buffMgr = new BufferMgrImpl();
		else
			buffMgr = new DummyBufferMgr();

		Transaction.addStartListener(buffMgr);
	}

	/**
	 * Initializes the task manager.
	 */
	public static void initTaskMgr() {
		taskMgr = new TaskMgr();
	}

	/**
	 * Initializes the transaction manager.
	 */
	public static void initTxMgr() {
		txMgr = new TransactionMgr();
	}

	/**
	 * Initializes the catalog manager. Note that the catalog manager should be
	 * initialized <em>before</em> system recovery.
	 * 
	 * @param isNew
	 *            an indication of whether a new database needs to be created.
	 * @param tx
	 *            the transaction performing the initialization
	 */
	public static void initCatalogMgr(boolean isNew, Transaction tx) {
		catalogMgr = new CatalogMgr(isNew, tx);
	}

	/**
	 * Initializes the statistics manager. Note that this manager should be
	 * initialized <em>after</em> system recovery.
	 * 
	 * @param tx
	 *            the transaction performing the initialization
	 */
	public static void initStatMgr(Transaction tx) {
		statMgr = new StatMgr(tx);
	}

	public static void initCheckpointingTask() {
		taskMgr.runTask(new CheckpointTask());
	}

	public static FileMgr fileMgr() {
		return fileMgr;
	}

	public static BufferMgr bufferMgr() {
		return buffMgr;
	}

	public static LogMgr logMgr() {
		return logMgr;
	}

	public static CatalogMgr catalogMgr() {
		return catalogMgr;
	}

	public static StatMgr statMgr() {
		return statMgr;
	}

	public static TaskMgr taskMgr() {
		return taskMgr;
	}

	public static TransactionMgr txMgr() {
		return txMgr;
	}

	/**
	 * Creates a planner for SQL commands. To change how the planner works,
	 * modify this method.
	 * 
	 * @return the system's planner for SQL commands
	 */
	public static Planner planner() {
		QueryPlanner qplanner = null;
		if (queryPlannerCls != null)
			try {
				qplanner = (QueryPlanner) queryPlannerCls.newInstance();
			} catch (Exception e) {
				// do nothing
			}
		if (qplanner == null) {
			if (logger.isLoggable(Level.WARNING))
				logger.warning("no query planner found, using default");
			qplanner = new HeuristicQueryPlanner();
		}

		UpdatePlanner uplanner = null;
		if (updatePlannerCls != null)
			try {
				uplanner = (UpdatePlanner) updatePlannerCls.newInstance();
			} catch (Exception e) {
				// do nothing
			}
		if (uplanner == null) {
			if (logger.isLoggable(Level.WARNING))
				logger.warning("no update planner found, using default");
			uplanner = new IndexUpdatePlanner();
		}
		return new Planner(qplanner, uplanner);
	}

	/**
	 * Initialize profiler and start collecting profiling data.
	 */
	public static void initAndStartProfiler() {
		profiler = new Profiler();
		profiler.startCollecting();
	}

	/**
	 * Stop profiler and generate report file.
	 */
	public static void stopProfilerAndReport() {
		profiler.stopCollecting();

		// Write a report file
		try {
			// Get path from property file
			String path = System.getProperty(VanillaDb.class.getName()
					+ ".PROFILE_OUTPUT_DIR");
			if (path == null || path.isEmpty())
				path = System.getProperty("user.home");
			File out = new File(path, System.currentTimeMillis()
					+ "_profile.txt");
			FileWriter wrFile = new FileWriter(out);
			BufferedWriter bwrFile = new BufferedWriter(wrFile);

			// Write Profiling Report
			bwrFile.write(profiler.getTopPackages(30));
			bwrFile.newLine();
			bwrFile.write(profiler.getTopMethods(30));
			bwrFile.newLine();
			bwrFile.write(profiler.getTopLines(30));

			/*
			 * I should write a more careful code here. I didn't do it, because
			 * of the same reason above.
			 */
			bwrFile.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
