package org.vanilladb.dd.server;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.dd.cache.CacheMgr;
import org.vanilladb.dd.cache.calvin.CalvinCacheMgr;
import org.vanilladb.dd.cache.oda.OdaCacheMgr;
import org.vanilladb.dd.cache.tpart.NewTPartCacheMgr;
import org.vanilladb.dd.remote.groupcomm.server.ConnectionMgr;
import org.vanilladb.dd.schedule.Scheduler;
import org.vanilladb.dd.schedule.calvin.CalvinScheduler;
import org.vanilladb.dd.schedule.oda.OdaScheduler;
import org.vanilladb.dd.schedule.tpart.HeuristicNodeInserter;
import org.vanilladb.dd.schedule.tpart.TGraph;
import org.vanilladb.dd.schedule.tpart.TPartPartitioner;
import org.vanilladb.dd.schedule.tpart.TPartTaskScheduler;
import org.vanilladb.dd.schedule.tpart.sink.CacheOptimizedSinker;
import org.vanilladb.dd.server.migration.MigrationManager;
import org.vanilladb.dd.storage.log.DdLogMgr;
import org.vanilladb.dd.storage.metadata.HashBasedPartitionMetaMgr;
import org.vanilladb.dd.storage.metadata.PartitionMetaMgr;

public class VanillaDdDb extends VanillaDb {
	private static Logger logger = Logger.getLogger(VanillaDb.class.getName());
	
	public static final long START_TIME = System.nanoTime();
	
	private static final String SERIAL_NAME = "CatFish";
	
	/**
	 * The type of transactional execution engine supported by distributed
	 * deterministic VanillaDB.
	 */
	public enum ServiceType {
		FULLY_REPLICATED, CALVIN, ODA, TPART
	}

	private static ServiceType serviceType;

	// dd modules
	private static ConnectionMgr connMgr;
	private static PartitionMetaMgr parMetaMgr;
	// private static ReplicationAndPartitionMetaMgr rparMgr;
	private static CacheMgr cacheMgr;
	private static Scheduler scheduler;
	private static TPartTaskScheduler tpartTaskScheduler;
	private static DdLogMgr ddLogMgr;
	private static MigrationManager migrateMgr;

	// connection information
	private static int myNodeId;

	// private static boolean hasReordering, hasReplication;

	/**
	 * Initializes the system. This method is called during system startup.
	 * 
	 * @param dirName
	 *            the name of the database directory
	 * @param id
	 *            the id of the server
	 */
	public static void init(String dirName, int id) {
		myNodeId = id;

		if (logger.isLoggable(Level.INFO))
			logger.info("vanilladddb initializing...");
		
		if (logger.isLoggable(Level.INFO))
			logger.info("Current version: " + SERIAL_NAME);

		// read config file
		boolean config = false;
		String path = System.getProperty("org.vanilladb.dd.config.file");
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

		// read service type properties
		String prop = System.getProperty(VanillaDdDb.class.getName()
				+ ".SERVICE_TYPE");
		serviceType = prop == null ? ServiceType.FULLY_REPLICATED : ServiceType
				.values()[Integer.parseInt(prop.trim())];

		// initialize all modules
		int bufferMgrType = 1;

		if (serviceType.equals(ServiceType.ODA))
			bufferMgrType = 2;

		VanillaDb.init(dirName, bufferMgrType);

		initCacheMgr();
		initPartitionMetaMgr();
		initMigration();
		initScheduler();
		initConnectionMgr(myNodeId);
		initDdLogMgr();
	}

	// ================
	// Initializers
	// ================

	public static void initCacheMgr() {
		switch (serviceType) {
		case FULLY_REPLICATED:
			cacheMgr = null; // TODO: Fill in this
			break;
		case CALVIN:
			cacheMgr = new CalvinCacheMgr();
			break;
		case ODA:
			cacheMgr = new OdaCacheMgr();
			break;
		case TPART:
			cacheMgr = new NewTPartCacheMgr();
			break;
		default:
			throw new UnsupportedOperationException();
		}
	}

	public static void initScheduler() {
		switch (serviceType) {
		case FULLY_REPLICATED:
			scheduler = null; // TODO: Fill in this
			break;
		case CALVIN:
			scheduler = initCalvinScheduler();
			break;
		case ODA:
			scheduler = initOdaScheduler();
			break;
		case TPART:
			scheduler = initTPartPartitioner();
			break;
		default:
			throw new UnsupportedOperationException();
		}
	}

	public static Scheduler initCalvinScheduler() {
		CalvinScheduler scheduler = new CalvinScheduler();
		taskMgr().runTask(scheduler);
		return scheduler;
	}

	public static Scheduler initOdaScheduler() {
		OdaScheduler scheduler = new OdaScheduler();
		taskMgr().runTask(scheduler);
		return scheduler;
	}

	// TODO: Fix it
	public static Scheduler initTPartPartitioner() {
		Scheduler s;
		//
		// if (hasReordering) {
		// TPartPartitioner tPartPartitioner = new TPartPartitioner(
		// new HeuristicNodeInserter(), new ReorderingSinker(
		// new CacheOptimizedSinker()), new TGraph());
		// s = tPartPartitioner;
		// taskMgr().runTask(tPartPartitioner);
		// } else if (hasReplication) {
		// ReplicatedTPartPartitioner rtp = new ReplicatedTPartPartitioner(
		// new ReplicatedNodeInserter(), new ReplicationSinker(),
		// new ReplicatedTGraph());
		// s = rtp;
		// taskMgr().runTask(rtp);
		// } else {
		// TPartPartitioner tp = new TPartPartitioner(
		// new HeuristicNodeInserter(), new CacheOptimizedSinker(),
		// new TGraph());
		// s = tp;
		// taskMgr().runTask(tp);
		// }
		//
		TPartPartitioner tp = new TPartPartitioner(new HeuristicNodeInserter(),
				new CacheOptimizedSinker(), new TGraph());
		s = tp;
		taskMgr().runTask(tp);

		tpartTaskScheduler = new TPartTaskScheduler();
		taskMgr().runTask(tpartTaskScheduler);
		return s;
	}

	public static void initPartitionMetaMgr() {
		String prop = System.getProperty(VanillaDdDb.class.getName()
				+ ".PARTITION_META_MGR");
		String parMgrCls = prop == null ? HashBasedPartitionMetaMgr.class
				.getName() : prop.trim();

		// TODO: organize it later
		// prop = System.getProperty(VanillaDdDb.class.getName()
		// + ".HAS_REORDERING");
		// hasReordering = prop == null ? false : Boolean
		// .parseBoolean(prop.trim());
		//
		// prop = System.getProperty(VanillaDdDb.class.getName()
		// + ".HAS_REPLICATION");
		// hasReplication = prop == null ? false : Boolean.parseBoolean(prop
		// .trim());
		//
		// prop = System.getProperty(VanillaDdDb.class.getName()
		// + ".REPLCATION_AND_PARTITION_META_MGR");
		// String rparMgrCls = prop == null ?
		// HashBasedPartitionMetaMgr.class.getName()
		// : prop.trim();

		try {
			parMetaMgr = (PartitionMetaMgr) Class.forName(parMgrCls)
					.newInstance();
			// if (hasReplication)
			// rparMgr = (ReplicationAndPartitionMetaMgr) Class.forName(
			// rparMgrCls).newInstance();
		} catch (Exception e) {
			if (logger.isLoggable(Level.WARNING))
				logger.warning("error reading the class name for partition manager");
			throw new RuntimeException();
		}
	}
	
	public static void initMigration(){
		String prop = System.getProperty(VanillaDdDb.class.getName()
				+ ".MIGRATION_MGR");
		String migrationCls = prop == null ? MigrationManager.class
				.getName() : prop.trim();

		try {
			migrateMgr = (MigrationManager) Class.forName(migrationCls)
					.newInstance();
		} catch (Exception e) {
			if (logger.isLoggable(Level.WARNING))
				logger.warning("error reading the class name for migration manager: " + migrationCls);
			throw new RuntimeException();
		}
	}

	public static void initConnectionMgr(int id) {
		connMgr = new ConnectionMgr(id);
	}
	
	public static void initDdLogMgr(){
		ddLogMgr = new DdLogMgr();
	}

	// ==================
	// Module Getters
	// ==================
	public static CacheMgr cacheMgr() {
		return cacheMgr;
	}

	public static Scheduler scheduler() {
		return scheduler;
	}

	public static PartitionMetaMgr partitionMetaMgr() {
		return parMetaMgr;
	}

	public static ConnectionMgr connectionMgr() {
		return connMgr;
	}

	public static TPartTaskScheduler tpartTaskScheduler() {
		return tpartTaskScheduler;
	}
	
	public static DdLogMgr DdLogMgr(){
		return ddLogMgr;
	}
	
	public static MigrationManager migrationMgr(){
		return migrateMgr;
	}

	// =================
	// Other Getters
	// =================

	public static int serverId() {
		return myNodeId;
	}

	public static ServiceType serviceType() {
		return serviceType;
	}
}
