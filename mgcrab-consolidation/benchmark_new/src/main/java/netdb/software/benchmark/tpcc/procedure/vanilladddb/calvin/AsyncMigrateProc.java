package netdb.software.benchmark.tpcc.procedure.vanilladddb.calvin;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.dd.cache.CachedRecord;
import org.vanilladb.dd.cache.LocalRecordMgr;
import org.vanilladb.dd.cache.calvin.CalvinCacheMgr;
import org.vanilladb.dd.remote.groupcomm.TupleSet;
import org.vanilladb.dd.schedule.calvin.CalvinStoredProcedure;
import org.vanilladb.dd.server.VanillaDdDb;
import org.vanilladb.dd.server.migration.MigrationManager;
import org.vanilladb.dd.sql.RecordKey;
import org.vanilladb.dd.storage.tx.concurrency.ConservativeOrderedCcMgr;

import netdb.software.benchmark.tpcc.procedure.AsyncMigrateParamHelper;

public class AsyncMigrateProc extends CalvinStoredProcedure<AsyncMigrateParamHelper> {
	private static Logger logger = Logger.getLogger(AsyncMigrateProc.class.getName());

	private static Constant FALSE = new IntegerConstant(0);
	private static Constant TRUE = new IntegerConstant(1);

	private MigrationManager migraMgr = VanillaDdDb.migrationMgr();
	private CalvinCacheMgr cacheMgr = (CalvinCacheMgr) VanillaDdDb.cacheMgr();
	
	private RecordKey[] pushingKeys = null;
	private Set<RecordKey> storingKeys = new HashSet<RecordKey>();

	public AsyncMigrateProc(long txNum) {
		super(txNum, new AsyncMigrateParamHelper());
	}

	@Override
	public void prepareKeys() {
		// Only the source and the dest node need to pay attention
		if (VanillaDdDb.serverId() != VanillaDdDb.migrationMgr().getSourcePartition() &&
				VanillaDdDb.serverId() != VanillaDdDb.migrationMgr().getDestPartition())
			return;
		
		// For phase One: The source node reads a set of records, then pushes to the
		// dest node.
//		for (RecordKey key : paramHelper.getPushingKeys())
//			if (!migraMgr.isRecordMigrated(key))
//				pushingKeys.add(key);
		pushingKeys = paramHelper.getPushingKeys();
		
		// For phase Two: The dest node acquire the locks, then storing them to the 
		// local storage.
		for (RecordKey key : paramHelper.getStoringKeys())
			// Important: We only insert the un-migrated records in the dest
			if (!migraMgr.isRecordMigrated(key)) {
				storingKeys.add(key);
				addBuPushKey(key);
			}
		
		// Note that we will do this in pipeline. The phase two of a BG will be
		// performed with the phase one of the next BG in the same time. 
		
		// Force this stored procedure executes on the source and dest node
		forceParticipated = true;
		forceReadWriteTx = true;
	}

	@Override
	protected int decideMaster() {
		return VanillaDdDb.migrationMgr().getDestPartition();
	}

	@Override
	protected void onLocalReadCollected(Map<RecordKey, CachedRecord> localReadings) {
	}

	@Override
	protected void onRemoteReadCollected(Map<RecordKey, CachedRecord> remoteReadings) {
	}

	@Override
	protected void writeRecords(Map<RecordKey, CachedRecord> remoteReadings) {
	}

	@Override
	protected void masterCollectResults(Map<RecordKey, CachedRecord> readings) {
	}

	@Override
	protected void executeTransactionLogic() {
		if (logger.isLoggable(Level.INFO))
			logger.info("Asnyc pushing tx. " + txNum + " will pushes " +
					pushingKeys.length + " records and stores " + storingKeys.size() + " records.");
		
		// The source node
		if (VanillaDdDb.serverId() == VanillaDdDb.migrationMgr().getSourcePartition()) {
			// Quick fix: Release the locks immediately to prevent blocking the records in the source node
			ConservativeOrderedCcMgr ccMgr = (ConservativeOrderedCcMgr) tx.concurrencyMgr();
			ccMgr.releaseAll();
			
			readAndPushInSource();
		} else if (VanillaDdDb.serverId() == VanillaDdDb.migrationMgr().getDestPartition()) {
			insertInDest(migraMgr.pushingCacheInDest);
			migraMgr.pushingCacheInDest = receiveInDest();
		}

		if (logger.isLoggable(Level.INFO))
			logger.info("Asnyc pushing tx. " + txNum + " ends");
	}

	@Override
	protected void afterCommit() {
		if (VanillaDdDb.serverId() == VanillaDdDb.migrationMgr().getDestPartition()) {
			TupleSet ts = new TupleSet(MigrationManager.SINK_ID_ASYNC_PUSHING);
			VanillaDdDb.connectionMgr().pushTupleSet(VanillaDdDb.migrationMgr().getSourcePartition(), ts);
		}
	}
	
	private void readAndPushInSource() {
		// Construct pushing tuple set
		TupleSet ts = new TupleSet(-1);

		// Construct key sets
		Map<String, Set<RecordKey>> keysPerTables = new HashMap<String, Set<RecordKey>>();
		for (RecordKey key : pushingKeys) {
			Set<RecordKey> keys = keysPerTables.get(key.getTableName());
			if (keys == null) {
				keys = new HashSet<RecordKey>();
				keysPerTables.put(key.getTableName(), keys);
			}
			keys.add(key);
		}

		// Batch read the records per table
		for (Map.Entry<String, Set<RecordKey>> entry : keysPerTables.entrySet()) {
			Map<RecordKey, CachedRecord> recordMap = LocalRecordMgr.batchRead(entry.getValue(), tx);

			for (RecordKey key : entry.getValue()) {
				// System.out.println(key);
				CachedRecord rec = recordMap.get(key);

				// Prevent null pointer exceptions in the destination node
				if (rec == null) {
					rec = new CachedRecord();
					rec.setSrcTxNum(txNum);
					rec.setVal("exists", FALSE);
				} else
					rec.setVal("exists", TRUE);

				ts.addTuple(key, txNum, txNum, rec);
			}
		}

		if (logger.isLoggable(Level.INFO))
			logger.info("Asnyc pushing tx. " + txNum + " pushes " + ts.size()
					+ " records to the dest. node.");

		// Push to the destination
		VanillaDdDb.connectionMgr().pushTupleSet(VanillaDdDb.migrationMgr().getDestPartition(), ts);
		
	}
	
	private Map<RecordKey, CachedRecord> receiveInDest() {
		if (logger.isLoggable(Level.INFO))
			logger.info("Asnyc pushing tx. " + txNum + " is receiving " + pushingKeys.length
					+ " records from the source node.");
		
		Map<RecordKey, CachedRecord> recordMap = new HashMap<RecordKey, CachedRecord>();

		// Receive the data from the source node and save them
		for (RecordKey key : pushingKeys) {
			CachedRecord rec = cacheMgr.read(key, txNum, tx, false);
			recordMap.put(key, rec);
			cacheMgr.remove(key, txNum);
		}
		
		return recordMap;
	}
	
	private void insertInDest(Map<RecordKey, CachedRecord> cachedRecords) {
		if (logger.isLoggable(Level.INFO))
			logger.info("Asnyc pushing tx. " + txNum + " is storing " + storingKeys.size()
					+ " records to the local storage.");
		
		Set<RecordKey> existingKeys = new HashSet<RecordKey>();

		// Store the cached records
		for (RecordKey key : storingKeys) {
			CachedRecord rec = cachedRecords.get(key);
			
			if (rec == null)
				throw new RuntimeException("Something wrong: " + key);

			// Flush them to the local storage engine
			if (rec.getVal("exists").equals(TRUE)) {
				rec.getFldValMap().remove("exists");
				rec.getDirtyFldNames().remove("exists");

				cacheMgr.insert(key, rec.getFldValMap(), tx);
				existingKeys.add(key);
				cacheMgr.flushToLocalStorage(key, txNum, tx);
				cacheMgr.remove(key, txNum);
			}
		}
	}
}
