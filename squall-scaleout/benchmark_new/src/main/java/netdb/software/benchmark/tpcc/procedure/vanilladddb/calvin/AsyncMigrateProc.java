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

import netdb.software.benchmark.tpcc.procedure.AsyncMigrateParamHelper;

public class AsyncMigrateProc extends CalvinStoredProcedure<AsyncMigrateParamHelper> {
	private static Logger logger = Logger.getLogger(AsyncMigrateProc.class.getName());
	
	private static Constant FALSE = new IntegerConstant(0);
	private static Constant TRUE = new IntegerConstant(1);
	
	private CalvinCacheMgr cacheMgr = (CalvinCacheMgr) VanillaDdDb.cacheMgr();
	
	public AsyncMigrateProc(long txNum) {
		super(txNum, new AsyncMigrateParamHelper());
	}

	@Override
	public void prepareKeys() {
		// Lock the pushing records
		for (RecordKey key : paramHelper.getPushingKeys())
			addWriteKey(key);
		isExecutingInSrc = false;
		forceReadWriteTx = true;
//		isBgPush = true;
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
		if (VanillaDdDb.serverId() == VanillaDdDb.migrationMgr().getSourcePartition())
			executeSourceLogic();
		else if (VanillaDdDb.serverId() == VanillaDdDb.migrationMgr().getDestPartition())
			executeDestLogic();
		
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
	
	private void executeSourceLogic() {
		if (logger.isLoggable(Level.INFO))
			logger.info("Asnyc pushing tx. " + txNum + " starts in the source node");
		
		// For Squall: Pulling-based migration
		// Wait for a pull request
		waitForPullRequest();
		
		// Construct pushing tuple set
		TupleSet ts = new TupleSet(-1);
		
		// Construct key sets
		Map<String, Set<RecordKey>> keysPerTables = new HashMap<String, Set<RecordKey>>();
		for (RecordKey key : paramHelper.getPushingKeys()) {
			Set<RecordKey> keys = keysPerTables.get(key.getTableName());
			if (keys == null) {
				keys = new HashSet<RecordKey>();
				keysPerTables.put(key.getTableName(), keys);
			}
			keys.add(key);
		}

		// Batch read the records per table
		for (Set<RecordKey> keys : keysPerTables.values()) {
			Map<RecordKey, CachedRecord> recordMap = LocalRecordMgr.batchRead(keys, tx);

			for (RecordKey key : keys) {
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
					+ " records to the dest. node.\nFirst record: "
					+ paramHelper.getPushingKeys()[0]);
		
		// Push to the destination
		VanillaDdDb.connectionMgr().pushTupleSet(VanillaDdDb.migrationMgr().getDestPartition(), ts);
	}
	
	private void executeDestLogic() {
		if (logger.isLoggable(Level.INFO))
			logger.info("Asnyc pushing tx. " + txNum + " starts in the destination node");
		
		// For Squall: Pulling-based migration
		// Send a pull request
		sendAPullRequest(VanillaDdDb.migrationMgr().getSourcePartition());
		
		// Receive the data from the source node and save them
		for (RecordKey key : paramHelper.getPushingKeys()) {
			CachedRecord rec = cacheMgr.read(key, txNum, tx, false);
			
			// Flush them to the local storage engine
			if (rec.getVal("exists").equals(TRUE)) {
				rec.getFldValMap().remove("exists");
				rec.getDirtyFldNames().remove("exists");
				
				cacheMgr.insert(key, rec.getFldValMap(), tx);
				cacheMgr.flushToLocalStorage(key, txNum, tx);
			}
			cacheMgr.remove(key, txNum);
		}
	}
}
