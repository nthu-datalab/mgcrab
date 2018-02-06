package netdb.software.benchmark.tpcc.procedure.vanilladddb.calvin;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.dd.cache.CachedRecord;
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
			addReadKey(key);
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
		
		// Construct pushing tuple set
		TupleSet ts = new TupleSet(-1);
		
		for (RecordKey key : paramHelper.getPushingKeys()) {
			CachedRecord rec = cacheMgr.read(key, txNum, tx, true);
			
			// Prevent null pointer exceptions in the destination node
			if (rec == null) {
				rec = new CachedRecord();
				rec.setSrcTxNum(txNum);
				rec.setVal("exists", FALSE);
			} else
				rec.setVal("exists", TRUE);
			
			ts.addTuple(key, txNum, txNum, rec);
			cacheMgr.remove(key, txNum);
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
		
		Set<RecordKey> existingKeys = new HashSet<RecordKey>();
		
		// Receive the data from the source node and save them
		for (RecordKey key : paramHelper.getPushingKeys()) {
			CachedRecord rec = cacheMgr.read(key, txNum, tx, false);
			pushedKeys.add(key);
			
			// Flush them to the local storage engine
			if (rec.getVal("exists").equals(TRUE)) {
				rec.getFldValMap().remove("exists");
				rec.getDirtyFldNames().remove("exists");
				savedKeys.add(key);
				
				cacheMgr.insert(key, rec.getFldValMap(), tx);
				existingKeys.add(key);
				cacheMgr.flushToLocalStorage(key, txNum, tx);
			}
			cacheMgr.remove(key, txNum);
		}
	}
}
