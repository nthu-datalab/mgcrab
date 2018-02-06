package netdb.software.benchmark.tpcc.procedure.vanilladddb.calvin;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.storedprocedure.StoredProcedureParamHelper;
import org.vanilladb.dd.cache.CachedRecord;
import org.vanilladb.dd.cache.calvin.CalvinCacheMgr;
import org.vanilladb.dd.remote.groupcomm.Tuple;
import org.vanilladb.dd.remote.groupcomm.TupleSet;
import org.vanilladb.dd.schedule.calvin.AllExecuteProcedure;
import org.vanilladb.dd.schedule.calvin.CalvinScheduler;
import org.vanilladb.dd.server.VanillaDdDb;
import org.vanilladb.dd.server.migration.MigrationManager;
import org.vanilladb.dd.sql.RecordKey;

public class StartMigrationProc extends AllExecuteProcedure<StoredProcedureParamHelper> {
	
	private CalvinCacheMgr cacheMgr = (CalvinCacheMgr) VanillaDdDb.cacheMgr();
	
	private static final int CHUNK_SIZE = 15000;
	
	private static final Constant TYPE_MORE = new IntegerConstant(0);
	private static final Constant TYPE_ACK = new IntegerConstant(1);
	private static final Constant INT0 = new IntegerConstant(0);
	private static final Constant INT1 = new IntegerConstant(1);
	
	public StartMigrationProc(long txNum) {
		super(txNum, StoredProcedureParamHelper.DefaultParamHelper());
//		VanillaDdDb.migrationMgr().startMigration();
		
		// Lock scheduler (Stop-and-copy Migration)
		VanillaDdDb.migrationMgr().startMigration();
		CalvinScheduler.stopProcessing();
		
		// Prevent from being set to a read-only transaction
		forceReadWriteTx = true;
	}

	@Override
	protected void executeSql() {
		System.out.println("Migration starts with tx number: " + txNum);
		
		// Last server node starts async-pushing transactions
//		if(VanillaDdDb.serverId() == VanillaDdDb.migrationMgr().getDestPartition()){
//			TupleSet ts = new TupleSet(MigrationManager.SINK_ID_ANALYSIS);
//			VanillaDdDb.connectionMgr().pushTupleSet(ConnectionMgr.SEQ_NODE_ID, ts);
//		}
		
		// Scan and copy data
		if (VanillaDdDb.serverId() == VanillaDdDb.migrationMgr().getSourcePartition())
			executeOnSrcNode();
		else if (VanillaDdDb.serverId() == VanillaDdDb.migrationMgr().getDestPartition())
			executeOnDstNode();
		
		System.out.println("Migration stops.");
	}
	
	@Override
	public void afterCommit() {
		// Change migration status
		VanillaDdDb.migrationMgr().stopMigration();
		// Scheduler good to go
		CalvinScheduler.startProcessing();
	}
	
	private void executeOnSrcNode() {
		// Generate data set
		Set<RecordKey> dataSet = VanillaDdDb.migrationMgr().getBgPushSet();
		Set<RecordKey> tupleKeys = new HashSet<RecordKey>();
		
		System.out.println("Estimated size of target data set for migration: " + dataSet.size());
		
		// Push data one chunk each time
		while (dataSet.size() > 0) {
			// Send a notification to inform there is more records
			sendNotification(TYPE_MORE, INT1, VanillaDdDb.migrationMgr().getDestPartition());
		
			// Construct pushing tuple set
			TupleSet ts = new TupleSet(MigrationManager.SINK_ID_STOP_AND_COPY);
			for (RecordKey key : dataSet) {
				CachedRecord rec = cacheMgr.read(key, txNum, tx, true);
				if (rec != null) {
					ts.addTuple(key, txNum, txNum, rec);
					cacheMgr.remove(key, txNum);
				}
				
				// Break if the max size reached
				tupleKeys.add(key);
				if (tupleKeys.size() >= CHUNK_SIZE)
					break;
			}
			
			// Push to the destination
			VanillaDdDb.connectionMgr().pushTupleSet(VanillaDdDb.migrationMgr().getDestPartition(), ts);
			
			// Remove the pushed keys from the data set
			for (RecordKey key : tupleKeys)
				dataSet.remove(key);
			tupleKeys.clear();
			
			// Read the ack
			Constant ack = readNotification(TYPE_ACK);
			if (!ack.equals(INT1))
				throw new RuntimeException("something wrong with the ack");
			
			System.out.println("Data sent. Remaining size: " + dataSet.size());
		}
		
		// Send a notification to inform termination
		sendNotification(TYPE_MORE, INT0, VanillaDdDb.migrationMgr().getDestPartition());
		
		System.out.println("All data sent.");
	}
	
	private void executeOnDstNode() {
		
		// Check if there is more data
		Constant more = readNotification(TYPE_MORE);
		while (more.equals(INT1)) {
			// Wait for pushing data
			System.out.println("Wait for data");
			TupleSet ts = VanillaDdDb.migrationMgr().getPushingData();
			System.out.println("Got data, start processing...");
			
			for (Tuple t : ts.getTupleSet()) {
				cacheMgr.insert(t.key, t.rec.getFldValMap(), tx);
				cacheMgr.flushToLocalStorage(t.key, txNum, tx);
				cacheMgr.remove(t.key, txNum);
			}
			
			// Send an ack
			sendNotification(TYPE_ACK, INT1, VanillaDdDb.migrationMgr().getSourcePartition());
			
			// Check if there is more data
			more = readNotification(TYPE_MORE);
		}
		
		System.out.println("All data saved.");
	}
	
	private void sendNotification(Constant type, Constant value, int nodeId) {
		RecordKey notKey = getNotificationKey(type);
		CachedRecord notVal = getNotificationValue(type, value);

		TupleSet ts = new TupleSet(-1);
		// Use node id as source tx number
		ts.addTuple(notKey, txNum, txNum, notVal);
		VanillaDdDb.connectionMgr().pushTupleSet(nodeId, ts);
	}
	
	private Constant readNotification(Constant type) {
		RecordKey notKey = getNotificationKey(type);
		
		CachedRecord rec = cacheMgr.read(notKey, txNum, tx, false);
		Constant value = rec.getVal("value");
		
		// Remove the record from the cache
		cacheMgr.remove(notKey, txNum);
		
		return value;
	}
	
	private RecordKey getNotificationKey(Constant type) {
		Map<String, Constant> keyEntryMap = new HashMap<String, Constant>();
		keyEntryMap.put("type", type);
		return new RecordKey("notification", keyEntryMap);
	}

	private CachedRecord getNotificationValue(Constant type, Constant value) {
		// Create key value sets
		Map<String, Constant> fldVals = new HashMap<String, Constant>();
		fldVals.put("type", type);
		fldVals.put("value", value);

		// Create a record
		CachedRecord rec = new CachedRecord(fldVals);
		rec.setSrcTxNum(txNum);
		return rec;
	}
}
