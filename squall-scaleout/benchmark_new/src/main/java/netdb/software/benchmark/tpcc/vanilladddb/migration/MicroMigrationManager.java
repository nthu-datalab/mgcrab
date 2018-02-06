package netdb.software.benchmark.tpcc.vanilladddb.migration;

import java.util.HashMap;
import java.util.Map;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.dd.remote.groupcomm.StoredProcedureCall;
import org.vanilladb.dd.server.VanillaDdDb;
import org.vanilladb.dd.server.migration.MigrationManager;
import org.vanilladb.dd.sql.RecordKey;

import netdb.software.benchmark.tpcc.TpccConstants;
import netdb.software.benchmark.tpcc.TransactionType;
import netdb.software.benchmark.tpcc.procedure.vanilladddb.calvin.YcsbBenchmarkProc;
import netdb.software.benchmark.tpcc.vanilladddb.metadata.MicroBenchPartitionMetaMgr;

public class MicroMigrationManager extends MigrationManager {
	
	public static final long RECORD_PERIOD = 3000;

	// Prototype variables
//	public static final double MIGRATE_PERCENTAGE = 0.5;
	
//	public static final int MIN_IID_IN_MIGRATION_RANGE = 
//			(int) ((NUM_PARTITIONS - 1) * ITEM_PER_PARTITION - (ITEM_PER_PARTITION * MIGRATE_PERCENTAGE) + 1);
//	public static final int MAX_IID_IN_MIGRATION_RANGE = 
//			(int) (MIN_IID_IN_MIGRATION_RANGE + (ITEM_PER_PARTITION * MIGRATE_PERCENTAGE) - 1);
//	public static final int MIN_ID_IN_MIGRATION_RANGE = 
//			(int) ((NUM_PARTITIONS - 1) * RECORD_PER_PARTITION - (RECORD_PER_PARTITION * MIGRATE_PERCENTAGE) + 1);
//	public static final int MAX_ID_IN_MIGRATION_RANGE = TpccConstants.YCSB_NUM_RECORDS * 10;
	
	private static final int COUNTS_FOR_SLEEP = 10000;
	private int parameterCounter = 0;
	
	public MicroMigrationManager() {
		super(RECORD_PERIOD);
	}
	
	@Override
	public boolean keyIsInMigrationRange(RecordKey key) {
		if (VanillaDdDb.partitionMetaMgr().isFullyReplicated(key))
			return false;
		
		// XXX: For YCSB and migration
//		int iid = (int) key.getKeyVal("i_id").asJavaVal();
//		return MIN_IID_IN_MIGRATION_RANGE <= iid && iid <= MAX_IID_IN_MIGRATION_RANGE;
		int partId = MicroBenchPartitionMetaMgr.getRangeIndex(key);
		return partId == this.getDestPartition();
	}
	
	/**
	 * This should only be executed on the sequencer node.
	 */
	@Override
	public void onReceiveStartMigrationReq(Object[] metadata) {
		// Send a store procedure call
		Object[] call = { new StoredProcedureCall(-1, -1, TransactionType.START_MIGRATION.ordinal(), (Object[]) null) };
		VanillaDdDb.connectionMgr().sendBroadcastRequest(call);
	}
	
	/**
	 * This should only be executed on the sequencer node.
	 */
	@Override
	public void onReceiveAnalysisReq(Object[] metadata) {
		// Send a store procedure call
		Object[] call = { new StoredProcedureCall(-1, -1, TransactionType.MIGRATION_ANALYSIS.ordinal(), (Object[]) null) };
		VanillaDdDb.connectionMgr().sendBroadcastRequest(call);
	}
	
	/**
	 * This should only be executed on the data source node.
	 */
	@Override
	public void onReceiveAsyncMigrateReq(Object[] metadata) {
		Object[] params = getAsyncPushingParameters();
		
		if (params != null && params.length > 0)
			System.out.println("Next start key: " + params[0]);
		
		// Send a store procedure call
		Object[] call;
		if (params.length > 0) {
			call = new Object[] { new StoredProcedureCall(-1, -1, TransactionType.ASYNC_MIGRATE.ordinal(), params) };
		} else
			call = new Object[] { new StoredProcedureCall(-1, -1, TransactionType.STOP_MIGRATION.ordinal(), (Object[]) null) };
		VanillaDdDb.connectionMgr().sendBroadcastRequest(call);
	}
	
	@Override
	// XXX: Not used for now
	public void onReceiveStopMigrateReq(Object[] metadata) {
		// Send a store procedure call
		Object[] call = { new StoredProcedureCall(-1, -1, TransactionType.STOP_MIGRATION.ordinal(), (Object[]) null) };
		VanillaDdDb.connectionMgr().sendBroadcastRequest(call);
	}
	
	@Override
	public void prepareAnalysis() {
		// Do nothing
	}
	
	@Override
	public Map<RecordKey, Boolean> generateDataSetForMigration() {
		// The map records the migration data set
		Map<RecordKey, Boolean> dataSet = new HashMap<RecordKey, Boolean>();
		Map<String, Constant> keyEntryMap;
		
		// Generate record keys
		// XXX: For YCSB and migration
		// For each item
//		for (int iid = MIN_IID_IN_MIGRATION_RANGE; iid <= MAX_IID_IN_MIGRATION_RANGE; iid++) {
//			keyEntryMap = new HashMap<String, Constant>();
//			keyEntryMap.put("i_id", new IntegerConstant(iid));
//			dataSet.put(new RecordKey("item", keyEntryMap), Boolean.FALSE);
//		}
		int startId = VanillaDdDb.migrationMgr().getDestPartition() * TpccConstants.YCSB_MAX_RECORD_PER_PART + 1;
		int endId = YcsbBenchmarkProc.getLatestInsertedID();
		
//		if (endId == -1)
//			endId = startId + TpccConstants.YCSB_RECORD_PER_PART / 2 - 1;
		
		System.out.println("Migrate from ycsb_id = " + startId + " to " + endId);
		
		for (int id = startId; id <= endId; id++) {
			keyEntryMap = new HashMap<String, Constant>();
			keyEntryMap.put("ycsb_id", new VarcharConstant(String.format(TpccConstants.YCSB_ID_FORMAT, id)));
			addOrSleep(dataSet, new RecordKey("ycsb", keyEntryMap));
		}
		
		return dataSet;
	}
	
	private void addOrSleep(Map<RecordKey, Boolean> map, RecordKey key) {
		parameterCounter++;
		
		if (parameterCounter % COUNTS_FOR_SLEEP == 0) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		map.put(key, Boolean.FALSE);
	}
	
	@Override
	public int recordSize(String tableName){
		switch(tableName){
		case "ycsb":
			return 990;
		default:
			throw new IllegalArgumentException("No such table for TPCC");
		}
	}
}
