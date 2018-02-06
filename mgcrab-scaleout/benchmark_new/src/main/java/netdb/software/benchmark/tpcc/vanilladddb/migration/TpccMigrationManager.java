package netdb.software.benchmark.tpcc.vanilladddb.migration;

import java.util.HashMap;
import java.util.Map;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.dd.remote.groupcomm.StoredProcedureCall;
import org.vanilladb.dd.server.VanillaDdDb;
import org.vanilladb.dd.server.migration.MigrationManager;
import org.vanilladb.dd.sql.RecordKey;

import netdb.software.benchmark.tpcc.TpccConstants;
import netdb.software.benchmark.tpcc.TransactionType;
import netdb.software.benchmark.tpcc.procedure.vanilladddb.calvin.NewOrderProc;
import netdb.software.benchmark.tpcc.procedure.vanilladddb.calvin.PaymentProc;
import netdb.software.benchmark.tpcc.vanilladddb.metadata.TpccPartitionMetaMgr;

public class TpccMigrationManager extends MigrationManager {
	
	public static final long RECORD_PERIOD = 5000;

	// Prototype variables
//	public static double MIGRATE_PERCENTAGE = 0.5;
	
//	public static int MIN_WID_IN_MIGRATION_RANGE =
//			(int) ((NUM_PARTITIONS - 1) * WAREHOUSE_PER_PART - (WAREHOUSE_PER_PART * MIGRATE_PERCENTAGE) + 1);
//	public static int MAX_WID_IN_MIGRATION_RANGE =
//			(int) (MIN_WID_IN_MIGRATION_RANGE + (WAREHOUSE_PER_PART * MIGRATE_PERCENTAGE) - 1);
	public static int MIN_WID_IN_MIGRATION_RANGE = 21;
	public static int MAX_WID_IN_MIGRATION_RANGE = 21;
	
	private static final int COUNTS_FOR_SLEEP = 10000;
	private int parameterCounter = 0;
	
	public TpccMigrationManager() {
		super(0, 450000, RECORD_PERIOD);
	}

	@Override
	public boolean keyIsInMigrationRange(RecordKey key) {
		if (VanillaDdDb.partitionMetaMgr().isFullyReplicated(key))
			return false;
		
		int wid = TpccPartitionMetaMgr.getWarehouseId(key);
		return isWarehouseIdInMigrationRange(wid);
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
		
		// Send a store procedure call
		Object[] call;
		// The parameters should at least contain 2 integers (phase1_count, phase2_count) 
		if (params.length > 2) {
			call = new Object[] { new StoredProcedureCall(-1, -1, TransactionType.ASYNC_MIGRATE.ordinal(), params) };
		} else
			call = new Object[] { new StoredProcedureCall(-1, -1, TransactionType.STOP_MIGRATION.ordinal(), (Object[]) null) };
		VanillaDdDb.connectionMgr().sendBroadcastRequest(call);
	}
	
	@Override
	public void onReceiveStartCatchUpReq(Object[] metadata) {
		// Send a store procedure call
		Object[] call = { new StoredProcedureCall(-1, -1, TransactionType.START_CATCH_UP.ordinal(), (Object[]) null) };
		VanillaDdDb.connectionMgr().sendBroadcastRequest(call);
	}

	@Override
	// XXX: Not used for now
	public void onReceiveStopMigrateReq(Object[] metadata) {
		// Send a store procedure call
		Object[] call = { new StoredProcedureCall(-1, -1, TransactionType.STOP_MIGRATION.ordinal(), (Object[]) null) };
		VanillaDdDb.connectionMgr().sendBroadcastRequest(call);
	}
	
	// Analysis parameters
	private int[][] maxOrderIds;
	private int[][][] maxHistoryIds;
	
	@Override
	public void prepareAnalysis() {
		// Fetch parameters
		int warehouseCount = MAX_WID_IN_MIGRATION_RANGE - MIN_WID_IN_MIGRATION_RANGE + 1;
		maxOrderIds = new int[warehouseCount][10];
		maxHistoryIds = new int[warehouseCount][10][3000];
		for (int wi = 0; wi < warehouseCount; wi++)
			for (int di = 0; di < 10; di++) {
				maxOrderIds[wi][di] = NewOrderProc.getNextOrderId(wi + MIN_WID_IN_MIGRATION_RANGE, di + 1) - 1;
				for (int ci = 0; ci < 3000; ci++)
					maxHistoryIds[wi][di][ci] = PaymentProc.getNextHistoryId(wi + MIN_WID_IN_MIGRATION_RANGE, di + 1, ci + 1) - 1;
			}
	}
	
	@Override
	public Map<RecordKey, Boolean> generateDataSetForMigration() {
		// The map records the migration data set
		Map<RecordKey, Boolean> dataSet = new HashMap<RecordKey, Boolean>();
		Map<String, Constant> keyEntryMap;
		
		// Generate record keys
		// For each warehouse
		for (int wid = MIN_WID_IN_MIGRATION_RANGE; wid <= MAX_WID_IN_MIGRATION_RANGE; wid++) {
			int wIndex = wid - MIN_WID_IN_MIGRATION_RANGE;
			
			keyEntryMap = new HashMap<String, Constant>();
			keyEntryMap.put("w_id", new IntegerConstant(wid));
			addOrSleep(dataSet, new RecordKey("warehouse", keyEntryMap));
			
			// For each district
			for (int did = 1; did <= 10; did++) {
				int dIndex = did - 1;
				
				keyEntryMap = new HashMap<String, Constant>();
				keyEntryMap.put("d_w_id", new IntegerConstant(wid));
				keyEntryMap.put("d_id", new IntegerConstant(did));
				addOrSleep(dataSet, new RecordKey("district", keyEntryMap));
				
				// For each customer
				for (int cid = 1; cid <= 3000; cid++) {
					int cIndex = cid - 1;
					
					keyEntryMap = new HashMap<String, Constant>();
					keyEntryMap.put("c_w_id", new IntegerConstant(wid));
					keyEntryMap.put("c_d_id", new IntegerConstant(did));
					keyEntryMap.put("c_id", new IntegerConstant(cid));
					addOrSleep(dataSet, new RecordKey("customer", keyEntryMap));
					
					// History
					for (int hid = 1; hid <= maxHistoryIds[wIndex][dIndex][cIndex]; hid++) {
						keyEntryMap = new HashMap<String, Constant>();
						keyEntryMap.put("h_id", new IntegerConstant(hid));
						keyEntryMap.put("h_c_w_id", new IntegerConstant(wid));
						keyEntryMap.put("h_c_d_id", new IntegerConstant(did));
						keyEntryMap.put("h_c_id", new IntegerConstant(cid));
						addOrSleep(dataSet, new RecordKey("history", keyEntryMap));
					}
				}
				
				// New Order
				for (int noid = TpccConstants.NEW_ORDER_START_ID; noid <= maxOrderIds[wIndex][dIndex]; noid++) {
					keyEntryMap = new HashMap<String, Constant>();
					keyEntryMap.put("no_w_id", new IntegerConstant(wid));
					keyEntryMap.put("no_d_id", new IntegerConstant(did));
					keyEntryMap.put("no_o_id", new IntegerConstant(noid));
					addOrSleep(dataSet, new RecordKey("new_order", keyEntryMap));
				}
				
				// Order
				for (int oid = 1; oid <= maxOrderIds[wIndex][dIndex]; oid++) {
					keyEntryMap = new HashMap<String, Constant>();
					keyEntryMap.put("o_w_id", new IntegerConstant(wid));
					keyEntryMap.put("o_d_id", new IntegerConstant(did));
					keyEntryMap.put("o_id", new IntegerConstant(oid));
					addOrSleep(dataSet, new RecordKey("orders", keyEntryMap));
					
					// Order Line
					for (int olNum = 1; olNum <= 15; olNum++) {
						keyEntryMap = new HashMap<String, Constant>();
						keyEntryMap.put("ol_w_id", new IntegerConstant(wid));
						keyEntryMap.put("ol_d_id", new IntegerConstant(did));
						keyEntryMap.put("ol_o_id", new IntegerConstant(oid));
						keyEntryMap.put("ol_number", new IntegerConstant(olNum));
						addOrSleep(dataSet, new RecordKey("order_line", keyEntryMap));
					}
				}
			}
			
			// Stock
			for (int iid = 1; iid <= 100000; iid++) {
				keyEntryMap = new HashMap<String, Constant>();
				keyEntryMap.put("s_i_id", new IntegerConstant(iid));
				keyEntryMap.put("s_w_id", new IntegerConstant(wid));
				addOrSleep(dataSet, new RecordKey("stock", keyEntryMap));
			}
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
		case "warehouse":
			return 344;
		case "district":
			return 352;
		case "customer":
			return 2552;
		case "history":
			return 132;
		case "new_order":
			return 12;
		case "orders":
			return 36;
		case "order_line":
			return 140;
		case "item":
			return 320;
		case "stock":
			return 1184;
		default:
			throw new IllegalArgumentException("No such table for TPCC");
		}
	}
	
	private boolean isWarehouseIdInMigrationRange(int wid) {
		if (wid >= MIN_WID_IN_MIGRATION_RANGE && wid <= MAX_WID_IN_MIGRATION_RANGE)
			return true;
		return false;
	}
}
