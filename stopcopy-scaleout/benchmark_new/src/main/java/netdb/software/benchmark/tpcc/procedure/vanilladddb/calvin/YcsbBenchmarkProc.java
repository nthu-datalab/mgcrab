package netdb.software.benchmark.tpcc.procedure.vanilladddb.calvin;

import java.util.HashMap;
import java.util.Map;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.dd.cache.CachedRecord;
import org.vanilladb.dd.schedule.calvin.CalvinStoredProcedure;
import org.vanilladb.dd.server.VanillaDdDb;
import org.vanilladb.dd.sql.RecordKey;

import netdb.software.benchmark.tpcc.procedure.YcsbBenchmarkProcParamHelper;
import netdb.software.benchmark.tpcc.vanilladddb.metadata.MicroBenchPartitionMetaMgr;

public class YcsbBenchmarkProc extends
		CalvinStoredProcedure<YcsbBenchmarkProcParamHelper> {
	
	private static int LATEST_ID_FOR_DEST = -1;
	
	private static void updateLatestId(int latestId) {
		LATEST_ID_FOR_DEST = latestId;
	}
	
	public static int getLatestInsertedID() {
		return LATEST_ID_FOR_DEST;
	}
	
	private RecordKey[] readKeys;
	private RecordKey[] writeKeys;
	private RecordKey[] insertKeys;

	public YcsbBenchmarkProc(long txNum) {
		super(txNum, new YcsbBenchmarkProcParamHelper());
	}

	@Override
	public void prepareKeys() {
		// set read keys
		readKeys = new RecordKey[paramHelper.getReadCount()];
		for (int i = 0; i < paramHelper.getReadCount(); i++) {
			// create RecordKey for reading
			Map<String, Constant> keyEntryMap = new HashMap<String, Constant>();
			keyEntryMap.put("ycsb_id", new VarcharConstant(paramHelper.getReadId(i)));
			RecordKey key = new RecordKey("ycsb", keyEntryMap);
			readKeys[i] = key;
			addReadKey(key);
		}

		// set write keys
		writeKeys = new RecordKey[paramHelper.getWriteCount()];
		for (int i = 0; i < paramHelper.getWriteCount(); i++) {
			// create record key for writing
			Map<String, Constant> keyEntryMap = new HashMap<String, Constant>();
			keyEntryMap.put("ycsb_id", new VarcharConstant(paramHelper.getWriteId(i)));
			RecordKey key = new RecordKey("ycsb", keyEntryMap);
			writeKeys[i] = key;
			addWriteKey(key);
		}
		
		// set insert keys
		insertKeys = new RecordKey[paramHelper.getInsertCount()];
		for (int i = 0; i < paramHelper.getInsertCount(); i++) {
			// create record key for inserting
			Map<String, Constant> keyEntryMap = new HashMap<String, Constant>();
			keyEntryMap.put("ycsb_id", new VarcharConstant(paramHelper.getInsertId(i)));
			RecordKey key = new RecordKey("ycsb", keyEntryMap);
			insertKeys[i] = key;
			addInsertKey(key);
		}
		
		// update latest id
		if (!VanillaDdDb.migrationMgr().isMigrating() && !VanillaDdDb.migrationMgr().isMigrated()) {
			if (paramHelper.getInsertCount() > 0) {
				int id = paramHelper.getLatestIdInParam();
				if (MicroBenchPartitionMetaMgr.getRangeIndex(id) == VanillaDdDb.migrationMgr().getDestPartition())
					updateLatestId(paramHelper.getLatestIdInParam());
			}
		}
	}
	
	@Override
	protected int decideMaster() {
		if (paramHelper.getInsertCount() > 0)
			return VanillaDdDb.partitionMetaMgr().getPartition(insertKeys[0]);
		return VanillaDdDb.partitionMetaMgr().getPartition(readKeys[0]);
	}

	@Override
	protected void onLocalReadCollected(
			Map<RecordKey, CachedRecord> localReadings) {
		// Do nothing
	}

	@Override
	protected void onRemoteReadCollected(
			Map<RecordKey, CachedRecord> remoteReadings) {
		// Do nothing
	}

	@Override
	protected void writeRecords(Map<RecordKey, CachedRecord> readings) {
		// UPDATE ycsb SET ycsb_1 = ... WHERE ycsb_id = ...
		for (int i = 0; i < paramHelper.getWriteCount(); i++) {
			CachedRecord rec = readings.get(writeKeys[i]);
			rec.setVal("ycsb_1", new VarcharConstant(paramHelper.getWriteValue(i)));
			update(writeKeys[i], rec);
		}
		
		// INSERT INTO ycsb (ycsb_id, ycsb_1, ...) VALUES ("...", "...", ...)
		for (int i = 0; i < paramHelper.getInsertCount(); i++) {
			insert(insertKeys[i], paramHelper.getInsertVals(i));
		}
	}

	@Override
	protected void masterCollectResults(Map<RecordKey, CachedRecord> readings) {
		// SELECT ycsb_id, ycsb_1 FROM ycsb WHERE ycsb_id = ...
		for (CachedRecord rec : readings.values()) {
			rec.getVal("ycsb_id").asJavaVal();
			rec.getVal("ycsb_1").asJavaVal();
		}
	}
}
