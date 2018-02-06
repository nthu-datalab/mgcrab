package org.vanilladb.dd.server.migration;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.vanilladb.dd.remote.groupcomm.TupleSet;
import org.vanilladb.dd.sql.RecordKey;

public abstract class MigrationManager {

	// Sink ids for sequencers to identify the messages of migration
	public static final int SINK_ID_START_MIGRATION = -555;
	public static final int SINK_ID_ANALYSIS = -777;
	public static final int SINK_ID_ASYNC_PUSHING = -888;
	public static final int SINK_ID_STOP_MIGRATION = -999;
	public static final int SINK_ID_STOP_AND_COPY = -54321;

	private static long startTime = System.currentTimeMillis();

	private AtomicBoolean isMigrating = new AtomicBoolean(false);
	public AtomicBoolean isMigrated = new AtomicBoolean(false);

	// Migration target data set
	private Map<RecordKey, Boolean> migratedBeforeAnalysis = new ConcurrentHashMap<RecordKey, Boolean>(100000, 0.75f, 1000);
	private Map<RecordKey, Boolean> targetDataSet;
	private AtomicBoolean analysisStarted = new AtomicBoolean(false);
	private AtomicBoolean analysisCompleted = new AtomicBoolean(false);
	
	private TupleSet pushingData = null;
	private Object pushingLock = new Object();
	
	public TupleSet getPushingData() {
		TupleSet tmp = null;
		
		synchronized (pushingLock) {
			while (pushingData == null) {
				try {
					pushingLock.wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			tmp = pushingData;
			pushingData = null;
		}
		
		return tmp;
	}
	
	public void receivePushingData(TupleSet data) {
		synchronized (pushingLock) {
			pushingData = data;
			pushingLock.notifyAll();
		}
	}
	
	// Async pushing
	private static int PUSHING_COUNT = 15000;
	private Set<RecordKey> bgPushCandidates = new HashSet<RecordKey>();
	
	public boolean migrated = false;

	public abstract boolean keyIsInMigrationRange(RecordKey key);
	
	public abstract void onReceiveStartMigrationReq(Object[] metadata);

	public abstract void onReceiveAnalysisReq(Object[] metadata);

	public abstract void onReceiveAsyncMigrateReq(Object[] metadata);

	public abstract void onReceiveStopMigrateReq(Object[] metadata);
	
	public abstract void prepareAnalysis();
	
	public abstract Map<RecordKey, Boolean> generateDataSetForMigration();

	// XXX: Assume there are 3 server nodes
	// The better way to set up is to base on NUM_PARTITIONS
	// , but this version doesn't have properties loader.
	// It may cause NUM_PARTITIONS too early to be read.
	// That is, NUM_PARTITIONS is read before the properties loaded.
	
	public int getSourcePartition() {
		return 1;
//		return NUM_PARTITIONS - 2;
	}

	public int getDestPartition() {
		return 2;
//		return NUM_PARTITIONS - 1;
	}
	
	// Executed on the source node
	public void analysisComplete(Map<RecordKey, Boolean> newDataSet) {
		for (RecordKey rk : newDataSet.keySet())
			addBgPushKey(rk);

		System.out.println("End of analysis: " + (System.currentTimeMillis() - startTime) / 1000);
	}
	
	public void setRecordMigrated(RecordKey key) {
		throw new RuntimeException("Should not be called for Stop-and-Copy");
		
//		if (analysisCompleted.get()) {
//			Boolean status = targetDataSet.get(key);
//			// Ignore the data that are not in the target data set
//			if (status != null && status == Boolean.FALSE)
//				targetDataSet.put(key, Boolean.TRUE);
//		} else {
//			migratedBeforeAnalysis.put(key, Boolean.TRUE);
//		}
	}

	public boolean isRecordMigrated(RecordKey key) {
		throw new RuntimeException("Should not be called for Stop-and-Copy");
		
//		Boolean status;
//		if (analysisCompleted.get()) {
//			status = targetDataSet.get(key);
//			return (status == null) || (status != null && status == Boolean.TRUE);
//		} else {
//			status = migratedBeforeAnalysis.get(key);
//			if (status == null)
//				return false;
//			return true;
//		}
	}
	
	// NOTE: only for the normal transactions on the source node 
	public void addNewInsertKey(RecordKey key) {
		addBgPushKey(key);
	}
	
	public synchronized HashSet<RecordKey> getBgPushSet() {
		return new HashSet<RecordKey>(bgPushCandidates);
	}
	
	// Note that there may be duplicate keys
	private synchronized void addBgPushKey(RecordKey key) {
		bgPushCandidates.add(key);
	}
	
	public void startAnalysis() {
		analysisStarted.set(true);
		analysisCompleted.set(false);
	}

	public void startMigration() {
		analysisCompleted.set(true);
		isMigrating.set(true);
		System.out.println("Migration starts at " + (System.currentTimeMillis() - startTime) / 1000);
	}

	public void stopMigration() {
		isMigrating.set(false);
		isMigrated.set(true);
		if (migratedBeforeAnalysis != null)
			migratedBeforeAnalysis.clear();
		if (targetDataSet != null)
			targetDataSet.clear();
		System.out.println("Migration completes at " + (System.currentTimeMillis() - startTime) / 1000);
	}
	
	public boolean isAnalyzing() {
		return analysisStarted.get() && !analysisCompleted.get();
	}

	public boolean isMigrating() {
		return isMigrating.get();
	}

	public boolean isMigrated() {
		return isMigrated.get();
	}
	
	// Only works on the source node
	protected RecordKey[] getAsyncPushingParameters() {
		Set<RecordKey> seenSet = new HashSet<RecordKey>();
		Set<RecordKey> pushingSet = new HashSet<RecordKey>();
		
		for (RecordKey key : bgPushCandidates) {
			seenSet.add(key);
			if (!isRecordMigrated(key))
				pushingSet.add(key);
			
			if (pushingSet.size() >= PUSHING_COUNT || seenSet.size() >= bgPushCandidates.size())
				break;
		}
		
		// Remove seen records
		for (RecordKey key : seenSet)
			bgPushCandidates.remove(key);
		
		System.out.println("The rest size of candidates: " + bgPushCandidates.size());
		
		return pushingSet.toArray(new RecordKey[0]);
	}
}
