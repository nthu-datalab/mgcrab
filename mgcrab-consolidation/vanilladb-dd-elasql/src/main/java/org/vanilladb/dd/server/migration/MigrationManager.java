package org.vanilladb.dd.server.migration;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.server.task.Task;
import org.vanilladb.dd.cache.CachedRecord;
import org.vanilladb.dd.remote.groupcomm.TupleSet;
import org.vanilladb.dd.server.VanillaDdDb;
import org.vanilladb.dd.server.task.calvin.CalvinStoredProcedureTask;
import org.vanilladb.dd.sql.RecordKey;

public abstract class MigrationManager {
	private static Logger logger = Logger.getLogger(MigrationManager.class.getName());
	
	public enum Mode { NOT_MIGRATED, ANALYSIS, CAUGHT_UP, CRABBING, CATCHING_UP, MIGRATED };

	// Sink ids for sequencers to identify the messages of migration
	public static final int SINK_ID_START_MIGRATION = -555;
	public static final int SINK_ID_ANALYSIS = -777;
	public static final int SINK_ID_ASYNC_PUSHING = -888;
	public static final int SINK_ID_START_CATCH_UP = -689;
	public static final int SINK_ID_START_CRABBING = -870;
	public static final int SINK_ID_STOP_MIGRATION = -999;

	private static long startTime = System.currentTimeMillis();
	
	private AtomicReference<Mode> currentMode = new AtomicReference<Mode>(Mode.NOT_MIGRATED);

	// For recording the migrated status in the other node and the destination node
	private Set<RecordKey> migratedKeys = new HashSet<RecordKey>(1000000);
	// These two sets are created for the source node identifying the migrated records
	// and the parameters of background pushes.
	private Map<RecordKey, Boolean> newInsertedData = new HashMap<RecordKey, Boolean>(1000000);
	private Map<RecordKey, Boolean> analyzedData;
	
//	private AtomicBoolean analysisStarted = new AtomicBoolean(false);
//	private AtomicBoolean analysisCompleted = new AtomicBoolean(false);

	// To decide the timing for background-pushing
	// Note that these variables should be only run by the scheduler thread
	private static final long TX_PER_RANGE = 10000;
	private static final double CATCH_UP_THRESHOLD = -1;
	private boolean caughtUpReqSent = false;
	private boolean crabbingReqSent = false;
	private boolean backPushStarted = false;
	private int totalMigratedSize, txCount, prevTotalMigratedSize;
	
	public boolean migrated = false;
	
	// Async pushing
	private static final int PUSHING_COUNT = 15000;
	private static final int PUSHING_BYTE_COUNT = 4000000;
//	private static final int PUSHING_BYTE_COUNT = 1000000;
	private ConcurrentLinkedQueue<RecordKey> skipRequestQueue = new ConcurrentLinkedQueue<RecordKey>();
	private Map<String, Set<RecordKey>> bgPushCandidates;

	private Set<Object> lastPushedKeys = new HashSet<Object>();
	
	public Map<RecordKey, CachedRecord> pushingCacheInDest;
	
	private boolean roundrobin = true;
	private boolean useCount = true;
	
	private boolean isSourceNode;
	
	// The time starts from the time which the first transaction arrives at
	private long bgStartTime, crabbingStartTime, caughtUpStartTime, printStatusPeriod;

	public MigrationManager(long bgStartTime, long crabbingStartTime, long catchUpStartTime, long printStatusPeriod) {
		this.bgStartTime = bgStartTime;
		this.crabbingStartTime = crabbingStartTime;
		this.caughtUpStartTime = catchUpStartTime;
		this.printStatusPeriod = printStatusPeriod;
		this.prevTotalMigratedSize = Integer.MAX_VALUE;
		this.bgPushCandidates = new HashMap<String, Set<RecordKey>>();
		this.isSourceNode = (VanillaDdDb.serverId() == getSourcePartition());
	}

	public abstract boolean keyIsInMigrationRange(RecordKey key);
	
	public abstract void onReceiveStartMigrationReq(Object[] metadata);

	public abstract void onReceiveAnalysisReq(Object[] metadata);

	public abstract void onReceiveAsyncMigrateReq(Object[] metadata);

	public abstract void onReceiveStartCrabbingReq(Object[] metadata);
	
	public abstract void onReceiveStartCatchUpReq(Object[] metadata);

	public abstract void onReceiveStopMigrateReq(Object[] metadata);

	public abstract void prepareAnalysis();

	public abstract int recordSize(String tableName);

	public abstract Map<RecordKey, Boolean> generateDataSetForMigration();

	// XXX: Assume there are 3 server nodes
	// The better way to set up is to base on NUM_PARTITIONS
	// , but this version doesn't have properties loader.
	// It may cause NUM_PARTITIONS too early to be read.
	// That is, NUM_PARTITIONS is read before the properties loaded.

	public int getSourcePartition() {
		return 1;
		// return NUM_PARTITIONS - 2;
	}

	public int getDestPartition() {
		return 2;
		// return NUM_PARTITIONS - 1;
	}

	/**
	 * 
	 * @param newDataSet A thread-safe hash map with the candidate keys maps to FALSE
	 */
	public void analysisComplete(Map<RecordKey, Boolean> analyzedKeys) {
		// Only the source node can call this method
		if (!isSourceNode)
			throw new RuntimeException("Something wrong");
		
		// Set all keys in the data set as pushing candidates
		// asyncPushingCandidates = new HashSet<RecordKey>(newDataSet.keySet());

		for (RecordKey rk : analyzedKeys.keySet())
			addBgPushKey(rk);

		// Save the set
		analyzedData = analyzedKeys;

		System.out.println("End of analysis: " + (System.currentTimeMillis() - startTime) / 1000);
	}
	
	// NOTE: only for the normal transactions on the source node 
	public void addNewInsertKey(RecordKey key) {
		if (!isSourceNode || !isAnalyzing())
			throw new RuntimeException("Something wrong");
		
		newInsertedData.put(key, Boolean.FALSE);
		addBgPushKey(key);
	}
	
	// Note that there may be duplicate keys
	private synchronized void addBgPushKey(RecordKey key) {
		Set<RecordKey> set = bgPushCandidates.get(key.getTableName());
		if (set == null)
			set = new HashSet<RecordKey>();
		bgPushCandidates.put(key.getTableName(), set);
		set.add(key);
	}

	public void setRecordMigrated(Collection<RecordKey> keys) {
		if (isSourceNode) {
			for (RecordKey key : keys) {
				Boolean status = newInsertedData.get(key);
				if (status != null && status == Boolean.FALSE) {
					skipRequestQueue.add(key);
					newInsertedData.put(key, Boolean.TRUE);
				} else {
					status = analyzedData.get(key);
					if (status != null && status == Boolean.FALSE) {
						skipRequestQueue.add(key);
						analyzedData.put(key, Boolean.TRUE);
					}
				}
			}
			// recordMigratedSize(keys.size());
//			waitForCrabbing();
		} else {
			migratedKeys.addAll(keys);
		}
	}

	public boolean isRecordMigrated(RecordKey key) {
		if (isSourceNode) {
			Boolean status = newInsertedData.get(key);
			if (status != null)
				return status;

			status = analyzedData.get(key);
			if (status != null)
				return status;

			// If there is no candidate in the map, it means that the record
			// must not be inserted before the migration starts. Therefore,
			// the record must have been foreground pushed.
			return true;
		} else
			return migratedKeys.contains(key);
	}
	
	public Mode getCurrentMode() {
		return currentMode.get();
	}
	
	public void startAnalysis() {
		currentMode.set(Mode.ANALYSIS);
	}

	public void startMigration() {
		// XXX: For consolidation
//		currentMode.set(Mode.CATCHING_UP);
		currentMode.set(Mode.CRABBING);
		crabbingReqSent = true;
		
		if (logger.isLoggable(Level.INFO)) {
			logger.info("Migration starts at " + (System.currentTimeMillis() - startTime) / 1000);
//			logger.info("Use Catching Up phase.");
		}
		
		startBackgroundPush();
	}

	public void startCaughtUpPhase() {
		currentMode.set(Mode.CAUGHT_UP);
		if (logger.isLoggable(Level.INFO))
			logger.info("Caught up phase starts at " + (System.currentTimeMillis() - startTime) / 1000);
	}
	
	public void startCatchingUpPhase() {
		currentMode.set(Mode.CATCHING_UP);
		if (logger.isLoggable(Level.INFO))
			logger.info("Catching up phase starts at " + (System.currentTimeMillis() - startTime) / 1000);
	}
	
	public void startCrabbingPhase() {
		currentMode.set(Mode.CRABBING);
		if (logger.isLoggable(Level.INFO))
			logger.info("Crabbing phase starts at " + (System.currentTimeMillis() - startTime) / 1000);
	}

	public void stopMigration() {
		currentMode.set(Mode.MIGRATED);
		if (migratedKeys != null)
			migratedKeys.clear();
		if (newInsertedData != null)
			newInsertedData.clear();
		if (analyzedData != null)
			analyzedData.clear();
		if (logger.isLoggable(Level.INFO))
			logger.info("Migration completes at " + (System.currentTimeMillis() - startTime) / 1000);
	}
	
	public boolean isAnalyzing() {
		return currentMode.get() == Mode.ANALYSIS;
	}

	public boolean isMigrating() {
		Mode mode = currentMode.get();
		return mode != Mode.MIGRATED && mode != Mode.ANALYSIS && mode != Mode.NOT_MIGRATED;
	}
	
	public boolean isCaughtUpPhase() {
		return currentMode.get() == Mode.CAUGHT_UP;
	}

	public boolean isCrabbingPhase() {
		return currentMode.get() == Mode.CRABBING;
	}
	
	public boolean isCatchingUpPhase() {
		return currentMode.get() == Mode.CATCHING_UP;
	}

	public boolean isMigrated() {
		return currentMode.get() == Mode.MIGRATED;
	}

	// Note: This only works on the source node
	protected synchronized Object[] getAsyncPushingParameters() {
		// Generate the pushing keys
		Set<Object> pushKeys = getBgPushKeys();

		// Generate the parameters using the keys
		// Parameters: [(# of keys for pushing), {pushing keys},
		// (# of keys for storing), {storing keys}]
		int paramLen = 2 + pushKeys.size() + lastPushedKeys.size();
		Object[] params = new Object[paramLen];

		int idx = 0;
		params[idx++] = pushKeys.size();
		for (Object key : pushKeys)
			params[idx++] = key;
		params[idx++] = lastPushedKeys.size();
		for (Object key : lastPushedKeys)
			params[idx++] = key;

		// Save the keys for the next time of generating
		lastPushedKeys = pushKeys;

		return params;
	}

	private Set<Object> getBgPushKeys() {
		// Set<RecordKey> seenSet = new HashSet<RecordKey>();
		Set<Object> pushingKeys = new HashSet<Object>();
		Set<String> emptyTables = new HashSet<String>();
		int pushing_bytes = 0;

		// YS version
		// for (RecordKey key : asyncPushingCandidates) {
		// seenSet.add(key);
		// if (!isRecordMigrated(key))
		// pushingSet.add(key);
		//
		// if (pushingSet.size() >= PUSHING_COUNT || seenSet.size() >=
		// asyncPushingCandidates.size())
		// break;
		// }
		//
		// // Remove seen records
		// asyncPushingCandidates.removeAll(seenSet);
		//
		// if (logger.isLoggable(Level.INFO))
		// logger.info("The rest size of candidates: " +
		// asyncPushingCandidates.size());

		// Remove the records that have been sent during fore-ground pushing
		RecordKey sentKey = null;
		while ((sentKey = skipRequestQueue.poll()) != null) {
			Set<RecordKey> keys = bgPushCandidates.get(sentKey.getTableName());
			if (keys != null)
				keys.remove(sentKey);
		}

		if (roundrobin) {
			if (useCount) {
				// RR & count
				while (pushingKeys.size() < PUSHING_COUNT && !bgPushCandidates.isEmpty()) {
					for (String tableName : bgPushCandidates.keySet()) {
						Set<RecordKey> set = bgPushCandidates.get(tableName);

						if (set.isEmpty()) {
							emptyTables.add(tableName);
							continue;
						}
						RecordKey key = set.iterator().next();
						pushingKeys.add(key);
						set.remove(key);
					}
					for (String table : emptyTables) {
						bgPushCandidates.remove(table);
					}
				}
			} else {
				// RR & byte
				while (pushing_bytes < PUSHING_BYTE_COUNT && !bgPushCandidates.isEmpty()) {
					for (String tableName : bgPushCandidates.keySet()) {
						Set<RecordKey> set = bgPushCandidates.get(tableName);

						if (set.isEmpty()) {
							emptyTables.add(tableName);
							continue;
						}
						RecordKey key = set.iterator().next();
						pushingKeys.add(key);
						set.remove(key);
						pushing_bytes += recordSize(tableName);
					}
					for (String table : emptyTables) {
						bgPushCandidates.remove(table);
					}
				}
			}
		} else {
			List<String> candidateTables = new LinkedList<String>(bgPushCandidates.keySet());
			// sort by table size , small first
			Collections.sort(candidateTables, new Comparator<String>() {
				@Override
				public int compare(String o1, String o2) {
					int s1 = bgPushCandidates.get(o1).size();
					int s2 = bgPushCandidates.get(o2).size();
					if (s1 > s2)
						return -1;
					if (s1 < s2)
						return 1;
					return 0;
				}
			});
			System.out.println("PUSHING_BYTE_COUNT" + PUSHING_BYTE_COUNT);
			if (useCount) {
				while (!bgPushCandidates.isEmpty() && pushingKeys.size() < PUSHING_COUNT) {
					for (String tableName : candidateTables) {
						Set<RecordKey> set = bgPushCandidates.get(tableName);

						while (!set.isEmpty() && pushingKeys.size() < PUSHING_COUNT) {
							RecordKey key = set.iterator().next();
							pushingKeys.add(key);
							set.remove(key);
						}

						if (set.isEmpty()) {
							emptyTables.add(tableName);
						} else {
							break;
						}
					}
					for (String table : emptyTables) {
						bgPushCandidates.remove(table);
						candidateTables.remove(table);
					}
				}

			} else {
				while (!bgPushCandidates.isEmpty() && pushing_bytes < PUSHING_BYTE_COUNT) {
					for (String tableName : candidateTables) {
						Set<RecordKey> set = bgPushCandidates.get(tableName);

						while (!set.isEmpty() && pushing_bytes < PUSHING_BYTE_COUNT) {
							RecordKey key = set.iterator().next();
							pushingKeys.add(key);
							set.remove(key);
							pushing_bytes += recordSize(tableName);
						}

						System.out.println("listEmpty: " + set.isEmpty());
						System.out.println("pushing_bytes: " + pushing_bytes);

						if (set.isEmpty()) {
							emptyTables.add(tableName);
							// if (!pushingSet.isEmpty()) {
							// VanillaDdDb.taskMgr().runTask(new Task() {
							//
							// @Override
							// public void run() {
							//
							// VanillaDdDb.initAndStartProfiler();
							// try {
							// Thread.sleep(10000);
							// } catch (InterruptedException e) {
							// // TODO Auto-generated catch block
							// e.printStackTrace();
							// }
							// VanillaDdDb.stopProfilerAndReport();
							// }
							//
							// });
							// }
							break;
						} else {
							break;
						}
					}
					for (String table : emptyTables) {
						bgPushCandidates.remove(table);
						candidateTables.remove(table);
					}
					System.out.println("pushingSet: " + pushingKeys.size());
					if (!pushingKeys.isEmpty()) {
						break;
					}
				}
			}

		}

		/*
		 * StringBuilder sb = new StringBuilder(); for (String tableName :
		 * roundRobinAsyncPushingCandidates.keySet()) { sb.append(tableName);
		 * sb.append(": ");
		 * sb.append(roundRobinAsyncPushingCandidates.get(tableName).size());
		 * sb.append("\n"); } for (List<RecordKey> table :
		 * roundRobinAsyncPushingCandidates.values()) { candidatesLeft +=
		 * table.size(); }
		 * 
		 * if (logger.isLoggable(Level.INFO)) logger.info(
		 * "The rest size of candidates: " + candidatesLeft + "\n" +
		 * sb.toString());
		 */

		return pushingKeys;
	}

	private void recordMigratedSize(int migratedSize) {
		if (!backPushStarted) {
			txCount++;
			totalMigratedSize += migratedSize;
			
			// XXX: No delayed background pushes
			startBackgroundPush();

			if (txCount >= TX_PER_RANGE) {
//				if (logger.isLoggable(Level.INFO)) {
//					logger.info("The number of migrated records in " + TX_PER_RANGE + " txs: " + totalMigratedSize);
//					logger.info("The convergence of migrated records in " + TX_PER_RANGE + " txs: "
//							+ ((double) Math.abs(prevTotalMigratedSize - totalMigratedSize)
//									/ (totalMigratedSize > 0 ? totalMigratedSize : Integer.MAX_VALUE)));
//				}

				// delta converges below threshold
//				if (Double.compare((double) Math.abs(prevTotalMigratedSize - totalMigratedSize)
//						/ (totalMigratedSize > 0 ? totalMigratedSize : Integer.MAX_VALUE), bgThreshold) < 0) {
//					startBackgroundPush();
//				}
				// XXX: Disable for the consolidation experiment
//				if (System.currentTimeMillis() - CalvinStoredProcedureTask.txStartTime > 1000000) {
//					startBackgroundPush();
//				}

				prevTotalMigratedSize = totalMigratedSize;
				txCount = 0;
				totalMigratedSize = 0;
			}
		}
		
		// for catching up request
		// XXX: Disable for the consolidation experiment
//		if (!crabbingReqSent) {
//			if (System.currentTimeMillis() - CalvinStoredProcedureTask.txStartTime > 120000) {
//				sendCrabbingRequest();
//			}
//		}
		
		// for caught up request
		// XXX: Disable for the consolidation experiment
//		if (!caughtUpReqSent) {
//			if (System.currentTimeMillis() - CalvinStoredProcedureTask.txStartTime > 1000000) {
//				sendCaughtUpRequest();
//			}
//		}
	}
	
	private void waitForCrabbing() {
		if (!crabbingReqSent) {
			if (System.currentTimeMillis() - CalvinStoredProcedureTask.txStartTime > crabbingStartTime) {
				sendCrabbingRequest();
			}
		}
	}
	
	private void waitForCaughtUp() {
		if (!caughtUpReqSent) {
			if (System.currentTimeMillis() - CalvinStoredProcedureTask.txStartTime > caughtUpStartTime) {
				sendCaughtUpRequest();
			}
		}
	}
	
	private void sendCrabbingRequest() {
		crabbingReqSent = true;

		// Use another thread to send catch up phase request
		VanillaDb.taskMgr().runTask(new Task() {
			@Override
			public void run() {
				TupleSet ts = new TupleSet(MigrationManager.SINK_ID_START_CRABBING);
				VanillaDdDb.connectionMgr().pushTupleSet(VanillaDdDb.migrationMgr().getSourcePartition(), ts);

				if (logger.isLoggable(Level.INFO))
					logger.info("Trigger catching up");
			}
		});
	}

	private void sendCaughtUpRequest() {
		caughtUpReqSent = true;

		// Use another thread to send catch up phase request
		VanillaDb.taskMgr().runTask(new Task() {
			@Override
			public void run() {
				TupleSet ts = new TupleSet(MigrationManager.SINK_ID_START_CATCH_UP);
				VanillaDdDb.connectionMgr().pushTupleSet(VanillaDdDb.migrationMgr().getSourcePartition(), ts);

				if (logger.isLoggable(Level.INFO))
					logger.info("Trigger catching up");
			}
		});
	}

	private void startBackgroundPush() {
		backPushStarted = true;
		
		// Only the source node can send the bg push request
		if (isSourceNode) {
			// Use another thread to start background pushing
			VanillaDb.taskMgr().runTask(new Task() {
				@Override
				public void run() {
					TupleSet ts = new TupleSet(MigrationManager.SINK_ID_ASYNC_PUSHING);
					VanillaDdDb.connectionMgr().pushTupleSet(VanillaDdDb.migrationMgr().getSourcePartition(), ts);
	
					if (logger.isLoggable(Level.INFO))
						logger.info("Trigger background pushing");
					
					long tCount = (System.currentTimeMillis() - CalvinStoredProcedureTask.txStartTime) / printStatusPeriod;
					StringBuilder sb = new StringBuilder();
					String preStr = "";
	
					for (String tableName : bgPushCandidates.keySet()) {
						sb.append(tableName);
						sb.append(":");
						sb.append(bgPushCandidates.get(tableName).size());
						sb.append(",");
					}
					preStr = "\nTable remain|" + sb.toString() + "\n";
					sb = new StringBuilder();
					for (long i = 0; i < tCount - 1; i++)
						sb.append(preStr);
					if (logger.isLoggable(Level.INFO))
						logger.info(sb.toString());
					while (true) {
						sb = new StringBuilder();
						for (String tableName : bgPushCandidates.keySet()) {
							sb.append(tableName);
							sb.append(":");
							sb.append(bgPushCandidates.get(tableName).size());
							sb.append(",");
						}
	
						if (logger.isLoggable(Level.INFO))
							logger.info("\nTable remain|" + sb.toString());
						try {
							Thread.sleep(printStatusPeriod);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
				}
			});
		}
	}
}
