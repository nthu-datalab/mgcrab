package org.vanilladb.dd.schedule.calvin;

import java.sql.Connection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.storedprocedure.StoredProcedureParamHelper;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.dd.cache.CachedRecord;
import org.vanilladb.dd.cache.calvin.CalvinCacheMgr;
import org.vanilladb.dd.remote.groupcomm.TupleSet;
import org.vanilladb.dd.schedule.DdStoredProcedure;
import org.vanilladb.dd.server.VanillaDdDb;
import org.vanilladb.dd.server.migration.MigrationManager;
import org.vanilladb.dd.sql.RecordKey;
import org.vanilladb.dd.storage.metadata.PartitionMetaMgr;
import org.vanilladb.dd.storage.tx.concurrency.ConservativeOrderedCcMgr;
import org.vanilladb.dd.storage.tx.recovery.DdRecoveryMgr;

public abstract class CalvinStoredProcedure<H extends StoredProcedureParamHelper>
		implements DdStoredProcedure {
	
//	private static AtomicInteger fgCount = new AtomicInteger(0);
//	private static AtomicInteger bgCount = new AtomicInteger(0);
//	
//	static {
//		new PeriodicalJob(3000, 500000, new Runnable() {
//	
//			@Override
//			public void run() {
//				System.out.println("FG-Push Count: " + fgCount.get());
//				System.out.println("BG-Push Count: " + bgCount.get());
//			}
//			
//		}).start();
//	}

	// Protected resource
	protected Transaction tx;
	protected long txNum;
	protected H paramHelper;
	protected int myNodeId = VanillaDdDb.serverId();
	protected boolean forceReadWriteTx = false;
	protected boolean forceParticipated = false;

	// Participants
	// Active Participants: Nodes that need to write records locally
	// Passive Participants: Nodes that only need to read records and push
	private Set<Integer> activeParticipants = new HashSet<Integer>();
	// private Set<Integer> passiveParticipants = new HashSet<Integer>();

	// Master node decision
	private int[] readPerNode = new int[PartitionMetaMgr.NUM_PARTITIONS];
	private int[] writePerNode = new int[PartitionMetaMgr.NUM_PARTITIONS];
	// Temp fix for the problem that the read-only tx executes in the dest node
	// but it cannot receive the records from the others
	private Set<Integer> masterNodes = new HashSet<Integer>();

	// Record keys
	private Set<String> localReadTables = new HashSet<String>();
	private Set<String> localWriteTables = new HashSet<String>();
	private Set<RecordKey> fullyRepKeys = new HashSet<RecordKey>();
	private Set<RecordKey> localReadKeys = new HashSet<RecordKey>();
	private Set<RecordKey> localWriteKeys = new HashSet<RecordKey>();
	private Set<RecordKey> remoteReadKeys = new HashSet<RecordKey>();

	// Records
	private Map<RecordKey, CachedRecord> readings = new HashMap<RecordKey, CachedRecord>();
	
	// Migration
	private MigrationManager migraMgr = VanillaDdDb.migrationMgr();
	private boolean isSourceNode = (myNodeId == migraMgr.getSourcePartition());
	private boolean isDestNode = (myNodeId == migraMgr.getDestPartition());
	private Set<RecordKey> migratedReadKeys = new HashSet<RecordKey>();
	private Set<RecordKey> migratingKeys = new HashSet<RecordKey>();
	private Set<RecordKey> writeKeysInMigrationRange = new HashSet<RecordKey>();
	private boolean isInMigrating = false, isCrabbing = false, isAnalyzing = false;
	private boolean hasWriteInMigraitonRange = false;
	
	private CalvinCacheMgr cacheMgr = (CalvinCacheMgr) VanillaDdDb.cacheMgr();

	private ConservativeOrderedCcMgr ccMgr;
	private String[] readTablesForLock, writeTablesForLock;
	private RecordKey[] readKeysForLock, writeKeysForLock;

	public CalvinStoredProcedure(long txNum, H paramHelper) {
		this.txNum = txNum;
		this.paramHelper = paramHelper;

		if (paramHelper == null)
			throw new NullPointerException("paramHelper should not be null");
	}

	/*******************
	 * Abstract methods
	 *******************/

	/**
	 * Prepare the RecordKey for each record to be used in this stored
	 * procedure. Use the {@link #addReadKey(RecordKey)},
	 * {@link #addWriteKey(RecordKey)} method to add keys.
	 */
	protected abstract void prepareKeys();

	protected abstract void onLocalReadCollected(
			Map<RecordKey, CachedRecord> localReadings);

	protected abstract void onRemoteReadCollected(
			Map<RecordKey, CachedRecord> remoteReadings);

	protected abstract void writeRecords(
			Map<RecordKey, CachedRecord> remoteReadings);

	protected abstract void masterCollectResults(
			Map<RecordKey, CachedRecord> readings);

	/**********************
	 * implemented methods
	 **********************/

	public void prepare(Object... pars) {
		// check if this transaction is in a migration period
		isInMigrating = migraMgr.isMigrating();
		isCrabbing = migraMgr.isCrabbingPhase();
		isAnalyzing = migraMgr.isAnalyzing();
		
		// prepare parameters
		paramHelper.prepareParameters(pars);
		
		// XXX: I am sure that why this should be executed first.
		// prepare keys
		prepareKeys();
		
		// decide which node the master is
		masterNodes.add(decideMaster());
		
		// For the keys in migration range
		if (isInMigrating)
			prepareKeysInMigrationRange();
		
		// create transaction
		boolean isReadOnly = localWriteKeys.isEmpty();
		if (forceReadWriteTx)
			isReadOnly = false;
		this.tx = VanillaDdDb.txMgr().transaction(
				Connection.TRANSACTION_SERIALIZABLE, isReadOnly, txNum);
		this.ccMgr = (ConservativeOrderedCcMgr) tx.concurrencyMgr();
		this.tx.addLifecycleListener(new DdRecoveryMgr(tx
				.getTransactionNumber()));

		// the records in the migration set should be set as migrated
		if (isInMigrating) {
			migraMgr.setRecordMigrated(migratingKeys);
//			if (isBgPush)
//				bgCount.addAndGet(migratingKeys.size());
//			else
//				fgCount.addAndGet(migratingKeys.size());
		}
	}
	
//	protected boolean isBgPush = false;

	public void requestConservativeLocks() {
		readTablesForLock = localReadTables.toArray(new String[0]);
		writeTablesForLock = localWriteTables.toArray(new String[0]);

		readKeysForLock = localReadKeys.toArray(new RecordKey[0]);
		writeKeysForLock = localWriteKeys.toArray(new RecordKey[0]);
		
		// XXX: We do not lock the fully replicated keys,
		// since we believe those record will not be modified.

		ccMgr.prepareSp(readTablesForLock, writeTablesForLock);
		ccMgr.prepareSp(readKeysForLock, writeKeysForLock);
	}

	private void getConservativeLocks() {
		ccMgr.executeSp(readTablesForLock, writeTablesForLock);
		ccMgr.executeSp(readKeysForLock, writeKeysForLock);
	}

	@Override
	public final RecordKey[] getReadSet() {
		return localReadKeys.toArray(new RecordKey[0]);
	}

	@Override
	public final RecordKey[] getWriteSet() {
		return localWriteKeys.toArray(new RecordKey[0]);
	}

	@Override
	public SpResultSet execute() {
		try {
			// Get conservative locks it has asked before
			getConservativeLocks();
			
			// Execute transaction
			executeTransactionLogic();

			// The transaction finishes normally
			tx.commit();
			
			// Something might be done after committing
			afterCommit();

		} catch (Exception e) {
			e.printStackTrace();
			tx.rollback();
			paramHelper.setCommitted(false);
		}
		if(isMasterNode())
			return paramHelper.createResultSet();
		return null;
	}

	/**
	 * Returns true if this machine is the master node which is responsible for
	 * sending response back to client.
	 * 
	 * @return
	 */
	public boolean isMasterNode() {
		// While migrating, if the source node is the master,
		// the destination can also be another master for early response.
		return masterNodes.contains(myNodeId);
	}

	public boolean isParticipated() {
		if (masterNodes.contains(myNodeId))
			return true;
		
		if (forceParticipated)
			return true;
		
		return !localReadKeys.isEmpty() || !localWriteKeys.isEmpty();
	}

	@Override
	public boolean isReadOnly() {
		return paramHelper.isReadOnly();
	}

	/**
	 * Choose a node be the master node. The master node must collect the
	 * readings from other nodes and take responsibility for reporting to
	 * clients. This method can be overridden if a developer wants to use
	 * another election algorithm. The default algorithm chooses the node which
	 * has the most writings or readings to be a master node. It has to be
	 * noticed that the algorithm must be deterministic on all server nodes.
	 * 
	 * @return the master node id
	 */
	protected int decideMaster() {
		int maxValue = -1;
		int masterId = -1;

		// Let the node with the most writings be the master
		for (int nodeId = 0; nodeId < writePerNode.length; nodeId++) {
			if (maxValue < writePerNode[nodeId]) {
				maxValue = writePerNode[nodeId];
				masterId = nodeId;
			}
		}

		if (maxValue > 0)
			return masterId;

		// Let the node with the most readings be the master
		for (int nodeId = 0; nodeId < readPerNode.length; nodeId++) {
			if (maxValue < readPerNode[nodeId]) {
				maxValue = readPerNode[nodeId];
				masterId = nodeId;
			}
		}

		return masterId;
	}

	/**
	 * This method will be called by execute(). The default implementation of
	 * this method follows the steps described by Calvin paper.
	 */
	protected void executeTransactionLogic() {
		// Read the local records
		performLocalRead();
		
		// Perform foreground pushing
		if (!migratingKeys.isEmpty()) {
			if (isSourceNode)
				pushMigrationData();
			else if (isDestNode)
				waitMigrationData();
		}

		// Push local records to the needed remote nodes
		pushReadingsToRemotes();

		// Read the remote records
		if (activeParticipants.contains(myNodeId) || isMasterNode())
			collectRemoteReadings();

		// Write the local records
		if (activeParticipants.contains(myNodeId))
			writeRecords(readings);

		// The master node must collect final results
		if (isMasterNode())
			masterCollectResults(readings);

		// Remove the cached records in CacheMgr
		removeCachedRecords();
	}
	
	protected void addBuPushKey(RecordKey bgKey) {
		if (isSourceNode || isDestNode) 
			migratingKeys.add(bgKey);
	}

	protected void addReadKey(RecordKey readKey) {
		// Check which node has the corresponding record
		int dataPartId = VanillaDdDb.partitionMetaMgr().getPartition(readKey);
		
		if (!VanillaDdDb.partitionMetaMgr().isFullyReplicated(readKey)) {
			readPerNode[dataPartId]++;
		} else {
			fullyRepKeys.add(readKey);
			return;
		}
		
		// Normal
		if (!isInMigrating || !migraMgr.keyIsInMigrationRange(readKey)) {
			if (dataPartId == myNodeId)
				localReadKeys.add(readKey);
			else
				remoteReadKeys.add(readKey);
		} else { // Migrating
			if (isSourceNode || isDestNode) {
				if (!migraMgr.isRecordMigrated(readKey))
					migratingKeys.add(readKey);
				else
					migratedReadKeys.add(readKey);
			} else
				remoteReadKeys.add(readKey);
		}
	}

	protected void addWriteKey(RecordKey writeKey) {
		// Check which node has the corresponding record
		int dataPartId = VanillaDdDb.partitionMetaMgr().getPartition(writeKey);
		writePerNode[dataPartId]++;

		// Normal
		if (!isInMigrating || !migraMgr.keyIsInMigrationRange(writeKey)) {
			if (dataPartId == myNodeId)
				localWriteKeys.add(writeKey);
			activeParticipants.add(dataPartId);
		} else { // Migrating
			if (isSourceNode || isDestNode) {
				if (!migraMgr.isRecordMigrated(writeKey))
					migratingKeys.add(writeKey);
				writeKeysInMigrationRange.add(writeKey);
			}
			hasWriteInMigraitonRange = true;
		}
	}
	
	protected void addInsertKey(RecordKey insertKey) {
		// Check which node has the corresponding record
		int dataPartId = VanillaDdDb.partitionMetaMgr().getPartition(insertKey);
		writePerNode[dataPartId]++;
		
		if (isSourceNode && isAnalyzing &&
				migraMgr.keyIsInMigrationRange(insertKey)) {
			migraMgr.addNewInsertKey(insertKey);
		}

		// Normal
		if (!isInMigrating || !migraMgr.keyIsInMigrationRange(insertKey)) {
			if (dataPartId == myNodeId)
				localWriteKeys.add(insertKey);
			activeParticipants.add(dataPartId);
		} else { // Migrating
			if (isSourceNode || isDestNode)
				writeKeysInMigrationRange.add(insertKey);
			hasWriteInMigraitonRange = true;
		}
	}

	protected Map<RecordKey, CachedRecord> getReadings() {
		return readings;
	}

	protected Set<RecordKey> getLocalWriteKeys() {
		return localWriteKeys;
	}
	
	protected void update(RecordKey key, CachedRecord rec) {
		if (localWriteKeys.contains(key))
			cacheMgr.update(key, rec, tx);
	}
	
	protected void insert(RecordKey key, Map<String, Constant> fldVals) {
		if (localWriteKeys.contains(key))
			cacheMgr.insert(key, fldVals, tx);
	}
	
	protected void afterCommit() {
		// do nothing
	}
	
	private void prepareKeysInMigrationRange() {
		if (isCrabbing) {
			// Re-assign the master node
			if (masterNodes.contains(migraMgr.getDestPartition()))
				masterNodes.add(migraMgr.getSourcePartition());
			
			// Active Participants
			// The pushing mechanism between the source node and the dest node
			// is different from the one between normal nodes.
			// Therefore, the source node and the dest node will not treat
			// each other as 'active participants'.
			if (hasWriteInMigraitonRange) {
				if (isSourceNode)
					activeParticipants.add(migraMgr.getSourcePartition());
				else if (isDestNode)
					activeParticipants.add(migraMgr.getDestPartition());
				else {
					activeParticipants.add(migraMgr.getSourcePartition());
					activeParticipants.add(migraMgr.getDestPartition());
				}
			}
			
			// Local read & write Keys
			if (isSourceNode) {
				localReadKeys.addAll(migratingKeys);
				localReadKeys.addAll(migratedReadKeys);
				localWriteKeys.addAll(writeKeysInMigrationRange);
			} else if (isDestNode) {
				localReadKeys.addAll(migratedReadKeys);
				localWriteKeys.addAll(migratingKeys);
				localWriteKeys.addAll(writeKeysInMigrationRange);
			}
			
		} else {
			// Active Participants
			// The dest node is the only one who own the data in migration range
			if (hasWriteInMigraitonRange) {
				if (!isSourceNode) {
					activeParticipants.add(migraMgr.getDestPartition());
				}
			}
			
			// Local read & write Keys
			if (isSourceNode) {
				// The source node doesn't need to write the data
				// but it still needs to read the data in the migration range 
				localReadKeys.addAll(migratingKeys);
				// We make the source node collect the data from the dest node
				// to avoid memory leak in CacheMgr.
				// But it actually can read the data from local
//				localReadKeys.addAll(migratedReadKeys);
				remoteReadKeys.addAll(migratedReadKeys);
			} else if (isDestNode) {
				localReadKeys.addAll(migratedReadKeys);
				localWriteKeys.addAll(migratingKeys);
				localWriteKeys.addAll(writeKeysInMigrationRange);
			}
		}
	}
	
	// Only for the source node
	private void pushMigrationData() {
		// Construct push set
		TupleSet ts = new TupleSet(-1);
		for (RecordKey k : migratingKeys)
			ts.addTuple(k, txNum, txNum, readings.get(k));
		
		// Push migration data set
		VanillaDdDb.connectionMgr().pushTupleSet(migraMgr.getDestPartition(), ts);
	}
	
	// Only for the destination node
	private void waitMigrationData() {
		// Receiving migration data set
		for (RecordKey k : migratingKeys) {
			CachedRecord rec = cacheMgr.read(k, txNum, tx, false);
			cacheMgr.markWriteback(k, tx);
			readings.put(k, rec);
		}
	}

	private void performLocalRead() {
		Map<RecordKey, CachedRecord> localReadings = new HashMap<RecordKey, CachedRecord>();

		// Read local records
		for (RecordKey k : localReadKeys) {
			CachedRecord rec = cacheMgr.read(k, txNum, tx, true);
			if (rec == null)
				throw new RuntimeException("Tx. " + txNum + " cannot find the record for "
						+ k + " in the local storage.");
			readings.put(k, rec);
			localReadings.put(k, rec);
		}
		
		// Read fully replicated records
		for (RecordKey k : fullyRepKeys) {
			CachedRecord rec = cacheMgr.read(k, txNum, tx, true);
			if (rec == null)
				throw new RuntimeException("Tx. " + txNum + " cannot find the record for "
						+ k + " in the local storage.");
			readings.put(k, rec);
			localReadings.put(k, rec);
		}

		// Notify the implementation of subclass
		onLocalReadCollected(localReadings);
	}
	
	private void pushReadingsToRemotes() {
		// The destination node does not need to send data.
		// The source will take care of it.
		if (isCrabbing && isDestNode)
			return;
		
		PartitionMetaMgr partMgr = VanillaDdDb.partitionMetaMgr();
		TupleSet ts = new TupleSet(-1);
		
		// For other node
		if (!readings.isEmpty()) {
			// Construct a pushing tuple set
			for (Entry<RecordKey, CachedRecord> e : readings.entrySet()) {
				if (!partMgr.isFullyReplicated(e.getKey()))
					ts.addTuple(e.getKey(), txNum, txNum, e.getValue());
			}
			
			if (!ts.isEmpty()) {
				// Push to all active participants
				for (Integer n : activeParticipants)
					if (n != myNodeId && !masterNodes.contains(n))
						VanillaDdDb.connectionMgr().pushTupleSet(n, ts);
	
				// Push a set to the master node
				for (Integer n : masterNodes) {
					if (n != myNodeId && !(isSourceNode && n == migraMgr.getDestPartition()))
						VanillaDdDb.connectionMgr().pushTupleSet(n, ts);
				}
			}
		}
		
		// For the source node to destination node
		if (isSourceNode && (hasWriteInMigraitonRange || masterNodes.contains(migraMgr.getDestPartition()))) {
			// Construct a pushing tuple set
			ts = new TupleSet(-1);
			
			// Only push the data not in the migration range
			for (RecordKey key : localReadKeys) {
				if (migratingKeys.contains(key))
					continue;
				if (migratedReadKeys.contains(key))
					continue;
				if (partMgr.isFullyReplicated(key))
					continue;
				
				ts.addTuple(key, txNum, txNum, readings.get(key));
			}

			// Push to the destination node
			if (!ts.isEmpty())
				VanillaDdDb.connectionMgr().pushTupleSet(migraMgr.getDestPartition(), ts);
		}
	}

	private void collectRemoteReadings() {
		Map<RecordKey, CachedRecord> remoteReadings = new HashMap<RecordKey, CachedRecord>();
//		System.out.println("Tx." + txNum + " collect remote records: " + remoteReadKeys);
		// Read remote records
		for (RecordKey k : remoteReadKeys) {
//			System.out.println("Tx." + txNum + " read remote record: " + k);
			CachedRecord rec = cacheMgr.read(k, txNum, tx, false);
			readings.put(k, rec);
			remoteReadings.put(k, rec);
		}
//		System.out.println("Tx." + txNum + " finish collecting remote records.");

		// Notify the implementation of subclass
		onRemoteReadCollected(remoteReadings);
	}

	private void removeCachedRecords() {
		// Write to local storage and remove cached records
		for (RecordKey k : localWriteKeys) {
			if (!cacheMgr.flushToLocalStorage(k, txNum, tx))
				throw new RuntimeException("Tx." + txNum + " is trying to flush a non-existent record: " + k);
			cacheMgr.remove(k, txNum);
		}

		// Remove cached read records
		for (RecordKey k : fullyRepKeys)
			cacheMgr.remove(k, txNum);
		for (RecordKey k : localReadKeys)
			cacheMgr.remove(k, txNum);
		for (RecordKey k : remoteReadKeys)
			cacheMgr.remove(k, txNum);
	}
}
