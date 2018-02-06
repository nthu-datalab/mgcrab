package org.vanilladb.dd.schedule.calvin;

import java.sql.Connection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

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

	// Protected resource
	protected Transaction tx;
	protected long txNum;
	protected H paramHelper;
	protected int myNodeId = VanillaDdDb.serverId();
	protected boolean forceReadWriteTx = false;

	// Participants
	// Active Participants: Nodes that need to write records locally
	// Passive Participants: Nodes that only need to read records and push
	private Set<Integer> activeParticipants = new HashSet<Integer>();
	// private Set<Integer> passiveParticipants = new HashSet<Integer>();

	// Master node decision
	private int[] readPerNode = new int[PartitionMetaMgr.NUM_PARTITIONS];
	private int[] writePerNode = new int[PartitionMetaMgr.NUM_PARTITIONS];
	private int masterNode;

	// Record keys
	private Set<String> localReadTables = new HashSet<String>();
	private Set<String> localWriteTables = new HashSet<String>();
	private Set<RecordKey> fullyRepKeys = new HashSet<RecordKey>();
	private Set<RecordKey> localReadKeys = new HashSet<RecordKey>();
	private Set<RecordKey> localWriteKeys = new HashSet<RecordKey>();
	private Set<RecordKey> remoteReadKeys = new HashSet<RecordKey>();
	private Set<RecordKey> keysForBGPush = new HashSet<RecordKey>();

	// Records
	private Map<RecordKey, CachedRecord> readings = new HashMap<RecordKey, CachedRecord>();
	
	// Migration
	private MigrationManager migraMgr = VanillaDdDb.migrationMgr();
	private boolean isSourceNode = (myNodeId == migraMgr.getSourcePartition());
	private boolean isDestNode = (myNodeId == migraMgr.getDestPartition());
	private Set<RecordKey> migrationSet = new HashSet<RecordKey>();
	private boolean isInMigrating = false, isAnalyzing = false;
	private boolean sourceTreatDestAsActive = false;
	
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
		isAnalyzing = migraMgr.isAnalyzing();
		
		// prepare parameters
		paramHelper.prepareParameters(pars);
		
		// XXX: I am sure that why this should be executed first.
		// prepare keys
		prepareKeys();
		
		// create transaction
		boolean isReadOnly = localWriteKeys.isEmpty();
		if (forceReadWriteTx)
			isReadOnly = false;
		this.tx = VanillaDdDb.txMgr().transaction(
				Connection.TRANSACTION_SERIALIZABLE, isReadOnly, txNum);
		this.ccMgr = (ConservativeOrderedCcMgr) tx.concurrencyMgr();
		this.tx.addLifecycleListener(new DdRecoveryMgr(tx
				.getTransactionNumber()));

		// decide which node the master is
		masterNode = decideMaster();

		// the records in the migration set should be set as migrated
		for (RecordKey key : migrationSet)
			migraMgr.setRecordMigrated(key);
		
		if (isAnalyzing) {
			// Add the inserted keys to the candidates for BG pushes
			if (isSourceNode) {
				for (RecordKey key : keysForBGPush)
					migraMgr.addNewInsertKey(key);
			}
		}
	}

	public void requestConservativeLocks() {
		readTablesForLock = localReadTables.toArray(new String[0]);
		writeTablesForLock = localWriteTables.toArray(new String[0]);

		readKeysForLock = localReadKeys.toArray(new RecordKey[0]);
		writeKeysForLock = localWriteKeys.toArray(new RecordKey[0]);

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

		return paramHelper.createResultSet();
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
		return masterNode == myNodeId ||
				(isInMigrating && isDestNode && activeParticipants.contains(myNodeId));
	}

	public boolean isParticipated() {
		if (masterNode == myNodeId)
			return true;
		return localReadTables.size() != 0 || localWriteTables.size() != 0
				|| localReadKeys.size() != 0 || localWriteKeys.size() != 0;
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

		// Push local records to the needed remote nodes
		if (!activeParticipants.isEmpty() || !isMasterNode() || !migrationSet.isEmpty())
			pushReadingsToRemotes();

		// Read the remote records
		if (activeParticipants.contains(myNodeId) || isMasterNode())
			collectRemoteReadings();
		
		// Collect the migrated records from the source
		if (isInMigrating && isDestNode && !migrationSet.isEmpty())
			collectMigratedRecords();

		// Write the local records
		if (activeParticipants.contains(myNodeId))
			writeRecords(readings);

		// The master node must collect final results
		if (isMasterNode())
			masterCollectResults(readings);

		// Remove the cached records in CacheMgr
		removeCachedRecords();
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
			if (isSourceNode) {
				localReadKeys.add(readKey);
				if (!migraMgr.isRecordMigrated(readKey)) {
					migrationSet.add(readKey);
				}
			} else if (isDestNode) {
				if (migraMgr.isRecordMigrated(readKey)) {
					localReadKeys.add(readKey);
				} else {
					migrationSet.add(readKey);
					localWriteKeys.add(readKey);
					// TODO: Need add to remote keys ?
				}
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
			if (isSourceNode) { // For the source node
				localWriteKeys.add(writeKey);
				activeParticipants.add(migraMgr.getSourcePartition());
				sourceTreatDestAsActive = true;
				
				if (!migraMgr.isRecordMigrated(writeKey)) {
					localReadKeys.add(writeKey);
					migrationSet.add(writeKey);
				}
			} else if (isDestNode) { // For the dest. node
				localWriteKeys.add(writeKey);
				activeParticipants.add(migraMgr.getDestPartition());
				
				if (!migraMgr.isRecordMigrated(writeKey)) {
					migrationSet.add(writeKey);
				}
			} else {
				// For other nodes (the migrating record must not be in these nodes)
				activeParticipants.add(migraMgr.getSourcePartition());
				activeParticipants.add(migraMgr.getDestPartition());
			}
		}
	}
	
	protected void addInsertKey(RecordKey insertKey) {
		// Check which node has the corresponding record
		int dataPartId = VanillaDdDb.partitionMetaMgr().getPartition(insertKey);
		
		if (isAnalyzing && migraMgr.keyIsInMigrationRange(insertKey))
			keysForBGPush.add(insertKey);

		writePerNode[dataPartId]++;

		// Normal
		if (!isInMigrating || !migraMgr.keyIsInMigrationRange(insertKey)) {
			if (dataPartId == myNodeId)
				localWriteKeys.add(insertKey);
			activeParticipants.add(dataPartId);
		} else { // Migrating
			if (isSourceNode) { // For the source node
				localWriteKeys.add(insertKey);
				activeParticipants.add(migraMgr.getSourcePartition());
				sourceTreatDestAsActive = true;
			} else if (isDestNode) { // For the dest. node
				localWriteKeys.add(insertKey);
				activeParticipants.add(migraMgr.getDestPartition());
			} else {
				// For other nodes (the migrating record must not be in these nodes)
				activeParticipants.add(migraMgr.getSourcePartition());
				activeParticipants.add(migraMgr.getDestPartition());
			}
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
	
	public static Set<RecordKey> pushedKeys = Collections.newSetFromMap(
			new ConcurrentHashMap<RecordKey, Boolean>());
	public static Set<RecordKey> savedKeys = Collections.newSetFromMap(
			new ConcurrentHashMap<RecordKey, Boolean>());

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
		if (isInMigrating && isDestNode)
			return;
		
		PartitionMetaMgr partMgr = VanillaDdDb.partitionMetaMgr();
		TupleSet ts = new TupleSet(-1);
		if (!readings.isEmpty()) {
			// Construct a pushing tuple set
			for (Entry<RecordKey, CachedRecord> e : readings.entrySet()) {
				if (!partMgr.isFullyReplicated(e.getKey()))
					ts.addTuple(e.getKey(), txNum, txNum, e.getValue());
			}
			
			if (!ts.isEmpty()) {
				// Push to all active participants
				for (Integer n : activeParticipants)
					if (n != myNodeId)
						VanillaDdDb.connectionMgr().pushTupleSet(n, ts);
	
				// Push a set to the master node
				if (!activeParticipants.contains(masterNode) && !isMasterNode())
					VanillaDdDb.connectionMgr().pushTupleSet(masterNode, ts);
			}
		}
		
		// The source node also need to push the data that the destination node needs:
		// (1) The data which are not in migration range, but the dest node needs them
		// (2) The data which need to be migrated
		if (isInMigrating && isSourceNode) {
			// Construct a pushing tuple set
			ts = new TupleSet(-1);
			// For the first class of data
			if (sourceTreatDestAsActive) {
				for (Entry<RecordKey, CachedRecord> e : readings.entrySet()) {
					if (!partMgr.isFullyReplicated(e.getKey()) &&
							!migraMgr.keyIsInMigrationRange(e.getKey()))
						ts.addTuple(e.getKey(), txNum, txNum, e.getValue());
				}
			}
			// For the second class of data
			for (RecordKey key : migrationSet) {
				ts.addTuple(key, txNum, txNum, readings.get(key));
			}

			// Push to the destination node
			if (!ts.isEmpty())
				VanillaDdDb.connectionMgr().pushTupleSet(migraMgr.getDestPartition(), ts);
		}
	}
	
	private void collectMigratedRecords() {
		// Read the migration records from the source node
//		System.out.println("Tx." + txNum + " collect migration records.");
		for (RecordKey k : migrationSet) {
			CachedRecord rec = cacheMgr.read(k, txNum, tx, false);
			readings.put(k, rec);
			cacheMgr.markWriteback(k, tx);
		}
//		System.out.println("Tx." + txNum + " finish collecting migration records.");
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
		// write to local storage and remove cached records
		for (RecordKey k : localWriteKeys) {
			if (!cacheMgr.flushToLocalStorage(k, txNum, tx))
				throw new RuntimeException("Tx." + txNum + " is trying to flush a non-existent record: " + k);
			cacheMgr.remove(k, txNum);
		}

		// remove cached read records
		for (RecordKey k : fullyRepKeys)
			cacheMgr.remove(k, txNum);
		for (RecordKey k : localReadKeys)
			cacheMgr.remove(k, txNum);
		for (RecordKey k : remoteReadKeys)
			cacheMgr.remove(k, txNum);
	}
}
