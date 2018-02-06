package org.vanilladb.dd.schedule.calvin;

import java.sql.Connection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;
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
	
	// For simulating pull request
	private static final RecordKey PULL_REQUEST_KEY;
	private static final String DUMMY_FIELD1 = "dummy_field1";
	private static final String DUMMY_FIELD2 = "dummy_field2";
	
	static {
		// Create a pull key
		Map<String, Constant> keyEntryMap = new HashMap<String, Constant>();
		keyEntryMap.put(DUMMY_FIELD1, new IntegerConstant(0));
		PULL_REQUEST_KEY = new RecordKey("notification", keyEntryMap);
	}

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

	// Records
	private Map<RecordKey, CachedRecord> readings = new HashMap<RecordKey, CachedRecord>();
	
	// Migration
	private MigrationManager migraMgr = VanillaDdDb.migrationMgr();
	private boolean isSourceNode = (myNodeId == migraMgr.getSourcePartition());
	private boolean isDestNode = (myNodeId == migraMgr.getDestPartition());
	private Set<RecordKey> pullKeys = new HashSet<RecordKey>();
	private Set<RecordKey> readKeysInMigration = new HashSet<RecordKey>();
	private Set<RecordKey> writeKeysInMigration = new HashSet<RecordKey>();
	private Set<RecordKey> keysForBGPush = new HashSet<RecordKey>();
	protected boolean isExecutingInSrc = true;
	private boolean isInMigrating = false, isAnalyzing = false;
	private boolean activePulling = false;
	
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
		
		// prepare keys
		prepareKeys();

		// decide which node the master is
		masterNode = decideMaster();
		
		if (isInMigrating) {
			// Re-assign the master node
			if (masterNode == migraMgr.getSourcePartition() && !isExecutingInSrc)
				masterNode = migraMgr.getDestPartition();
			
			// The one executing the tx needs to take care the data in the migration range:
			if ((isExecutingInSrc && isSourceNode) || (!isExecutingInSrc && isDestNode)) {
				localReadKeys.addAll(readKeysInMigration);
				localWriteKeys.addAll(writeKeysInMigration);
			}
			if (!isExecutingInSrc) {
				if (isSourceNode)
					remoteReadKeys.addAll(readKeysInMigration);
				if (isDestNode)
					localWriteKeys.addAll(pullKeys);
			}
			
			// Other nodes should treat the source or the dest node as an active participant ?
			if (!writeKeysInMigration.isEmpty()) {
				if (isExecutingInSrc) {
					activeParticipants.add(migraMgr.getSourcePartition());
				} else {
					activeParticipants.add(migraMgr.getDestPartition());
				}
			}
			
			// Check if we need to pull data
			if (!isExecutingInSrc && !pullKeys.isEmpty()) {
				migraMgr.setRecordMigrated(pullKeys);
				activePulling = true;
			}

			// Add the inserted keys to the candidates for BG pushes
			if (isExecutingInSrc && isSourceNode) {
				for (RecordKey key : keysForBGPush)
					migraMgr.addNewInsertKey(key);
			}
		} else if (isAnalyzing) {
			// Add the inserted keys to the candidates for BG pushes
			if (isSourceNode) {
				for (RecordKey key : keysForBGPush)
					migraMgr.addNewInsertKey(key);
			}
		}
		
		// create transaction
		boolean isReadOnly = localWriteKeys.isEmpty();
		if (forceReadWriteTx)
			isReadOnly = false;
		this.tx = VanillaDdDb.txMgr().transaction(
				Connection.TRANSACTION_SERIALIZABLE, isReadOnly, txNum);
		this.ccMgr = (ConservativeOrderedCcMgr) tx.concurrencyMgr();
		this.tx.addLifecycleListener(new DdRecoveryMgr(tx
				.getTransactionNumber()));
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
		return masterNode == myNodeId;
	}

	public boolean isParticipated() {
		if (masterNode == myNodeId)
			return true;
		return !localReadKeys.isEmpty() || !localWriteKeys.isEmpty() || (isSourceNode && !pullKeys.isEmpty());
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
		// For pulling
		if (activePulling) {
			if (isSourceNode)
				pushMigrationData();
			else if (isDestNode)
				pullMigrationData();
		}
		
		// The destination may not involve
		if (isInMigrating && !isExecutingInSrc && isSourceNode)
			return;
		
		// Read the local records
		performLocalRead();

		// Push local records to the needed remote nodes
		if (!activeParticipants.isEmpty() || !isMasterNode())
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
			// Check the migration status
			if (!migraMgr.isRecordMigrated(readKey)) {
				pullKeys.add(readKey);
			} else
				isExecutingInSrc = false;
			
			readKeysInMigration.add(readKey);
			if (!isSourceNode && !isDestNode)
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
			// Check the migration status
			if (!migraMgr.isRecordMigrated(writeKey)) {
				pullKeys.add(writeKey);
			} else {
				isExecutingInSrc = false;
			}
			
			writeKeysInMigration.add(writeKey);
		}
	}
	
	protected void addInsertKey(RecordKey insertKey) {
		// Check which node has the corresponding record
		int dataPartId = VanillaDdDb.partitionMetaMgr().getPartition(insertKey);
		writePerNode[dataPartId]++;
		
		if ((isAnalyzing || isInMigrating) && migraMgr.keyIsInMigrationRange(insertKey)) {
			keysForBGPush.add(insertKey);
		}

		// Normal
		if (!isInMigrating || !migraMgr.keyIsInMigrationRange(insertKey)) {
			if (dataPartId == myNodeId)
				localWriteKeys.add(insertKey);
			activeParticipants.add(dataPartId);
		} else { // Migrating
			writeKeysInMigration.add(insertKey);
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
	
	protected void waitForPullRequest() {
		CachedRecord rec = cacheMgr.read(PULL_REQUEST_KEY, txNum, tx, false);
		int value = (int) rec.getVal(DUMMY_FIELD1).asJavaVal();
		if (value != 0)
			throw new RuntimeException("something wrong for the pull request of tx." + txNum);
	}
	
	protected void sendAPullRequest(int nodeId) {
		Map<String, Constant> fldVals = new HashMap<String, Constant>();
		fldVals.put(DUMMY_FIELD1, new IntegerConstant(0));
		fldVals.put(DUMMY_FIELD2, new IntegerConstant(0));
		CachedRecord rec = new CachedRecord(fldVals);
		rec.setSrcTxNum(txNum);

		TupleSet ts = new TupleSet(-1);
		ts.addTuple(PULL_REQUEST_KEY, txNum, txNum, rec);
		VanillaDdDb.connectionMgr().pushTupleSet(nodeId, ts);
	}

	private void performLocalRead() {
		Map<RecordKey, CachedRecord> localReadings = new HashMap<RecordKey, CachedRecord>();

		// Read local records
		for (RecordKey k : localReadKeys) {
			if (!readings.containsKey(k)) {
				CachedRecord rec = cacheMgr.read(k, txNum, tx, true);
				if (rec == null)
					throw new RuntimeException("Tx. " + txNum + " cannot find the record for "
							+ k + " in the local storage.");
				readings.put(k, rec);
				localReadings.put(k, rec);
			} else {
				localReadings.put(k, readings.get(k));
			}
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
		PartitionMetaMgr partMgr = VanillaDdDb.partitionMetaMgr();
		Set<RecordKey> pushKeys = new HashSet<RecordKey>(readings.keySet());
		TupleSet ts = new TupleSet(-1);
		
		for (RecordKey key : pushKeys) {
			if (!partMgr.isFullyReplicated(key))
				ts.addTuple(key, txNum, txNum, readings.get(key));
		}
		
		if (!pushKeys.isEmpty()) {
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
	}
	
	// Only for the source node
	private void pushMigrationData() {
		// Wait for pull request
		waitForPullRequest();
		
		// Perform reading for pulling data
		TupleSet ts = new TupleSet(-1);
		for (RecordKey k : pullKeys) {
			CachedRecord rec = cacheMgr.read(k, txNum, tx, true);
			if (rec == null)
				throw new RuntimeException("Tx. " + txNum + " cannot find the record for "
						+ k + " in the local storage.");
			ts.addTuple(k, txNum, txNum, rec);
		}
		
		// Push migration data set
		VanillaDdDb.connectionMgr().pushTupleSet(migraMgr.getDestPartition(), ts);
	}
	
	// Only for the destination node
	private void pullMigrationData() {
		// Send pull request
		sendAPullRequest(migraMgr.getSourcePartition());
		
		// Receiving migration data set
		for (RecordKey k : pullKeys) {
			CachedRecord rec = cacheMgr.read(k, txNum, tx, false);
			cacheMgr.markWriteback(k, tx);
			readings.put(k, rec);
		}
	}

	private void collectRemoteReadings() {
		Map<RecordKey, CachedRecord> remoteReadings = new HashMap<RecordKey, CachedRecord>();
		
		// Read remote records
		for (RecordKey k : remoteReadKeys) {
			CachedRecord rec = cacheMgr.read(k, txNum, tx, false);
			readings.put(k, rec);
			remoteReadings.put(k, rec);
		}

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
		
		// remove pull request
		cacheMgr.remove(PULL_REQUEST_KEY, txNum);
	}
}
