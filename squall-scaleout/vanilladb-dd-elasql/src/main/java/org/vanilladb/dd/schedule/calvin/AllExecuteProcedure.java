package org.vanilladb.dd.schedule.calvin;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.storedprocedure.StoredProcedureParamHelper;
import org.vanilladb.dd.cache.CachedRecord;
import org.vanilladb.dd.cache.calvin.CalvinCacheMgr;
import org.vanilladb.dd.remote.groupcomm.TupleSet;
import org.vanilladb.dd.server.VanillaDdDb;
import org.vanilladb.dd.sql.RecordKey;
import org.vanilladb.dd.storage.metadata.PartitionMetaMgr;

public abstract class AllExecuteProcedure<H extends StoredProcedureParamHelper>
		extends CalvinStoredProcedure<H> {
	private static Logger logger = Logger.getLogger(AllExecuteProcedure.class
			.getName());

	private static final String NOTIFICATION_FILED_NAME = "finish";

	public AllExecuteProcedure(long txNum, H paramHelper) {
		super(txNum, paramHelper);
	}

	/**
	 * Only return true in order to force all nodes to participate.
	 * 
	 * @return true
	 */
	public boolean isParticipated() {
		return true;
	}

	@Override
	protected void prepareKeys() {
		// Do nothing
	}

	@Override
	protected int decideMaster() {
		// The first node be the master node
		return 0;
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
		// Do nothing
	}

	@Override
	protected void masterCollectResults(Map<RecordKey, CachedRecord> readings) {
		// Do nothing
	}

	protected void executeTransactionLogic() {
		executeSql();

		// Notification for finish
		if (isMasterNode()) {
			if (logger.isLoggable(Level.INFO))
				logger.info("Waiting for other servers...");

			// Master: Wait for notification from other nodes
			waitForNotification();
			
			if (logger.isLoggable(Level.INFO))
				logger.info("Other servers completion comfirmed.");
		} else {
			// Salve: Send notification to the master
			sendNotification();
		}
	}

	protected abstract void executeSql();

	private void waitForNotification() {
		CalvinCacheMgr cm = (CalvinCacheMgr) VanillaDdDb.cacheMgr();

		// Wait for notification from other nodes
		for (int nodeId = 0; nodeId < PartitionMetaMgr.NUM_PARTITIONS; nodeId++)
			if (nodeId != VanillaDdDb.serverId()) {
				RecordKey notKey = getFinishNotificationKey(nodeId);
				CachedRecord rec = cm.read(notKey, txNum, tx, false);
				Constant con = rec.getVal(NOTIFICATION_FILED_NAME);
				int value = (int) con.asJavaVal();
				if (value != 1)
					throw new RuntimeException(
							"Notification value error, node no." + nodeId
									+ " sent " + value);

				if (logger.isLoggable(Level.FINE))
					logger.fine("Receive notification from node no." + nodeId);
			}
	}

	private void sendNotification() {
		RecordKey notKey = getFinishNotificationKey(VanillaDdDb.serverId());
		CachedRecord notVal = getFinishNotificationValue(txNum);

		TupleSet ts = new TupleSet(-1);
		// Use node id as source tx number
		ts.addTuple(notKey, txNum, txNum, notVal);
		VanillaDdDb.connectionMgr().pushTupleSet(0, ts);

		if (logger.isLoggable(Level.FINE))
			logger.fine("The notification is sent to the master by tx." + txNum);
	}

	private RecordKey getFinishNotificationKey(int nodeId) {
		Map<String, Constant> keyEntryMap = new HashMap<String, Constant>();
		keyEntryMap.put(NOTIFICATION_FILED_NAME, new IntegerConstant(nodeId));
		return new RecordKey("notification", keyEntryMap);
	}

	private CachedRecord getFinishNotificationValue(long txNum) {
		// Create key value sets
		Map<String, Constant> fldVals = new HashMap<String, Constant>();
		fldVals.put(NOTIFICATION_FILED_NAME, new IntegerConstant(1));

		// Create a record
		CachedRecord rec = new CachedRecord(fldVals);
		rec.setSrcTxNum(txNum);
		return rec;
	}
}
