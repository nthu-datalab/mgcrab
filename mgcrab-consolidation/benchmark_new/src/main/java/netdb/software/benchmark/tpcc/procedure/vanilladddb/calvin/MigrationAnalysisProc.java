package netdb.software.benchmark.tpcc.procedure.vanilladddb.calvin;

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.vanilladb.core.sql.storedprocedure.StoredProcedureParamHelper;
import org.vanilladb.dd.cache.CachedRecord;
import org.vanilladb.dd.remote.groupcomm.TupleSet;
import org.vanilladb.dd.remote.groupcomm.server.ConnectionMgr;
import org.vanilladb.dd.schedule.calvin.CalvinStoredProcedure;
import org.vanilladb.dd.server.VanillaDdDb;
import org.vanilladb.dd.server.migration.MigrationManager;
import org.vanilladb.dd.sql.RecordKey;

public class MigrationAnalysisProc extends CalvinStoredProcedure<StoredProcedureParamHelper> {
	private static Logger logger = Logger.getLogger(MigrationAnalysisProc.class.getName());
	
	private int sourceNode = VanillaDdDb.migrationMgr().getSourcePartition();
	
	public MigrationAnalysisProc(long txNum) {
		super(txNum, StoredProcedureParamHelper.DefaultParamHelper());
		VanillaDdDb.migrationMgr().startAnalysis();
	}

	@Override
	public void prepareKeys() {
		VanillaDdDb.migrationMgr().prepareAnalysis();
	}

	@Override
	protected int decideMaster() {
		return sourceNode;
	}

	@Override
	protected void onLocalReadCollected(Map<RecordKey, CachedRecord> localReadings) {
	}

	@Override
	protected void onRemoteReadCollected(Map<RecordKey, CachedRecord> remoteReadings) {
	}

	@Override
	protected void writeRecords(Map<RecordKey, CachedRecord> remoteReadings) {
	}

	@Override
	protected void masterCollectResults(Map<RecordKey, CachedRecord> readings) {
	}
	
	@Override
	protected void executeTransactionLogic() {
		if (VanillaDdDb.serverId() == sourceNode) {
			if (logger.isLoggable(Level.INFO))
				logger.info("Start analysis phase for migration...");
			
			// Generate data set
			Map<RecordKey, Boolean> dataSet = 
					VanillaDdDb.migrationMgr().generateDataSetForMigration();
			
			if (logger.isLoggable(Level.INFO))
				logger.info("Estimated size of target data set for migration: "
						+ dataSet.size());
			
			// Send the target data set to the migration manager
			VanillaDdDb.migrationMgr().analysisComplete(dataSet);
			
			if (logger.isLoggable(Level.INFO))
				logger.info("Finish analysis.");
			
			TupleSet ts = new TupleSet(MigrationManager.SINK_ID_START_MIGRATION);
			VanillaDdDb.connectionMgr().pushTupleSet(ConnectionMgr.SEQ_NODE_ID, ts);
			
			if (logger.isLoggable(Level.INFO))
				logger.info("Sent the migration starting request.");
		}
	}
}
