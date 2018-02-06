package netdb.software.benchmark.tpcc.procedure.vanilladddb.calvin;

import org.vanilladb.core.sql.storedprocedure.StoredProcedureParamHelper;
import org.vanilladb.dd.schedule.calvin.AllExecuteProcedure;
import org.vanilladb.dd.server.VanillaDdDb;

public class StartMigrationProc extends AllExecuteProcedure<StoredProcedureParamHelper> {
	
	public StartMigrationProc(long txNum) {
		super(txNum, StoredProcedureParamHelper.DefaultParamHelper());
		VanillaDdDb.migrationMgr().startMigration();
	}

	@Override
	protected void executeSql() {
		System.out.println("Migration starts with tx number: " + txNum);
		// Last server node starts async-pushing transactions
//		if(VanillaDdDb.serverId() == VanillaDdDb.migrationMgr().getDestPartition()){
//			TupleSet ts = new TupleSet(MigrationManager.SINK_ID_ANALYSIS);
//			VanillaDdDb.connectionMgr().pushTupleSet(ConnectionMgr.SEQ_NODE_ID, ts);
//		}
	}
}
