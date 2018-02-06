package netdb.software.benchmark.tpcc.procedure.vanilladddb.calvin;

import org.vanilladb.core.sql.storedprocedure.StoredProcedureParamHelper;
import org.vanilladb.dd.schedule.calvin.AllExecuteProcedure;
import org.vanilladb.dd.server.VanillaDdDb;

public class StartCatchUpProc extends AllExecuteProcedure<StoredProcedureParamHelper> {
	
	public StartCatchUpProc(long txNum) {
		super(txNum, StoredProcedureParamHelper.DefaultParamHelper());
		VanillaDdDb.migrationMgr().startCaughtUpPhase();
	}

	@Override
	protected void executeSql() {
		// Do nothing
	}
}
