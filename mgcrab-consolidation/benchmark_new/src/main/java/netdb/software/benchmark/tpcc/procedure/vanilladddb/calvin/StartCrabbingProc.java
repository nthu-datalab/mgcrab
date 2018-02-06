package netdb.software.benchmark.tpcc.procedure.vanilladddb.calvin;

import org.vanilladb.core.sql.storedprocedure.StoredProcedureParamHelper;
import org.vanilladb.dd.schedule.calvin.AllExecuteProcedure;
import org.vanilladb.dd.server.VanillaDdDb;

public class StartCrabbingProc extends AllExecuteProcedure<StoredProcedureParamHelper> {
	
	public StartCrabbingProc(long txNum) {
		super(txNum, StoredProcedureParamHelper.DefaultParamHelper());
		VanillaDdDb.migrationMgr().startCrabbingPhase();
	}

	@Override
	protected void executeSql() {
		// Do nothing
	}
}
