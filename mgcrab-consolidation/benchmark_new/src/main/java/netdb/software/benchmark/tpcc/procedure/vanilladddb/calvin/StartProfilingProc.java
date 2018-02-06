package netdb.software.benchmark.tpcc.procedure.vanilladddb.calvin;

import org.vanilladb.core.sql.storedprocedure.StoredProcedureParamHelper;
import org.vanilladb.dd.schedule.calvin.AllExecuteProcedure;
import org.vanilladb.dd.server.VanillaDdDb;

public class StartProfilingProc extends AllExecuteProcedure<StoredProcedureParamHelper> {

	public StartProfilingProc(long txNum) {
		super(txNum, StoredProcedureParamHelper.DefaultParamHelper());
	}

	@Override
	protected void executeSql() {
		VanillaDdDb.initAndStartProfiler();
	}
}
