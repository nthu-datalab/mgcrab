package netdb.software.benchmark.tpcc.procedure.vanilladddb.calvin;

import org.vanilladb.core.sql.storedprocedure.StoredProcedureParamHelper;
import org.vanilladb.dd.schedule.calvin.AllExecuteProcedure;
import org.vanilladb.dd.server.VanillaDdDb;

public class StopProfilingProc extends AllExecuteProcedure<StoredProcedureParamHelper> {

	public StopProfilingProc(long txNum) {
		super(txNum, StoredProcedureParamHelper.DefaultParamHelper());
	}

	@Override
	protected void executeSql() {
		VanillaDdDb.stopProfilerAndReport();
	}

}
