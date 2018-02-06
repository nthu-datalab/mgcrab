package netdb.software.benchmark.tpce.procedure.vanilladddb.calvin;

import org.vanilladb.core.sql.storedprocedure.StoredProcedureParamHelper;
import org.vanilladb.dd.schedule.calvin.CalvinStoredProcedure;
import org.vanilladb.dd.server.VanillaDdDb;

public class StartProfilingProc extends
		CalvinStoredProcedure<StoredProcedureParamHelper> {

	public StartProfilingProc(long txNum) {
		super(txNum, StoredProcedureParamHelper.DefaultParamHelper());
		isMaster = true;
	}

	@Override
	public void prepareKeys() {
		// Do nothing
	}

	@Override
	public boolean executeSql() {
		VanillaDdDb.initAndStartProfiler();
		return true;
	}

}
