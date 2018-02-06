package netdb.software.benchmark.tpcc.procedure.vanilladddb.oda;

import org.vanilladb.core.sql.storedprocedure.StoredProcedureParamHelper;
import org.vanilladb.dd.schedule.oda.OdaStoredProcedure;
import org.vanilladb.dd.server.VanillaDdDb;

public class StartProfilingProc extends OdaStoredProcedure {

	public StartProfilingProc(long txNum) {
		this.txNum = txNum;
		this.paramHelper = StoredProcedureParamHelper.DefaultParamHelper();
	}

	@Override
	public void prepareKeys() {

	}

	@Override
	public void executeSql() {
		VanillaDdDb.initAndStartProfiler();
	}

}
