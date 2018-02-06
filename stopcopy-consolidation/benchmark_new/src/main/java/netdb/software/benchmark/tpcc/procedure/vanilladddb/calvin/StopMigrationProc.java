package netdb.software.benchmark.tpcc.procedure.vanilladddb.calvin;

import org.vanilladb.core.sql.storedprocedure.StoredProcedureParamHelper;
import org.vanilladb.dd.schedule.calvin.AllExecuteProcedure;
import org.vanilladb.dd.server.VanillaDdDb;

public class StopMigrationProc extends AllExecuteProcedure<StoredProcedureParamHelper> {

	public StopMigrationProc(long txNum) {
		super(txNum, StoredProcedureParamHelper.DefaultParamHelper());
		VanillaDdDb.migrationMgr().stopMigration();
	}

	@Override
	protected void executeSql() {
		System.out.println("Migration stops with tx number: " + txNum);
	}
}
