package netdb.software.benchmark.tpcc.procedure.vanilladb;

import netdb.software.benchmark.tpcc.procedure.StartProfilingProcedure;

import org.vanilladb.core.sql.storedprocedure.StoredProcedure;

public class StartProfilingProc extends StartProfilingProcedure implements
		StoredProcedure {

	@Override
	public void prepare(Object... pars) {
		// do nothing
	}
}
