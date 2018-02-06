package netdb.software.benchmark.tpcc.procedure.vanilladb;

import netdb.software.benchmark.tpcc.procedure.StopProfilingProcedure;

import org.vanilladb.core.sql.storedprocedure.StoredProcedure;

public class StopProfilingProc extends StopProfilingProcedure implements
		StoredProcedure {

	@Override
	public void prepare(Object... pars) {
		// do nothing
	}
}
