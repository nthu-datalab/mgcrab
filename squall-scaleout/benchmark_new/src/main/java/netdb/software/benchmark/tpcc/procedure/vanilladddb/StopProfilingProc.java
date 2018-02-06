package netdb.software.benchmark.tpcc.procedure.vanilladddb;

import netdb.software.benchmark.tpcc.procedure.StopProfilingProcedure;

import org.vanilladb.dd.schedule.DdStoredProcedure;

public class StopProfilingProc extends StopProfilingProcedure implements
		DdStoredProcedure {

	public StopProfilingProc(long txNum) {
	}

	@Override
	public void prepare(Object... pars) {
		// do nothing
	}

	@Override
	public void requestConservativeLocks() {
		// do nothing
	}
}
