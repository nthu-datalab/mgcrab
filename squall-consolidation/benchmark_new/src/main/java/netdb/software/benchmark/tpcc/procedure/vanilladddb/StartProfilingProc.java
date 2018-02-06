package netdb.software.benchmark.tpcc.procedure.vanilladddb;

import netdb.software.benchmark.tpcc.procedure.StartProfilingProcedure;

import org.vanilladb.dd.schedule.DdStoredProcedure;

public class StartProfilingProc extends StartProfilingProcedure implements
		DdStoredProcedure {
	public StartProfilingProc(long txNum) {
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
