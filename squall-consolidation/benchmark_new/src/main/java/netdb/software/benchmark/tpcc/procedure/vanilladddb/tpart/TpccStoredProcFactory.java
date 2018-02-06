package netdb.software.benchmark.tpcc.procedure.vanilladddb.tpart;

import netdb.software.benchmark.tpcc.TransactionType;

import org.vanilladb.dd.schedule.tpart.TPartStoredProcedure;
import org.vanilladb.dd.schedule.tpart.TPartStoredProcedureFactory;

public class TpccStoredProcFactory extends TPartStoredProcedureFactory {

	@Override
	public TPartStoredProcedure getStoredProcedure(int pid, long txNum) {
		TPartStoredProcedure sp;
		switch (TransactionType.values()[pid]) {
		case NEW_ORDER:
			sp = new TPartNewOrderProc(txNum);
			break;
		// case FULL_TABLE_SCAN:
		// sp = new FullTableScanProc(txNum);
		// break;
		// case SCHEMA_BUILDER:
		// sp = new SchemaBuilderProc(txNum);
		// break;
		// case TESTBED_LOADER:
		// sp = new TestbedLoaderProc(txNum);
		// break;
		case MICRO_BENCHMARK:
			sp = new MicroBenchmarkProc(txNum);
			break;
		case START_PROFILING:
			sp = new StartProfilingProc(txNum);
			break;
		case STOP_PROFILING:
			sp = new StopProfilingProc(txNum);
			break;
		default:
			sp = super.getStoredProcedure(pid, txNum);
		}
		return sp;
	}
}
