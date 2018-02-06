package netdb.software.benchmark.tpcc.procedure.vanilladddb.oda;

import netdb.software.benchmark.tpcc.TransactionType;

import org.vanilladb.dd.schedule.oda.OdaStoredProcedure;
import org.vanilladb.dd.schedule.oda.OdaStoredProcedureFactory;

public class TpccStoredProcFactory implements OdaStoredProcedureFactory {

	@Override
	public OdaStoredProcedure getStoredProcedure(int pid, long txNum) {
		OdaStoredProcedure sp;
		switch (TransactionType.values()[pid]) {
		case NEW_ORDER:
			sp = new OdaNewOrderProc(txNum);
			break;
		case ORDER_STATUS:
			sp = new OdaOrderStatusProc(txNum);
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
			sp = null;
		}
		return sp;
	}
}
