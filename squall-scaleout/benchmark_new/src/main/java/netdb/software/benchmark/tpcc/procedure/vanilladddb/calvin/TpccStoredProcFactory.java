package netdb.software.benchmark.tpcc.procedure.vanilladddb.calvin;

import netdb.software.benchmark.tpcc.TransactionType;

import org.vanilladb.dd.schedule.calvin.CalvinStoredProcedure;
import org.vanilladb.dd.schedule.calvin.CalvinStoredProcedureFactory;

public class TpccStoredProcFactory implements CalvinStoredProcedureFactory {

	@Override
	public CalvinStoredProcedure<?> getStoredProcedure(int pid, long txNum) {
		CalvinStoredProcedure<?> sp;
		switch (TransactionType.values()[pid]) {
		case NEW_ORDER:
			sp = new NewOrderProc(txNum);
			break;
		// case ORDER_STATUS:
		// sp = new OdaOrderStatusProc(txNum);
		// break;
		// case FULL_TABLE_SCAN:
		// sp = new FullTableScanProc(txNum);
		// break;
		case SCHEMA_BUILDER:
			sp = new SchemaBuilderProc(txNum);
			break;
		case TESTBED_LOADER:
			sp = new TestbedLoaderProc(txNum);
			break;
		case MICRO_BENCHMARK:
			sp = new MicroBenchmarkProc(txNum);
			break;
		case START_PROFILING:
			sp = new StartProfilingProc(txNum);
			break;
		case STOP_PROFILING:
			sp = new StopProfilingProc(txNum);
			break;
		case START_MIGRATION:
			sp = new StartMigrationProc(txNum);
			break;
		case STOP_MIGRATION:
			sp = new StopMigrationProc(txNum);
			break;
		case ASYNC_MIGRATE:
			sp = new AsyncMigrateProc(txNum);
			break;
		case MIGRATION_ANALYSIS:
			sp = new MigrationAnalysisProc(txNum);
			break;
		case PAYMENT:
			sp = new PaymentProc(txNum);
			break;
		case YCSB:
			sp = new YcsbBenchmarkProc(txNum);
			break;
		default:
			sp = null;
		}
		return sp;
	}
}
