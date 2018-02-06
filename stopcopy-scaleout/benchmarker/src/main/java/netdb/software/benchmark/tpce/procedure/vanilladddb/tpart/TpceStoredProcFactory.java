package netdb.software.benchmark.tpce.procedure.vanilladddb.tpart;

import netdb.software.benchmark.tpce.TransactionType;

import org.vanilladb.dd.schedule.tpart.TPartStoredProcedure;
import org.vanilladb.dd.schedule.tpart.TPartStoredProcedureFactory;

public class TpceStoredProcFactory extends TPartStoredProcedureFactory {

	@Override
	public TPartStoredProcedure getStoredProcedure(int pid, long txNum) {
		TPartStoredProcedure sp;
		TransactionType type = TransactionType.values()[pid];
		switch (type) {
		case SCHEMA_BUILDER:
			sp = new SchemaBuilderProc(txNum);
			break;
		case TESTBED_LOADER:
			sp = new TestbedLoaderProc(txNum);
			break;
		case TRADE_ORDER:
			sp = new NewTradeOrderProc(txNum);
			break;
		case TRADE_RESULT:
			sp = new NewTradeResultProc(txNum);
			break;
		case START_PROFILING:
			sp = new StartProfilingProc(txNum);
			break;
		case STOP_PROFILING:
			sp = new StopProfilingProc(txNum);
			break;
		default:
			throw new UnsupportedOperationException();
		}
		return sp;
	}
}