package netdb.software.benchmark.tpcc.procedure.vanilladddb.tpart;

import netdb.software.benchmark.tpcc.procedure.StopProfilingProcedure;

import org.vanilladb.dd.schedule.tpart.TPartStoredProcedure;
import org.vanilladb.dd.schedule.tpart.sink.SunkPlan;
import org.vanilladb.dd.server.VanillaDdDb;
import org.vanilladb.dd.sql.RecordKey;

public class StopProfilingProc extends StopProfilingProcedure implements
		TPartStoredProcedure {

	public StopProfilingProc(long txNum) {
	}

	@Override
	public void prepare(Object... pars) {
		// do nothing
	}

	@Override
	public RecordKey[] getReadSet() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RecordKey[] getWriteSet() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setSunkPlan(SunkPlan plan) {
		// TODO Auto-generated method stub

	}

	@Override
	public double getWeight() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getProcedureType() {
		// TODO Auto-generated method stub
		return TPartStoredProcedure.PROFILE;
	}

	@Override
	public boolean isReadOnly() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isMaster() {
		// TODO Auto-generated method stub
		return true;
	}

	@Override
	public void requestConservativeLocks() {
		// TODO Auto-generated method stub

	}
}
