package netdb.software.benchmark.tpcc.remote.vanilladddb;

import netdb.software.benchmark.tpcc.remote.SutResultSet;

import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.sql.Record;
import org.vanilladb.core.sql.Schema;

public class VanillaDdDbResultSet implements SutResultSet {
	private Record[] recs;
	private Schema sch;

	public VanillaDdDbResultSet(SpResultSet result) {
		recs = result.getRecords();
		sch = result.getSchema();
	}
	
	@Override
	public int getSender() {
		if (!sch.hasField("sender"))
			return -1;
		int nodeId = (int) recs[0].getVal("sender").asJavaVal();
		return nodeId;
	}
	
	@Override
	public boolean isCommitted() {
		if (!sch.hasField("status"))
			throw new RuntimeException("result set not completed");
		String status = (String) recs[0].getVal("status").asJavaVal();
		return status.equals("committed");
	}
	
	@Override
	public String outputMsg() {
		return recs[0].toString();
	}
}
