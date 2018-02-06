package netdb.software.benchmark.tpcc.procedure;

import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.sql.storedprocedure.SpResultRecord;
import org.vanilladb.core.sql.storedprocedure.StoredProcedureParamHelper;
import org.vanilladb.dd.sql.RecordKey;

public class AsyncMigrateParamHelper extends StoredProcedureParamHelper {
	
	private RecordKey[] pushingKeys;
	
	@Override
	public void prepareParameters(Object... pars) {
		pushingKeys = (RecordKey[]) pars;
	}
	
	public RecordKey[] getPushingKeys() {
		return pushingKeys;
	}
	
	@Override
	public SpResultSet createResultSet() {
		// Return the result
		Schema sch = new Schema();
		Type t = Type.VARCHAR(10);
		sch.addField("status", t);
		SpResultRecord rec = new SpResultRecord();
		String status = isCommitted ? "committed" : "abort";
		rec.setVal("status", new VarcharConstant(status, t));
		return new SpResultSet(sch, rec);
	}
}
