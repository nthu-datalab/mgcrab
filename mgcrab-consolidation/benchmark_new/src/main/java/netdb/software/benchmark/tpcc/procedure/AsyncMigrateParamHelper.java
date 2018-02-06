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
	private RecordKey[] storingKeys;
	
	@Override
	public void prepareParameters(Object... pars) {
		int pushingCount = (Integer) pars[0];
		int storingCount = (Integer) pars[pushingCount + 1];
		
		// Read pushing keys
		pushingKeys = new RecordKey[pushingCount];
		for (int i = 0; i < pushingCount; i++)
			pushingKeys[i] = (RecordKey) pars[i + 1];
		
		// Read storing keys
		storingKeys = new RecordKey[storingCount];
		for (int i = 0; i < storingCount; i++)
			storingKeys[i] = (RecordKey) pars[i + 2 + pushingCount];
	}
	
	public RecordKey[] getPushingKeys() {
		return pushingKeys;
	}
	
	public RecordKey[] getStoringKeys() {
		return storingKeys;
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
