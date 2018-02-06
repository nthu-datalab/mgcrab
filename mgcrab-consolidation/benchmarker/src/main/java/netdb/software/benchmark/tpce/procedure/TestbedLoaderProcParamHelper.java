package netdb.software.benchmark.tpce.procedure;

import java.util.HashMap;
import java.util.Map;

import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.sql.storedprocedure.SpResultRecord;
import org.vanilladb.core.sql.storedprocedure.StoredProcedureParamHelper;
import org.vanilladb.dd.cache.CachedRecord;
import org.vanilladb.dd.sql.RecordKey;

public class TestbedLoaderProcParamHelper extends StoredProcedureParamHelper {

	@Override
	public void prepareParameters(Object... pars) {
		// Do nothing
	}

	@Override
	public SpResultSet createResultSet() {
		// create schema
		Schema sch = new Schema();
		Type statusType = Type.VARCHAR(10);
		sch.addField("status", statusType);

		// create record
		SpResultRecord rec = new SpResultRecord();
		String status = isCommitted ? "committed" : "abort";
		rec.setVal("status", new VarcharConstant(status, statusType));

		// create result set
		return new SpResultSet(sch, rec);
	}

	public String[] getTables() {
		String[] tables = { "customer", "customer_account", "holding",
				"holding_history", "broker", "trade", "trade_history",
				"trade_type", "company", "last_trade", "security" };
		return tables;
	}

	public RecordKey getFinishNotificationKey(int nodeId) {
		Map<String, Constant> keyEntryMap = new HashMap<String, Constant>();
		keyEntryMap.put("finish", new IntegerConstant(nodeId));
		return new RecordKey("notification", keyEntryMap);
	}

	public CachedRecord getFinishNotificationValue(long txNum) {
		// Create key value sets
		Map<String, Constant> fldVals = new HashMap<String, Constant>();
		fldVals.put("finish", new IntegerConstant(1));

		// Create a record
		CachedRecord rec = new CachedRecord(fldVals);
		rec.setSrcTxNum(txNum);
		return rec;
	}

	public String getFinishFieldName() {
		return "finish";
	}

	public int getMasterNodeId() {
		return 0;
	}
}
