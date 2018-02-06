package netdb.software.benchmark.tpce.procedure;

import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.sql.storedprocedure.SpResultRecord;
import org.vanilladb.core.storage.tx.Transaction;

public class CustomerPositionProcedure {

	protected Transaction tx;
	protected long customerId;
	protected boolean needHistory;
	protected boolean isCommitted = true;

	public void prepareParameters(Object... pars) {
		if (pars.length != 2)
			throw new RuntimeException("wrong pars list");
		customerId = (Long) pars[0];
		needHistory = (Boolean) pars[1];
	}

	public SpResultSet createResultSet() {
		Schema sch = new Schema();
		Type statusType = Type.VARCHAR(10);
		sch.addField("status", statusType);

		SpResultRecord rec = new SpResultRecord();
		String status = isCommitted ? "committed" : "abort";
		rec.setVal("status", new VarcharConstant(status, statusType));

		return new SpResultSet(sch, rec);
	}
}
