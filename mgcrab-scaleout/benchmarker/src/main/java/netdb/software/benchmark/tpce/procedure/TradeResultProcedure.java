package netdb.software.benchmark.tpce.procedure;

import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.sql.BigIntConstant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.sql.storedprocedure.SpResultRecord;
import org.vanilladb.core.storage.tx.Transaction;

public class TradeResultProcedure {
	protected Transaction tx;

	protected long customerId;
	protected long acctId;
	protected long brokerId;
	protected long tradeId;
	protected boolean isCommitted = true;

	protected String customerName;

	public void prepareParameters(Object... pars) {
		if (pars.length != 4)
			throw new RuntimeException("wrong pars list");
		int idxCntr = 0;
		tradeId = (Long) pars[idxCntr++];
		customerId = (Long) pars[idxCntr++];
		acctId = (Long) pars[idxCntr++];
		brokerId = (Long) pars[idxCntr++];
	}

	public SpResultSet createResultSet() {
		Schema sch = new Schema();
		Type statusType = Type.VARCHAR(10);
		Type longType = Type.BIGINT;

		sch.addField("c_id", longType);
		sch.addField("ac_id", longType);
		sch.addField("b_id", longType);
		sch.addField("t_id", longType);
		sch.addField("status", Type.VARCHAR(10));

		SpResultRecord rec = new SpResultRecord();
		String status = isCommitted ? "committed" : "abort";
		rec.setVal("c_id", new BigIntConstant(customerId));
		rec.setVal("c_name", new VarcharConstant(customerName));
		rec.setVal("ac_id", new BigIntConstant(acctId));
		rec.setVal("b_id", new BigIntConstant(brokerId));
		rec.setVal("t_id", new BigIntConstant(tradeId));
		rec.setVal("status", new VarcharConstant(status, statusType));

		return new SpResultSet(sch, rec);
	}
}
