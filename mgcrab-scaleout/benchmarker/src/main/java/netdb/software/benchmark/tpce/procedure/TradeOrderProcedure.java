package netdb.software.benchmark.tpce.procedure;

import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.sql.BigIntConstant;
import org.vanilladb.core.sql.DoubleConstant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.sql.storedprocedure.SpResultRecord;
import org.vanilladb.core.storage.tx.Transaction;

public class TradeOrderProcedure {
	protected Transaction tx;
	protected long acctId;
	protected int tradeQty;
	protected String tradeTypeId;
	protected boolean rollback;
	protected double requestedPrice;
	protected String coName, sSymb;
	protected boolean isCommitted = true;

	protected long customerId;
	protected String customerName;
	protected long brokerId;
	protected double marketPrice;
	protected double tradePrice;
	protected long tradeId;

	public void prepareParameters(Object... pars) {
		if (pars.length != 10)
			throw new RuntimeException("wrong pars list");
		acctId = (Long) pars[0];
		customerId = (Long) pars[1];
		brokerId = (Long) pars[2];
		coName = (String) pars[3];
		requestedPrice = (Double) pars[4];
		rollback = (Boolean) pars[5];
		sSymb = (String) pars[6];
		tradeQty = (Integer) pars[7];
		tradeTypeId = (String) pars[8];
		tradeId = (Long) pars[9];
	}

	public SpResultSet createResultSet() {
		Schema sch = new Schema();
		Type statusType = Type.VARCHAR(10);
		Type nameType = Type.VARCHAR(46);
		Type companyNameType = Type.VARCHAR(60);
		Type securitySymbolType = Type.VARCHAR(15);
		Type longType = Type.BIGINT;
		Type doubleType = Type.DOUBLE;

		sch.addField("c_id", longType);
		sch.addField("c_name", nameType);
		sch.addField("ac_id", longType);
		sch.addField("b_id", longType);
		sch.addField("co_name", companyNameType);
		sch.addField("s_symbol", securitySymbolType);
		sch.addField("mrkt_price", doubleType);
		sch.addField("t_id", longType);
		sch.addField("trade_price", doubleType);
		sch.addField("status", Type.VARCHAR(10));

		SpResultRecord rec = new SpResultRecord();
		String status = isCommitted ? "committed" : "abort";
		rec.setVal("c_id", new BigIntConstant(customerId));
		rec.setVal("c_name", new VarcharConstant(customerName));
		rec.setVal("ac_id", new BigIntConstant(acctId));
		rec.setVal("b_id", new BigIntConstant(brokerId));
		rec.setVal("co_name", new VarcharConstant(coName));
		rec.setVal("s_symbol", new VarcharConstant(sSymb));
		rec.setVal("mrkt_price", new DoubleConstant(marketPrice));
		rec.setVal("trade_price", new DoubleConstant(tradePrice));
		rec.setVal("t_id", new BigIntConstant(tradeId));
		rec.setVal("status", new VarcharConstant(status, statusType));

		return new SpResultSet(sch, rec);
	}
}
