package netdb.software.benchmark.tpcc.procedure;

import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.sql.storedprocedure.SpResultRecord;
import org.vanilladb.core.storage.tx.Transaction;

public abstract class StockLevelProcedure {
	protected Transaction tx;

	protected int wid, did, threshold, lowStockNum;
	protected boolean isCommitted = true;

	protected void prepareParameters(Object... pars) {
		if (pars.length != 3)
			throw new RuntimeException("wrong pars list");
		wid = (Integer) pars[0];
		did = (Integer) pars[1];
		threshold = (Integer) pars[2];
	}

	protected void executeSql() {
		int dNextOid = 0;
		String sql = "SELECT d_next_o_id FROM district WHERE d_w_id = " + wid
				+ " AND d_id = " + did;
		Plan p = VanillaDb.planner().createQueryPlan(sql, tx);

		Scan s = p.open();
		s.beforeFirst();
		if (s.next()) {
			dNextOid = (Integer) s.getVal("d_next_o_id").asJavaVal();
		} else {
			throw new RuntimeException();
		}
		s.close();

		sql = "SELECT COUNT(DISTINCT s_i_id) FROM order_line, stock "
				+ "WHERE ol_w_id = " + wid + " AND ol_d_id = " + did
				+ " AND ol_o_id < " + dNextOid + " AND ol_o_id >= "
				+ (dNextOid - 20) + " AND s_w_id = " + wid
				+ " AND s_i_id = ol_i_id AND s_quantity < " + threshold;
		p = VanillaDb.planner().createQueryPlan(sql, tx);

		s = p.open();
		s.beforeFirst();
		boolean isSelected = false;
		while (s.next()) {
			lowStockNum = (Integer) s.getVal("dstcountofs_i_id").asJavaVal();
			isSelected = true;
		}
		s.close();

		if (!isSelected)
			throw new RuntimeException();
	}

	protected SpResultSet createResultSet() {
		Schema sch = new Schema();
		Type statusType = Type.VARCHAR(10);
		sch.addField("status", statusType);
		sch.addField("wid", Type.INTEGER);
		sch.addField("did", Type.INTEGER);
		sch.addField("threshold", Type.INTEGER);
		sch.addField("low_stock", Type.INTEGER);

		SpResultRecord rec = new SpResultRecord();
		String status = isCommitted ? "committed" : "abort";
		rec.setVal("status", new VarcharConstant(status, statusType));
		rec.setVal("wid", new IntegerConstant(wid));
		rec.setVal("did", new IntegerConstant(did));
		rec.setVal("threshold", new IntegerConstant(threshold));
		rec.setVal("low_stock", new IntegerConstant(lowStockNum));

		return new SpResultSet(sch, rec);
	}
}
