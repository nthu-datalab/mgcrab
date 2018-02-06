package netdb.software.benchmark.tpcc.procedure;

import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.sql.storedprocedure.SpResultRecord;

public abstract class FullTableScanProcedure {
	protected boolean isCommitted;
	protected static final String TPCC_TABLES_DML[] = {
			"SELECT w_id FROM warehouse", "SELECT d_id FROM district",
			"SELECT c_id FROM customer", "SELECT h_c_id FROM history",
			"SELECT no_o_id FROM new_order", "SELECT o_id FROM orders",
			"SELECT ol_o_id FROM order_line", "SELECT i_id FROM item",
			"SELECT s_i_id FROM stock" };

	protected static final String READ_TABLES[] = { "warehouse", "district",
			"customer", "new_order", "history", "orders", "order_line",
			"stock", "item" };

	protected SpResultSet createResultSet() {
		Schema sch = new Schema();
		Type t = Type.VARCHAR(10);
		sch.addField("status", t);
		SpResultRecord rec = new SpResultRecord();
		String status = isCommitted ? "committed" : "abort";
		rec.setVal("status", new VarcharConstant(status, t));
		return new SpResultSet(sch, rec);
	}
}
