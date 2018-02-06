package netdb.software.benchmark.tpce.procedure;

import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.sql.storedprocedure.SpResultRecord;
import org.vanilladb.core.sql.storedprocedure.StoredProcedureParamHelper;

public class SchemaBuilderProcParamHelper extends StoredProcedureParamHelper {
	
	protected final String TPCE_TABLES_DDL[] = {
			"CREATE TABLE customer ( c_id LONG, c_name VARCHAR(46) )",
			"CREATE TABLE customer_account ( ca_id LONG, ca_b_id LONG, ca_c_id LONG, ca_name VARCHAR(50), ca_bal DOUBLE )",
			"CREATE TABLE holding ( h_t_id LONG, h_ca_id LONG, h_s_symb VARCHAR(15), h_dts LONG, h_price DOUBLE, h_qty INT )",
			"CREATE TABLE holding_history ( hh_h_t_id LONG, hh_t_id LONG, hh_before_qty INT, hh_after_qty INT )",
			"CREATE TABLE broker ( b_id LONG, b_name VARCHAR(49), b_num_trades INT )",
			"CREATE TABLE trade ( t_id LONG, t_dts LONG, t_tt_id VARCHAR(3), t_s_symb VARCHAR(15), t_qty INT, t_bid_price DOUBLE, t_ca_id LONG, t_trade_price DOUBLE )",
			"CREATE TABLE trade_history ( th_t_id LONG, th_dts LONG )",
			"CREATE TABLE trade_type ( tt_id VARCHAR(3), tt_name VARCHAR(12), tt_is_sell INT, tt_is_mrkt INT )",
			"CREATE TABLE company ( co_id LONG, co_name VARCHAR(60), co_ceo VARCHAR(46) )",
			"CREATE TABLE last_trade ( lt_s_symb VARCHAR(15), lt_dts LONG, lt_price DOUBLE, lt_open_price DOUBLE, lt_vol INT )",
			"CREATE TABLE security ( s_symb VARCHAR(15), s_name VARCHAR(70), s_co_id LONG )" };

	protected final String TPCE_INDEXES_DDL[] = {
			"CREATE INDEX idx_customer_id ON customer (c_id)",
			"CREATE INDEX idx_customer_account_id ON customer_account (ca_id)",
			"CREATE INDEX idx_broker_id ON broker (b_id)",
			"CREATE INDEX idx_security_symbol ON security (s_symb)",
			"CREATE INDEX idx_last_trade_security_symbol ON last_trade (lt_s_symb)",
			"CREATE INDEX idx_trade_type_id ON trade_type (tt_id)" };

	protected final String TABLES[] = { "customer", "customer_account",
			"holding", "holding_history", "broker", "trade", "trade_history",
			"trade_type", "company", "last_trade", "security" };

	public String[] getTableSchemas() {
		return TPCE_TABLES_DDL;
	}

	public String[] getIndexSchemas() {
		return TPCE_INDEXES_DDL;
	}
	
	public String[] getTableNames() {
		return TABLES;
	}
	
	@Override
	public void prepareParameters(Object... pars) {
		// nothing to do
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
}
