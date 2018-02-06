package netdb.software.benchmark.tpcc.procedure.mysql;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import netdb.software.benchmark.tpcc.procedure.StockLevelProcedure;
import netdb.software.benchmark.tpcc.util.MysqlService;

import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.storedprocedure.StoredProcedure;

public class MysqlStockLevelProc extends StockLevelProcedure implements
		StoredProcedure {

	Connection conn;

	public MysqlStockLevelProc() {
	}

	@Override
	public void prepare(Object... pars) {
		prepareParameters(pars);
	}

	@Override
	public SpResultSet execute() {
		this.tx = VanillaDb.txMgr().transaction(
				Connection.TRANSACTION_SERIALIZABLE, true);
		try {
			executeMysql();
			tx.commit();
		} catch (Exception e) {
			tx.rollback();
			isCommitted = false;
			e.printStackTrace();
		} finally {
			MysqlService.disconnect(conn);
		}
		return createResultSet();
	}

	protected void executeMysql() throws SQLException {

		conn = MysqlService.connect();
		Statement stm = MysqlService.createStatement(conn);
		ResultSet rs = null;
		conn.setAutoCommit(false);

		int dNextOid = 0;
		String sql = "SELECT d_next_o_id FROM district WHERE d_w_id = " + wid
				+ " AND d_id = " + did;
		rs = MysqlService.executeQuery(sql, stm);
		rs.beforeFirst();
		if (rs.next()) {
			dNextOid = rs.getInt("d_next_o_id");
		} else {
			throw new RuntimeException();
		}
		rs.close();

		sql = "SELECT COUNT(DISTINCT s_i_id) as \"dstcountofs_i_id\" FROM order_line, stock "
				+ "WHERE ol_w_id = "
				+ wid
				+ " AND ol_d_id = "
				+ did
				+ " AND ol_o_id < "
				+ dNextOid
				+ " AND ol_o_id >= "
				+ (dNextOid - 20)
				+ " AND s_w_id = "
				+ wid
				+ " AND s_i_id = ol_i_id AND s_quantity < " + threshold;
		rs = MysqlService.executeQuery(sql, stm);
		rs.beforeFirst();
		boolean isSelected = false;
		while (rs.next()) {
			lowStockNum = rs.getInt("dstcountofs_i_id");
			isSelected = true;
		}
		rs.close();

		if (!isSelected)
			throw new RuntimeException();

		conn.commit();
	}
}
