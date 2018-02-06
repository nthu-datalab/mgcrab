package netdb.software.benchmark.tpcc.procedure.mysql;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import netdb.software.benchmark.tpcc.procedure.MicroBenchmarkProcedure;
import netdb.software.benchmark.tpcc.util.MysqlService;

import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.storedprocedure.StoredProcedure;

public class MysqlMicroBenchmarkProc extends MicroBenchmarkProcedure implements
		StoredProcedure {

	Connection conn;

	public MysqlMicroBenchmarkProc() {

	}

	@Override
	public void prepare(Object... pars) {
		prepareParameters(pars);
	}

	@Override
	public SpResultSet execute() {
		tx = VanillaDb.txMgr().transaction(Connection.TRANSACTION_SERIALIZABLE,
				false);

		conn = MysqlService.connect();
		try {
			executeSql();
			tx.commit();
		} catch (Exception e) {
			tx.rollback();
			isCommitted = false;
			e.printStackTrace();
		}

		MysqlService.disconnect(conn);

		return createResultSet();
	}

	private void executeSql() {

		Statement stm = MysqlService.createStatement(conn);
		ResultSet rs = null;
		try {
			conn.setAutoCommit(false);
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		StringBuffer sb = new StringBuffer(
				"SELECT i_name, i_price FROM item WHERE i_id IN (");
		for (int i = 0; i < readCount; i++) {
			sb.append(readItemId[i] + ",");
		}
		sb.replace(sb.length() - 1, sb.length(), "");
		sb.append(")");

		rs = MysqlService.executeQuery(sb.toString(), stm);
		String name;
		double price;
		try {
			rs.beforeFirst();
			while (rs.next()) {
				name = rs.getString("i_name");
				price = rs.getDouble("i_price");
			}
			rs.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// update the items

		if (writeCount != 0) {

			sb = new StringBuffer("UPDATE item SET i_price = "
					+ newItemPrice[0] + " WHERE i_id IN (");

			for (int i = 0; i < writeCount; i++) {
				sb.append(writeItemId[i] + ",");
			}
			sb.replace(sb.length() - 1, sb.length(), "");
			sb.append(")");

			MysqlService.executeUpdateQuery(sb.toString(), stm);
			;
		}

		// for (int i = 0; i < writeCount; i++) {
		// String sql = "UPDATE item SET i_price = " + newItemPrice[i]
		// + " WHERE i_id = " + writeItemId[i];
		// MysqlService.executeUpdateQuery(sql, stm);
		//
		// }

		try {
			conn.commit();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
