package netdb.software.benchmark.tpcc.procedure.mysql;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import netdb.software.benchmark.tpcc.procedure.PaymentProcedure;
import netdb.software.benchmark.tpcc.util.DoublePlainPrinter;
import netdb.software.benchmark.tpcc.util.MysqlService;

import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.sql.storedprocedure.SpResultRecord;
import org.vanilladb.core.sql.storedprocedure.StoredProcedure;

public class MysqlPaymentProc extends PaymentProcedure implements
		StoredProcedure {

	Connection conn;

	public MysqlPaymentProc() {

	}

	@Override
	public void prepare(Object... pars) {
		prepareParameters(pars);
	}

	@Override
	public SpResultSet execute() {
		this.tx = VanillaDb.txMgr().transaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		try {
			if (selectByCLast)
				executeMysqlByCLast();
			else
				executeMysqlByCid();
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

	protected void executeMysqlByCLast() throws SQLException {

		conn = MysqlService.connect();
		Statement stm = MysqlService.createStatement(conn);
		ResultSet rs = null;
		conn.setAutoCommit(false);

		String wName, wStreet1, wStreet2, wCity, wState, wZip;
		double wYtd;
		String sql = "SELECT w_name, w_street_1, w_street_2, w_city, "
				+ "w_state, w_zip, w_ytd FROM warehouse WHERE w_id =" + wid;
		rs = MysqlService.executeQuery(sql, stm);
		rs.beforeFirst();
		if (rs.next()) {
			wName = rs.getString("w_name");
			wStreet1 = rs.getString("w_street_1");
			wStreet2 = rs.getString("w_street_2");
			wCity = rs.getString("w_city");
			wState = rs.getString("w_state");
			wZip = rs.getString("w_zip");
			wYtd = rs.getDouble("w_ytd");
		} else
			throw new RuntimeException();
		rs.close();

		sql = "UPDATE warehouse SET w_ytd =  "
				+ DoublePlainPrinter.toPlainString(wYtd + hAmount)
				+ " WHERE w_id =" + wid;
		MysqlService.executeUpdateQuery(sql, stm);

		String dName, dStreet1, dStreet2, dCity, dState, dZip;
		double dYtd;
		sql = "SELECT d_name, d_street_1, d_street_2, d_city, d_state, d_zip,"
				+ "d_ytd FROM district WHERE d_w_id = " + wid + " AND d_id = "
				+ did;
		rs = MysqlService.executeQuery(sql, stm);
		rs.beforeFirst();
		if (rs.next()) {
			dName = rs.getString("d_name");
			dStreet1 = rs.getString("d_street_1");
			dStreet2 = rs.getString("d_street_2");
			dCity = rs.getString("d_city");
			dState = rs.getString("d_state");
			dZip = rs.getString("d_zip");
			dYtd = rs.getDouble("d_ytd");
		} else
			throw new RuntimeException();
		rs.close();

		sql = "UPDATE district SET d_ytd =  "
				+ DoublePlainPrinter.toPlainString(dYtd + hAmount)
				+ " WHERE d_w_id =" + wid + " AND d_id = " + did;
		MysqlService.executeUpdateQuery(sql, stm);

		long cIdCount;
		sql = "SELECT COUNT(c_id) as \"countofc_id\" FROM customer WHERE c_w_id = "
				+ cwid
				+ " AND c_d_id = "
				+ cdid
				+ " AND c_last = '"
				+ cLastStr
				+ "'";
		rs = MysqlService.executeQuery(sql, stm);
		rs.beforeFirst();

		if (rs.next())
			cIdCount = rs.getInt("countofc_id");
		else
			throw new RuntimeException("last name '" + cLastStr
					+ "' is not existed");

		rs.close();
		if (cIdCount % 2 == 1)
			cIdCount++;

		int cPaymentCnt = 0;
		double cYtdPayment = 0;
		double cBalanceDouble = 0;
		String cCreditStr = null;

		sql = "SELECT c_first, c_middle, c_id, c_street_1, c_street_2, c_city,"
				+ " c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim, "
				+ "c_discount, c_balance, c_ytd_payment, c_payment_cnt FROM "
				+ "customer WHERE c_w_id =" + cwid + " AND c_d_id = " + cdid
				+ " AND c_last = '" + cLastStr + "'";

		rs = MysqlService.executeQuery(sql, stm);
		rs.beforeFirst();
		int t = 1;
		boolean isSelected = false;
		while (rs.next()) {
			if (t == cIdCount / 2) {
				rs.getString("c_first");
				rs.getString("c_middle");
				rs.getInt("c_id");
				rs.getString("c_street_1");
				rs.getString("c_street_2");
				rs.getString("c_city");
				rs.getString("c_state");
				rs.getString("c_zip");
				rs.getString("c_phone");
				cCreditStr = rs.getString("c_credit");
				rs.getLong("c_since");
				rs.getDouble("c_credit_lim");
				cBalanceDouble = rs.getDouble("c_balance");
				rs.getDouble("c_discount");
				rs.getDouble("c_ytd_payment");
				rs.getInt("c_payment_cnt");
				isSelected = true;
				break;
			}
			t++;
		}
		rs.close();
		if (!isSelected)
			throw new RuntimeException();

		sql = "UPDATE customer SET c_balance = "
				+ DoublePlainPrinter.toPlainString(cBalanceDouble - hAmount)
				+ ", c_ytd_payment = "
				+ DoublePlainPrinter.toPlainString(cYtdPayment + hAmount)
				+ ", c_payment_cnt = " + (cPaymentCnt + 1) + " WHERE c_w_id ="
				+ cwid + " AND c_d_id = " + cdid + " AND c_id = " + cidInt;
		MysqlService.executeUpdateQuery(sql, stm);

		if (cCreditStr.equals("BC")) {
			isBadCredit = true;
			sql = "SELECT c_data FROM customer WHERE c_w_id = " + cwid
					+ " AND c_d_id = " + cdid + " AND c_id = " + cidInt;
			rs = MysqlService.executeQuery(sql, stm);
			rs.beforeFirst();
			if (rs.next())
				cDataStr = rs.getString("c_data");
			else
				throw new RuntimeException();
			rs.close();
			cDataStr = cidInt + " " + cdid + " " + cwid + " " + did + " " + wid
					+ " " + hAmount + " " + cDataStr;
			if (cDataStr.length() > 500)
				cDataStr = cDataStr.substring(0, 499);

			sql = "UPDATE customer SET c_data  = '" + cDataStr
					+ "' WHERE c_w_id = " + cwid + " AND c_d_id = " + cdid
					+ " AND c_id = " + cidInt;

			MysqlService.executeQuery(sql, stm);

			if (cDataStr.length() > 200)
				cDataStr = cDataStr.substring(0, 199);
		}
		String hData = wName + " " + dName;

		hDateLong = System.currentTimeMillis();

		sql = "INSERT INTO history (h_c_id, h_c_d_id, h_c_w_id, h_d_id, "
				+ "h_w_id, h_date, h_amount, h_data) VALUES (" + cidInt + ", "
				+ cdid + ", " + cwid + ", " + did + ", " + wid + ", "
				+ hDateLong + ", " + DoublePlainPrinter.toPlainString(hAmount)
				+ ", '" + hData + "')";
		MysqlService.executeQuery(sql, stm);

		conn.commit();
		MysqlService.closeStatement(stm);
		MysqlService.disconnect(conn);
	}

	protected void executeMysqlByCid() throws SQLException {

		conn = MysqlService.connect();
		Statement stm = MysqlService.createStatement(conn);
		ResultSet rs = null;
		conn.setAutoCommit(false);

		String wName, wStreet1, wStreet2, wCity, wState, wZip;
		double wYtd;
		String sql = "SELECT w_name, w_street_1, w_street_2, w_city, "
				+ "w_state, w_zip, w_ytd FROM warehouse WHERE w_id =" + wid;
		rs = MysqlService.executeQuery(sql, stm);
		rs.beforeFirst();
		if (rs.next()) {
			wName = rs.getString("w_name");
			wStreet1 = rs.getString("w_street_1");
			wStreet2 = rs.getString("w_street_2");
			wCity = rs.getString("w_city");
			wState = rs.getString("w_state");
			wZip = rs.getString("w_zip");
			wYtd = rs.getDouble("w_ytd");
		} else
			throw new RuntimeException();
		rs.close();

		sql = "UPDATE warehouse SET w_ytd =  "
				+ DoublePlainPrinter.toPlainString(wYtd + hAmount)
				+ " WHERE w_id =" + wid;
		MysqlService.executeUpdateQuery(sql, stm);

		String dName, dStreet1, dStreet2, dCity, dState, dZip;
		double dYtd;
		sql = "SELECT d_name, d_street_1, d_street_2, d_city, d_state, d_zip,"
				+ "d_ytd FROM district WHERE d_w_id = " + wid + " AND d_id = "
				+ did;
		rs = MysqlService.executeQuery(sql, stm);
		rs.beforeFirst();
		if (rs.next()) {
			dName = rs.getString("d_name");
			dStreet1 = rs.getString("d_street_1");
			dStreet2 = rs.getString("d_street_2");
			dCity = rs.getString("d_city");
			dState = rs.getString("d_state");
			dZip = rs.getString("d_zip");
			dYtd = rs.getDouble("d_ytd");
		} else
			throw new RuntimeException();
		rs.close();

		sql = "UPDATE district SET d_ytd =  "
				+ DoublePlainPrinter.toPlainString(dYtd + hAmount)
				+ " WHERE d_w_id =" + wid + " AND d_id = " + did;
		MysqlService.executeUpdateQuery(sql, stm);

		int cPaymentCnt;
		double cYtdPayment;
		double cBalanceDouble;
		String cCreditStr;
		sql = "SELECT c_first, c_middle, c_last, c_street_1, c_street_2, c_city,"
				+ " c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim, "
				+ "c_discount, c_balance, c_ytd_payment, c_payment_cnt FROM "
				+ "customer WHERE c_w_id ="
				+ cwid
				+ " AND c_d_id = "
				+ cdid
				+ " AND c_id = " + cidInt;

		rs = MysqlService.executeQuery(sql, stm);
		rs.beforeFirst();
		if (rs.next()) {

			rs.getString("c_first");
			rs.getString("c_middle");
			rs.getString("c_last");
			rs.getString("c_street_1");
			rs.getString("c_street_2");
			rs.getString("c_city");
			rs.getString("c_state");
			rs.getString("c_zip");
			rs.getString("c_phone");
			cCreditStr = rs.getString("c_credit");
			rs.getLong("c_since");
			rs.getDouble("c_credit_lim");
			cBalanceDouble = rs.getDouble("c_balance");
			rs.getDouble("c_discount");
			cYtdPayment = rs.getDouble("c_ytd_payment");
			cPaymentCnt = rs.getInt("c_payment_cnt");

		} else
			throw new RuntimeException();
		rs.close();

		sql = "UPDATE customer SET c_balance = "
				+ DoublePlainPrinter.toPlainString(cBalanceDouble - hAmount)
				+ ", c_ytd_payment = "
				+ DoublePlainPrinter.toPlainString(cYtdPayment + hAmount)
				+ ", c_payment_cnt = " + (cPaymentCnt + 1) + " WHERE c_w_id ="
				+ cwid + " AND c_d_id = " + cdid + " AND c_id = " + cidInt;
		MysqlService.executeUpdateQuery(sql, stm);

		if (cCreditStr.equals("BC")) {
			isBadCredit = true;
			sql = "SELECT c_data FROM customer WHERE c_w_id = " + cwid
					+ " AND c_d_id = " + cdid + " AND c_id = " + cidInt;
			rs = MysqlService.executeQuery(sql, stm);
			rs.beforeFirst();
			if (rs.next())
				cDataStr = rs.getString("c_data");
			else
				throw new RuntimeException();
			rs.close();
			cDataStr = cidInt + " " + cdid + " " + cwid + " " + did + " " + wid
					+ " " + hAmount + " " + cDataStr;

			if (cDataStr.length() > 500)
				cDataStr = cDataStr.substring(0, 499);

			sql = "UPDATE customer SET c_data  = '" + cDataStr
					+ "' WHERE c_w_id = " + cwid + " AND c_d_id = " + cdid
					+ " AND c_id = " + cidInt;

			MysqlService.executeUpdateQuery(sql, stm);

			if (cDataStr.length() > 200)
				cDataStr = cDataStr.substring(0, 199);
		}
		String hData = wName + " " + dName;

		hDateLong = System.currentTimeMillis();

		sql = "INSERT INTO history (h_c_id, h_c_d_id, h_c_w_id, h_d_id, "
				+ "h_w_id, h_date, h_amount, h_data) VALUES (" + cidInt + ", "
				+ cdid + ", " + cwid + ", " + did + ", " + wid + ", "
				+ hDateLong + ", " + DoublePlainPrinter.toPlainString(hAmount)
				+ ", '" + hData + "')";
		MysqlService.executeUpdateQuery(sql, stm);

		conn.commit();
	}

	protected SpResultSet createResultSet() {
		/*
		 * TODO The output information is not strictly followed the TPC-C
		 * definition. See the session 2.5.3.4 in TPC-C 5.11 document.
		 */
		Schema sch = new Schema();
		Type statusType = Type.VARCHAR(10);
		sch.addField("status", statusType);

		SpResultRecord rec = new SpResultRecord();
		String status = isCommitted ? "committed" : "abort";
		rec.setVal("status", new VarcharConstant(status, statusType));

		return new SpResultSet(sch, rec);
	}
}