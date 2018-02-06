package netdb.software.benchmark.tpcc.procedure.mysql;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;

import netdb.software.benchmark.tpcc.procedure.NewOrderProcedure;
import netdb.software.benchmark.tpcc.util.DoublePlainPrinter;
import netdb.software.benchmark.tpcc.util.MysqlService;

import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.sql.storedprocedure.StoredProcedure;

public class MysqlNewOrderProc extends NewOrderProcedure implements
		StoredProcedure {

	Connection conn;

	public MysqlNewOrderProc() {

	}

	@Override
	public void prepare(Object... pars) {
		prepareParameters(pars);
	}

	public SpResultSet execute() {
		// tx =
		// VanillaDb.txMgr().transaction(Connection.TRANSACTION_SERIALIZABLE,
		// false);
		try {
			executeSql();

		} catch (Exception e) {
			// tx.rollback();
			isCommitted = false;
			e.printStackTrace();
		} finally {
			MysqlService.disconnect(conn);
		}
		return createResultSet();
	}

	protected void executeSql() throws SQLException {

		conn = MysqlService.connect();
		Statement stm = MysqlService.createStatement(conn);
		ResultSet rs = null;

		try {
			conn.setAutoCommit(false);
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		String sql = "SELECT w_tax FROM warehouse WHERE w_id = " + wid;
		rs = MysqlService.executeQuery(sql, stm);

		rs.beforeFirst();
		if (rs.next())
			wTax = rs.getDouble("w_tax");
		else
			throw new RuntimeException();
		rs.close();

		int nextOId = 0;
		sql = "SELECT d_tax, d_next_o_id FROM district WHERE d_w_id = " + wid
				+ " AND d_id = " + did;
		rs = MysqlService.executeQuery(sql, stm);
		rs.beforeFirst();
		if (rs.next()) {
			dTax = rs.getDouble("d_tax");
			nextOId = rs.getInt("d_next_o_id");
		} else
			throw new RuntimeException();
		rs.close();

		sql = "SELECT c_discount, c_last, c_credit FROM customer WHERE c_w_id = "
				+ wid + " AND c_d_id = " + did + " AND c_id = " + cid;
		rs = MysqlService.executeQuery(sql, stm);
		rs.beforeFirst();
		if (rs.next()) {
			cDiscount = rs.getDouble("c_discount");
			cLast = rs.getString("c_last");
			cCredit = rs.getString("c_credit");
		} else
			throw new RuntimeException();
		rs.close();

		sql = "UPDATE district SET d_next_o_id = " + (nextOId + 1)
				+ " WHERE d_w_id = " + wid + " AND d_id = " + did;
		MysqlService.executeUpdateQuery(sql, stm);

		oEntryDate = System.currentTimeMillis();
		int isAllLocal = allLocal ? 1 : 0;
		sql = "INSERT INTO orders(o_id, o_d_id, o_w_id, o_c_id, o_entry_d,"
				+ "o_carrier_id, o_ol_cnt, o_all_local) VALUES (" + nextOId
				+ ", " + did + ", " + wid + ", " + cid + ", " + oEntryDate
				+ ", 0, " + olCount + ", " + isAllLocal + ")";
		MysqlService.executeUpdateQuery(sql, stm);

		sql = "INSERT INTO new_order (no_o_id, no_d_id, no_w_id)" + "VALUES ("
				+ nextOId + ", " + did + ", " + wid + ")";
		MysqlService.executeUpdateQuery(sql, stm);

		for (int i = 1; i <= olCount; i++) {
			int olIId = items[i - 1][0];
			int olSupplyWId = items[i - 1][1];
			int olQuantity = items[i - 1][2];
			String iName, iData;
			double iPrice;
			sql = "SELECT i_price, i_name, i_data FROM item WHERE i_id = "
					+ olIId;
			rs = MysqlService.executeQuery(sql, stm);
			rs.beforeFirst();
			if (rs.next()) {
				iPrice = rs.getDouble("i_price");
				iName = rs.getString("i_name");
				iData = rs.getString("i_data");
			} else {
				itemNotFound = true;
				break;
			}
			rs.close();

			int sQuantity = 0, sYtd, sOrderCnt;
			String sData, sDistXX, sDistInfo;
			if (did == 10)
				sDistXX = "s_dist_10";
			else
				sDistXX = "s_dist_0" + did;

			sql = "SELECT S_quantity, s_data, s_ytd, s_order_cnt, " + sDistXX
					+ " FROM stock WHERE s_i_id = " + olIId + " and s_w_id ="
					+ olSupplyWId;
			rs = MysqlService.executeQuery(sql, stm);
			rs.beforeFirst();
			if (rs.next()) {
				sQuantity = rs.getInt("s_quantity");
				sYtd = rs.getInt("s_ytd");
				sOrderCnt = rs.getInt("s_order_cnt");
				sData = rs.getString("s_data");
				sDistInfo = rs.getString(sDistXX);
			} else
				throw new RuntimeException("error select from stock...");

			rs.close();

			int q = sQuantity - olQuantity;
			if (q >= 10)
				sql = "UPDATE stock SET s_quantity =  " + q + ", s_ytd ="
						+ (sYtd + olQuantity) + ", s_order_cnt =  "
						+ (sOrderCnt + 1) + " WHERE s_i_id =" + olIId
						+ " AND s_w_id =" + olSupplyWId;
			else
				sql = "UPDATE stock SET s_quantity =  " + (q + 91)
						+ ", s_ytd =" + (sYtd + olQuantity)
						+ ", s_order_cnt =  " + (sOrderCnt + 1)
						+ " WHERE s_i_id =" + olIId + " AND s_w_id ="
						+ olSupplyWId;
			MysqlService.executeUpdateQuery(sql, stm);

			double olAmount = olQuantity * iPrice;
			String brandGeneric;
			if (iData.contains("ORIGINAL") && sData.contains("ORIGINAL"))
				brandGeneric = "B";
			else
				brandGeneric = "G";
			Timestamp time = new Timestamp(new java.util.Date().getTime());
			sql = "INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number,"
					+ "ol_i_id, ol_supply_w_id, ol_delivery_d, ol_quantity, ol_amount, "
					+ "ol_dist_info) VALUES ("
					+ nextOId
					+ ", "
					+ did
					+ ", "
					+ wid
					+ ", "
					+ i
					+ ", "
					+ olIId
					+ ", "
					+ olSupplyWId
					+ ", '"
					+ time
					+ "', "
					+ olQuantity
					+ ", "
					+ DoublePlainPrinter.toPlainString(olAmount)
					+ ", '"
					+ sDistInfo + "')";
			MysqlService.executeUpdateQuery(sql, stm);

			i++;
			totalAmount = totalAmount + olAmount;
		}

		try {
			conn.commit();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		MysqlService.closeStatement(stm);
		MysqlService.disconnect(conn);
	}
}