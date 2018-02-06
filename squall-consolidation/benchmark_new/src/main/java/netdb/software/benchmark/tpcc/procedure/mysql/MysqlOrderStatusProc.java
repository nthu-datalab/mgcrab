package netdb.software.benchmark.tpcc.procedure.mysql;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import netdb.software.benchmark.tpcc.procedure.OrderStatusProcParamHelper;
import netdb.software.benchmark.tpcc.util.MysqlService;

import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.BigIntConstant;
import org.vanilladb.core.sql.DoubleConstant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.sql.storedprocedure.SpResultRecord;
import org.vanilladb.core.sql.storedprocedure.StoredProcedure;
import org.vanilladb.core.storage.tx.Transaction;

public class MysqlOrderStatusProc implements StoredProcedure {

	Connection conn;
	private Transaction tx;

	private OrderStatusProcParamHelper paramHelper = new OrderStatusProcParamHelper();

	public MysqlOrderStatusProc() {

	}

	@Override
	public void prepare(Object... pars) {
		paramHelper.prepareParameters(pars);
	}

	@Override
	public SpResultSet execute() {
		this.tx = VanillaDb.txMgr().transaction(
				Connection.TRANSACTION_SERIALIZABLE, true);

		conn = MysqlService.connect();
		try {
			if (paramHelper.isSelectByCLast())
				executeMysqlByCLast();
			else
				executeMysqlByCid();
			conn.commit();
			tx.commit();
		} catch (Exception e) {
			tx.rollback();
			paramHelper.setCommitted(false);
			e.printStackTrace();
		} finally {
			MysqlService.disconnect(conn);
		}
		return createResultSet();
	}

	protected void executeMysqlByCid() throws SQLException {

		Statement stm = MysqlService.createStatement(conn);
		ResultSet rs = null;
		conn.setAutoCommit(false);

		String sql = "SELECT c_balance, c_first, c_middle, c_last "
				+ "FROM customer WHERE c_w_id = " + paramHelper.getCwid()
				+ " AND c_d_id = " + paramHelper.getCdid() + " AND c_id = "
				+ paramHelper.getCid();
		rs = MysqlService.executeQuery(sql, stm);
		rs.beforeFirst();
		if (rs.next()) {
			paramHelper.setcFirst(rs.getString("c_first"));
			paramHelper.setcMiddle(rs.getString("c_middle"));
			paramHelper.setcLast(rs.getString("c_last"));
			paramHelper.setcBalance(rs.getDouble("c_balance"));
		} else
			throw new RuntimeException();
		rs.close();

		sql = "SELECT o_id, o_carrier_id, o_entry_d FROM orders WHERE o_w_id ="
				+ paramHelper.getCwid() + " AND o_d_id = "
				+ paramHelper.getCdid() + " AND o_c_id = "
				+ paramHelper.getCid() + " ORDER BY o_id desc";
		rs = MysqlService.executeQuery(sql, stm);
		rs.beforeFirst();
		if (rs.next()) {
			paramHelper.setOid(rs.getInt("o_id"));
			paramHelper.setCarrierId(rs.getInt("o_carrier_id"));
			paramHelper.setoEntryDate(rs.getLong("o_entry_d"));
		} else
			throw new RuntimeException();
		rs.close();

		int olIId, olSupplyWid, olQuantity;
		double olAmount;
		long olDeliveryDate;
		sql = "SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, "
				+ "ol_delivery_d FROM order_line WHERE ol_o_id ="
				+ paramHelper.getOid() + " AND ol_d_id = "
				+ paramHelper.getCdid() + " AND ol_w_id = "
				+ paramHelper.getCwid();
		rs = MysqlService.executeQuery(sql, stm);
		rs.beforeFirst();
		while (rs.next()) {
			olIId = rs.getInt("ol_i_id");
			olSupplyWid = rs.getInt("ol_supply_w_id");
			olQuantity = rs.getInt("ol_quantity");
			olAmount = rs.getDouble("ol_amount");
			olDeliveryDate = rs.getLong("ol_delivery_d");
		}
		rs.close();

	}

	protected void executeMysqlByCLast() throws SQLException {

		Statement stm = MysqlService.createStatement(conn);
		ResultSet rs = null;
		conn.setAutoCommit(false);

		long cidCount;
		String sql = "SELECT COUNT(c_id) as \"countofc_id\" FROM customer WHERE c_w_id = "
				+ paramHelper.getCwid()
				+ " AND c_d_id = "
				+ paramHelper.getCdid()
				+ " AND c_last = '"
				+ paramHelper.getcLast() + "'";
		rs = MysqlService.executeQuery(sql, stm);
		rs.beforeFirst();
		if (rs.next())
			cidCount = rs.getInt("countofc_id");
		else
			throw new RuntimeException();
		rs.close();
		if (cidCount % 2 == 1)
			cidCount++;

		sql = "SELECT c_balance, c_first, c_middle, c_id "
				+ "FROM customer WHERE c_w_id = " + paramHelper.getCwid()
				+ " AND c_d_id = " + paramHelper.getCdid() + " AND c_last = '"
				+ paramHelper.getcLast() + "'";
		rs = MysqlService.executeQuery(sql, stm);
		rs.beforeFirst();
		int t = 1;
		boolean isSelected = false;
		while (rs.next()) {
			if (t == cidCount / 2) {
				paramHelper.setcFirst(rs.getString("c_first"));
				paramHelper.setcMiddle(rs.getString("c_middle"));
				paramHelper.setCid(rs.getInt("c_id"));
				paramHelper.setcBalance(rs.getDouble("c_balance"));
				isSelected = true;
			}
			t++;
		}
		rs.close();
		if (!isSelected)
			throw new RuntimeException();

		sql = "SELECT o_id, o_carrier_id, o_entry_d FROM orders WHERE o_w_id ="
				+ paramHelper.getCwid() + " AND o_d_id = "
				+ paramHelper.getCdid() + " AND o_c_id = "
				+ paramHelper.getCid() + " ORDER BY o_id desc";
		rs = MysqlService.executeQuery(sql, stm);
		rs.beforeFirst();
		if (rs.next()) {
			paramHelper.setOid(rs.getInt("o_id"));
			paramHelper.setCarrierId(rs.getInt("o_carrier_id"));
			paramHelper.setoEntryDate(rs.getLong("o_entry_d"));
		} else
			throw new RuntimeException();
		rs.close();

		int olIId, olSupplyWid, olQuantity;
		double olAmount;
		long olDeliveryDate;
		sql = "SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, "
				+ "ol_delivery_d FROM order_line WHERE ol_o_id ="
				+ paramHelper.getOid() + " AND ol_d_id = "
				+ paramHelper.getCdid() + " AND ol_w_id = "
				+ paramHelper.getCwid();
		rs = MysqlService.executeQuery(sql, stm);
		rs.beforeFirst();
		while (rs.next()) {
			olIId = rs.getInt("ol_i_id");
			olSupplyWid = rs.getInt("ol_supply_w_id");
			olQuantity = rs.getInt("ol_quantity");
			olAmount = rs.getDouble("ol_amount");
			olDeliveryDate = rs.getLong("ol_delivery_d");
		}
		rs.close();

	}

	protected SpResultSet createResultSet() {
		/*
		 * TODO The output information is not strictly followed the TPC-C
		 * definition. See the session 2.6.3.4 in TPC-C 5.11 document.
		 */
		Schema sch = new Schema();
		Type statusType = Type.VARCHAR(10);
		Type var16 = Type.VARCHAR(16);
		Type var2 = Type.VARCHAR(2);
		sch.addField("status", statusType);
		sch.addField("cid", Type.INTEGER);
		sch.addField("c_first", var16);
		sch.addField("c_last", var16);
		sch.addField("c_middle", var2);
		sch.addField("c_balance", Type.DOUBLE);
		sch.addField("o_entry_date", Type.BIGINT);
		sch.addField("o_carrier_id", Type.INTEGER);

		SpResultRecord rec = new SpResultRecord();
		String status = paramHelper.isCommitted() ? "committed" : "abort";
		rec.setVal("status", new VarcharConstant(status, statusType));
		rec.setVal("cid", new IntegerConstant(paramHelper.getCid()));
		rec.setVal("c_first", new VarcharConstant(paramHelper.getcFirst(),
				var16));
		rec.setVal("c_last", new VarcharConstant(paramHelper.getcLast(), var16));
		rec.setVal("c_middle", new VarcharConstant(paramHelper.getcMiddle(),
				var2));
		rec.setVal("c_balance", new DoubleConstant(paramHelper.getcBalance()));
		rec.setVal("o_entry_date",
				new BigIntConstant(paramHelper.getoEntryDate()));
		rec.setVal("o_carrier_id",
				new IntegerConstant(paramHelper.getCarrierId()));

		return new SpResultSet(sch, rec);
	}
}
