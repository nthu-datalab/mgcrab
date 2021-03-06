package netdb.software.benchmark.tpcc.procedure.mysql;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import netdb.software.benchmark.tpcc.TpccConstants;
import netdb.software.benchmark.tpcc.server.StartUp;
import netdb.software.benchmark.tpcc.util.DoublePlainPrinter;
import netdb.software.benchmark.tpcc.util.MysqlService;
import netdb.software.benchmark.tpcc.util.OutputMsgBuilder;

import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.sql.storedprocedure.SpResultRecord;
import org.vanilladb.core.sql.storedprocedure.StoredProcedure;
import org.vanilladb.core.storage.tx.Transaction;

public class MysqlDeliveryProc implements StoredProcedure {
	public static String resultFileName = "DeliveryResultFile.txt";

	private int wid, carrierId;

	public MysqlDeliveryProc() {

	}

	@Override
	public void prepare(Object... pars) {
		prepareParameters(pars);
	}

	@Override
	public SpResultSet execute() {
		queueRequest();
		return createResultSet();
	}

	private void prepareParameters(Object... pars) {
		if (pars.length != 2)
			throw new RuntimeException("wrong pars list");
		wid = (Integer) pars[0];
		carrierId = (Integer) pars[1];
	}

	private SpResultSet createResultSet() {
		Schema sch = new Schema();
		Type statusType = Type.VARCHAR(30);
		sch.addField("status", statusType);
		sch.addField("wid", Type.INTEGER);
		sch.addField("carrier_id", Type.INTEGER);

		SpResultRecord rec = new SpResultRecord();
		rec.setVal("status", new VarcharConstant(TpccConstants.QUEUED_MESSAGE,
				statusType));
		rec.setVal("wid", new IntegerConstant(wid));
		rec.setVal("carrier_id", new IntegerConstant(carrierId));
		return new SpResultSet(sch, rec);
	}

	private void queueRequest() {
		DeferredDeliverProc proc = new DeferredDeliverProc(wid, carrierId,
				System.nanoTime());
		proc.start();
	}

	private class DeferredDeliverProc extends Thread {
		private int wid;
		private int carrierId;
		private long queuedTime;

		public DeferredDeliverProc(int wid, int carrierId, long queuedTime) {
			this.wid = wid;
			this.carrierId = carrierId;
			this.queuedTime = queuedTime;
		}

		@Override
		public void run() {

			OutputMsgBuilder builder = new OutputMsgBuilder();
			builder.append(queuedTime).append(wid);
			// For each district, run one database transaction
			for (int i = 1; i <= TpccConstants.DISTRICTS_PER_WAREHOUSE; i++) {
				Transaction tx = VanillaDb.txMgr().transaction(
						Connection.TRANSACTION_SERIALIZABLE, false);

				Connection conn = MysqlService.connect();
				Statement stm = MysqlService.createStatement(conn);
				ResultSet rs = null;

				try {

					conn.setAutoCommit(false);

					int did = i;
					boolean hasNewOrder = true;
					int noOid = 0;
					// Choose one oldest un-delivered order of this district
					String sql = "SELECT no_o_id FROM new_order WHERE no_w_id = "
							+ wid
							+ " AND no_d_id = "
							+ did
							+ " ORDER BY no_o_id ASC";
					rs = MysqlService.executeQuery(sql, stm);
					rs.beforeFirst();
					if (rs.next()) {
						noOid = rs.getInt("no_o_id");
						builder.append(did, noOid);
						hasNewOrder = false;
					} else {
						builder.append(did, "NO-ORDER");
						hasNewOrder = false;
					}
					rs.close();

					if (hasNewOrder) {
						// Delete this order from new order
						sql = "DELETE FROM new_order WHERE no_w_id = " + wid
								+ " AND no_d_id = " + did + " AND no_o_id = "
								+ noOid;
						MysqlService.executeUpdateQuery(sql, stm);

						// get customer id
						int oCid;
						sql = "SELECT o_c_id FROM orders WHERE o_id = " + noOid
								+ " AND o_w_id = " + wid + " AND o_d_id = "
								+ did;
						rs = MysqlService.executeQuery(sql, stm);
						rs.beforeFirst();
						if (rs.next())
							oCid = rs.getInt("o_c_id");
						else
							throw new SQLException("no_o_id:" + noOid);

						rs.close();

						// update carrier_id
						sql = "UPDATE orders SET o_carrier_id = " + carrierId
								+ " WHERE o_id = " + noOid + " AND o_w_id = "
								+ wid + " AND o_d_id = " + did;
						MysqlService.executeUpdateQuery(sql, stm);

						// Update delivery date
						long olDelDate = System.currentTimeMillis();
						sql = "UPDATE order_line SET ol_delivery_d =  "
								+ olDelDate + " WHERE ol_o_id = " + noOid
								+ " AND ol_w_id = " + wid + " AND ol_d_id = "
								+ did;
						MysqlService.executeUpdateQuery(sql, stm);

						// Retrieve the sum of all order-line amounts
						double sumOfOlAmount = 0;
						sql = "SELECT SUM(ol_amount) as \"sumofol_amount\" FROM order_line WHERE ol_o_id ="
								+ noOid
								+ " AND ol_w_id = "
								+ wid
								+ " AND ol_d_id = " + did;
						rs = MysqlService.executeQuery(sql, stm);
						rs.beforeFirst();
						if (rs.next())
							sumOfOlAmount = rs.getDouble("sumofol_amount");
						else
							throw new SQLException();
						rs.close();

						// Update customer balance
						// TODO strange
						sql = "UPDATE customer SET c_balance = c_balance + "
								+ DoublePlainPrinter
										.toPlainString(sumOfOlAmount)
								+ ", c_delivery_cnt = c_delivery_cnt + 1 WHERE c_id = "
								+ oCid + " AND c_w_id = " + wid
								+ " AND c_d_id = " + did;
						MysqlService.executeUpdateQuery(sql, stm);

						conn.commit();

					}
				} catch (Exception e) {
					e.printStackTrace();
					tx.rollback();
				}

				MysqlService.closeStatement(stm);
				MysqlService.disconnect(conn);
				tx.commit();
			}
			// Record the time delivery txn completed
			builder.append(System.nanoTime());

			System.out.println("finish delivery procedure!!!!");

			// Write into result file
			writeToResultFile(builder.build());
		}

	}

	private static synchronized void writeToResultFile(String outMsg) {
		// Need to be thread-safe
		String homedir = System.getProperty("user.home");
		File dbDirectory = new File(homedir, StartUp.dirName);
		File f = new File(dbDirectory, resultFileName);
		try {
			if (!f.exists())
				f.createNewFile();
			FileWriter out = new FileWriter(f, true);
			out.write(outMsg);
			out.write('\n');
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
