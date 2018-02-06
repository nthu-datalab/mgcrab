package netdb.software.benchmark.tpce.procedure.vanilladddb.calvin;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import netdb.software.benchmark.tpce.TpceWorkload;
import netdb.software.benchmark.tpce.procedure.TestbedLoaderProcParamHelper;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.storage.tx.recovery.CheckpointTask;
import org.vanilladb.core.storage.tx.recovery.RecoveryMgr;
import org.vanilladb.dd.cache.CachedRecord;
import org.vanilladb.dd.cache.calvin.CalvinCacheMgr;
import org.vanilladb.dd.remote.groupcomm.TupleSet;
import org.vanilladb.dd.schedule.calvin.CalvinStoredProcedure;
import org.vanilladb.dd.server.VanillaDdDb;
import org.vanilladb.dd.sql.RecordKey;
import org.vanilladb.dd.storage.metadata.PartitionMetaMgr;

public class TestbedLoaderProc extends
		CalvinStoredProcedure<TestbedLoaderProcParamHelper> {
	private static Logger logger = Logger.getLogger(TestbedLoaderProc.class
			.getName());

	public TestbedLoaderProc(long txNum) {
		super(txNum, new TestbedLoaderProcParamHelper());

		if (VanillaDdDb.serverId() == paramHelper.getMasterNodeId())
			isMaster = true;
	}

	@Override
	protected void prepareKeys() {
		List<String> writeTables = Arrays.asList(paramHelper.getTables());
		localWriteTables.addAll(writeTables);
	}

	@Override
	protected boolean executeSql() {
		if (logger.isLoggable(Level.INFO))
			logger.info("Start loading testbed...");

		// turn off logging set value to speed up loading process
		// TODO: remove this hack code in the future
		RecoveryMgr.logSetVal(false);

		// Load testbed
		boolean success = loadTestbed();

		if (logger.isLoggable(Level.INFO))
			logger.info("Loading completed. Flush all loading data to disks...");

		// TODO: remove this hack code in the future
		RecoveryMgr.logSetVal(true);

		// Create a checkpoint
		CheckpointTask cpt = new CheckpointTask();
		cpt.createCheckpoint();

		// Delete the log file and create a new one
		VanillaDb.logMgr().removeAndCreateNewLog();

		// Notification
		if (isMaster) {
			if (logger.isLoggable(Level.INFO))
				logger.info("Waiting for other servers...");

			// Master: Wait for notification from other nodes
			waitForNotification();
		} else {
			// Salve: Send notification to the master
			sendNotification();
		}

		if (logger.isLoggable(Level.INFO))
			logger.info("Loading procedure finished.");

		return success;
	}

	public boolean loadTestbed() {

		try {
			generateTradeType();
			generateBroker();
			generateCustomer();
			generateCustomerAccount();
			generateCompany();
			generateSecurity();
			// generateHolding();
			// generateHoldingHistory();
			// generateTrade();
			// generateTradeHistory();
			generateLastTrade();
			
			return true;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
	}

	private void generateCustomer() throws IOException {
		if (logger.isLoggable(Level.FINE))
			logger.info("Start populate Customers");
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(TpceWorkload.TESTBED_DIR
					+ "Customer.txt"));

			String line = null;
			while ((line = br.readLine()) != null) {
				String[] fields = line.split("\\|");

				String sql = String.format(
						"INSERT INTO customer(c_id, c_name) VALUES(%s, '%s')",
						fields[0], fields[3] + " " + fields[4]);
				int result = VanillaDb.planner().executeUpdate(sql, tx);
				if (result <= 0)
					throw new RuntimeException();

			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			br.close();
		}
	}

	private void generateCustomerAccount() throws IOException {
		if (logger.isLoggable(Level.FINE))
			logger.info("Start populate Customers Account");

		BufferedReader br = null;
		try {

			br = new BufferedReader(new FileReader(TpceWorkload.TESTBED_DIR
					+ "CustomerAccount.txt"));

			String line = null;
			while ((line = br.readLine()) != null) {
				String[] fields = line.split("\\|");

				String sql = String.format(
						"INSERT INTO customer_account(ca_id, ca_b_id, ca_c_id, ca_name, ca_bal) "
								+ "VALUES (%s, %s, %s, '%s', %s)", fields[0],
						fields[1], fields[2], fields[3], fields[5]);
				int result = VanillaDb.planner().executeUpdate(sql, tx);
				if (result <= 0)
					throw new RuntimeException();

			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			br.close();
		}
	}

	private void generateBroker() throws IOException {
		if (logger.isLoggable(Level.FINE))
			logger.info("Start populate Broker");

		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(TpceWorkload.TESTBED_DIR
					+ "Broker.txt"));
			String line = null;
			while ((line = br.readLine()) != null) {
				String[] fields = line.split("\\|");

				String sql = String.format(
						"INSERT INTO broker(b_id, b_name, b_num_trades) "
								+ "VALUES(%s,'%s',%s)", fields[0], fields[2],
						fields[3]);
				int result = VanillaDb.planner().executeUpdate(sql, tx);
				if (result <= 0)
					throw new RuntimeException();

			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			br.close();
		}
	}

	private void generateTradeType() throws IOException {
		if (logger.isLoggable(Level.FINE))
			logger.info("Start populate Trade Type");

		BufferedReader br = null;
		try {
			System.out.println("path: " + TpceWorkload.TESTBED_DIR
					+ "TradeType.txt");
			br = new BufferedReader(new FileReader(TpceWorkload.TESTBED_DIR
					+ "TradeType.txt"));
			String line = null;
			while ((line = br.readLine()) != null) {
				String[] fields = line.split("\\|");

				String sql = String.format(
						"INSERT INTO trade_type( tt_id, tt_name, tt_is_sell, tt_is_mrkt) "
								+ "VALUES('%s','%s',%s, %s)", fields[0],
						fields[1], fields[2], fields[3]);
				int result = VanillaDb.planner().executeUpdate(sql, tx);
				if (result <= 0)
					throw new RuntimeException();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			br.close();
		}
	}

	private void generateCompany() throws IOException {
		if (logger.isLoggable(Level.FINE))
			logger.info("Start populate Company");

		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(TpceWorkload.TESTBED_DIR
					+ "Company.txt"));
			String line = null;
			while ((line = br.readLine()) != null) {
				String[] fields = line.split("\\|");

				String sql = String.format(
						"INSERT INTO company(co_id, co_name, co_ceo) "
								+ "VALUES(%s, '%s', '%s')", fields[0],
						fields[2].replaceAll("\\'", "\\\\\\'"), fields[5]);
				int result = VanillaDb.planner().executeUpdate(sql, tx);
				if (result <= 0)
					throw new RuntimeException();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			br.close();
		}
	}

	private void generateSecurity() throws IOException {
		if (logger.isLoggable(Level.FINE))
			logger.info("Start populate Security");

		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(TpceWorkload.TESTBED_DIR
					+ "Security.txt"));
			String line = null;
			while ((line = br.readLine()) != null) {
				String[] fields = line.split("\\|");

				String sql = String.format(
						"INSERT INTO security(s_symb, s_name, s_co_id) "
								+ "VALUES('%s', '%s', %s)", fields[0],
						fields[3].replaceAll("\\'", "\\\\\\'"), fields[5]);
				int result = VanillaDb.planner().executeUpdate(sql, tx);
				if (result <= 0)
					throw new RuntimeException();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			br.close();
		}
	}

	private void generateHolding() throws IOException {
		if (logger.isLoggable(Level.FINE))
			logger.info("Start populate Holding");

		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(TpceWorkload.TESTBED_DIR
					+ "Holding.txt"));
			String line = null;
			while ((line = br.readLine()) != null) {
				String[] fields = line.split("\\|");

				// parse the date to long
				DateFormat formatter = new SimpleDateFormat(
						"yyyy-mm-dd hh:mm:ss");
				long longDate = formatter.parse(fields[3]).getTime();
				String sql = String.format(
						"INSERT INTO holding(h_t_id, h_ca_id, h_s_symb, h_dts, h_price, h_qty) "
								+ "VALUES(%s, %s, '%s', %s, %s, %s)",
						fields[0], fields[1], fields[2], longDate, fields[4],
						fields[5]);
				int result = VanillaDb.planner().executeUpdate(sql, tx);
				if (result <= 0)
					throw new RuntimeException();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			br.close();
		}
	}

	private void generateHoldingHistory() throws IOException {
		if (logger.isLoggable(Level.FINE))
			logger.info("Start populate Holding History");

		BufferedReader br = null;
		try {

			br = new BufferedReader(new FileReader(TpceWorkload.TESTBED_DIR
					+ "HoldingHistory.txt"));
			String line = null;
			while ((line = br.readLine()) != null) {
				String[] fields = line.split("\\|");

				String sql = String.format(
						"INSERT INTO holding_history(hh_h_t_id, hh_t_id, hh_before_qty, hh_after_qty) "
								+ "VALUES(%s, %s, %s, %s)", fields[0],
						fields[1], fields[2], fields[3]);
				int result = VanillaDb.planner().executeUpdate(sql, tx);
				if (result <= 0)
					throw new RuntimeException();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			br.close();
		}
	}

	private void generateTrade() throws IOException {
		if (logger.isLoggable(Level.FINE))
			logger.info("Start populate Trade");

		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(TpceWorkload.TESTBED_DIR
					+ "Trade.txt"));
			String line = null;
			while ((line = br.readLine()) != null) {
				String[] fields = line.split("\\|");

				// parse the date to long
				DateFormat formatter = new SimpleDateFormat(
						"yyyy-mm-dd hh:mm:ss");
				long longDate = formatter.parse(fields[1]).getTime();
				String sql = String
						.format("INSERT INTO trade(t_id, t_dts, t_tt_id, t_s_symb, t_qty, t_bid_price, t_ca_id, t_trade_price) "
								+ "VALUES(%s, %s, '%s', '%s', %s, %s, %s, %s)",
								fields[0], longDate, fields[3], fields[5],
								fields[6], fields[7], fields[8], fields[10]);
				int result = VanillaDb.planner().executeUpdate(sql, tx);
				if (result <= 0)
					throw new RuntimeException();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			br.close();
		}
	}

	private void generateTradeHistory() throws IOException {
		if (logger.isLoggable(Level.FINE))
			logger.info("Start populate Trade History");

		BufferedReader br = null;
		try {

			br = new BufferedReader(new FileReader(TpceWorkload.TESTBED_DIR
					+ "TradeHistory.txt"));
			String line = null;
			while ((line = br.readLine()) != null) {
				String[] fields = line.split("\\|");

				// parse the date to long
				DateFormat formatter = new SimpleDateFormat(
						"yyyy-mm-dd hh:mm:ss");
				long longDate = formatter.parse(fields[1]).getTime();
				String sql = String.format(
						"INSERT INTO trade_history(th_t_id, th_dts) "
								+ "VALUES(%s, %s)", fields[0], longDate);
				int result = VanillaDb.planner().executeUpdate(sql, tx);
				if (result <= 0)
					throw new RuntimeException();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			br.close();
		}
	}

	private void generateLastTrade() throws IOException {
		if (logger.isLoggable(Level.FINE))
			logger.info("Start populate Last Trade");

		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(TpceWorkload.TESTBED_DIR
					+ "LastTrade.txt"));
			String line = null;
			while ((line = br.readLine()) != null) {
				String[] fields = line.split("\\|");

				// parse the date to long
				DateFormat formatter = new SimpleDateFormat(
						"yyyy-mm-dd hh:mm:ss");
				long longDate = formatter.parse(fields[1]).getTime();
				String sql = String.format(
						"INSERT INTO last_trade(lt_s_symb, lt_dts, lt_price, lt_open_price, lt_vol) "
								+ "VALUES('%s', %s, %s, %s, %s)", fields[0],
						longDate, fields[2], fields[3], fields[4]);
				int result = VanillaDb.planner().executeUpdate(sql, tx);
				if (result <= 0)
					throw new RuntimeException();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			br.close();
		}
	}

	private void waitForNotification() {
		CalvinCacheMgr cm = (CalvinCacheMgr) VanillaDdDb.cacheMgr();

		// Wait for notification from other nodes
		for (int nodeId = 0; nodeId < PartitionMetaMgr.NUM_PARTITIONS; nodeId++)
			if (nodeId != VanillaDdDb.serverId()) {
				RecordKey notKey = paramHelper.getFinishNotificationKey(nodeId);
				CachedRecord rec = cm.read(notKey, txNum, tx, false);
				Constant con = rec.getVal(paramHelper.getFinishFieldName());
				int value = (int) con.asJavaVal();
				if (value != 1)
					throw new RuntimeException(
							"Notification value error, node no." + nodeId
									+ " sent " + value);

				if (logger.isLoggable(Level.FINE))
					logger.fine("Receive notification from node no." + nodeId);
			}
	}

	private void sendNotification() {
		RecordKey notKey = paramHelper.getFinishNotificationKey(VanillaDdDb
				.serverId());
		CachedRecord notVal = paramHelper.getFinishNotificationValue(txNum);

		TupleSet ts = new TupleSet(-1);
		// Use node id as source tx number
		ts.addTuple(notKey, txNum, txNum, notVal);
		VanillaDdDb.connectionMgr().pushTupleSet(paramHelper.getMasterNodeId(),
				ts);

		if (logger.isLoggable(Level.FINE))
			logger.fine("The notification is sent to the master by tx." + txNum);
	}
}
