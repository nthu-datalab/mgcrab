package netdb.software.benchmark.tpce.procedure.vanilladddb.tpart;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.logging.Level;
import java.util.logging.Logger;

import netdb.software.benchmark.tpce.TpceWorkload;

import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.sql.storedprocedure.SpResultRecord;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.storage.tx.recovery.CheckpointTask;
import org.vanilladb.core.storage.tx.recovery.RecoveryMgr;
import org.vanilladb.dd.schedule.tpart.TPartStoredProcedure;
import org.vanilladb.dd.schedule.tpart.sink.SunkPlan;
import org.vanilladb.dd.server.VanillaDdDb;
import org.vanilladb.dd.sql.RecordKey;
import org.vanilladb.dd.storage.tx.concurrency.ConservativeOrderedCcMgr;

public class TestbedLoaderProc implements TPartStoredProcedure {
	private static Logger logger = Logger.getLogger(TestbedLoaderProc.class
			.getName());
	private final String TABLES[] = { "customer", "customer_account",
			"holding", "holding_history", "broker", "trade", "trade_history",
			"trade_type", "company", "last_trade", "security" };

	private Transaction tx;
	private long txNum;
	private boolean isCommitted;

	public TestbedLoaderProc(long txNum) {
		this.txNum = txNum;
	}

	@Override
	public RecordKey[] getReadSet() {
		return null;
	}

	@Override
	public RecordKey[] getWriteSet() {
		return null;
	}

	@Override
	public void setSunkPlan(SunkPlan plan) {
		// do nothing
	}

	@Override
	public double getWeight() {
		return 0;
	}

	@Override
	public int getProcedureType() {
		return TPartStoredProcedure.POPULATE;
	}

	@Override
	public void prepare(Object... pars) {
		// do nothing
	}

	@Override
	public boolean isReadOnly() {
		return false;
	}

	@Override
	public void requestConservativeLocks() {
		startTransaction();
		ConservativeOrderedCcMgr ccMgr = (ConservativeOrderedCcMgr) tx
				.concurrencyMgr();
		ccMgr.prepareSp(null, TABLES);
	}

	public Transaction startTransaction() {
		if (tx == null)
			tx = VanillaDdDb.txMgr().transaction(
					Connection.TRANSACTION_SERIALIZABLE, false, txNum);
		return tx;
	}

	@Override
	public SpResultSet execute() {
		loadTestbed();

		Schema sch = new Schema();
		Type t = Type.VARCHAR(10);
		sch.addField("status", t);
		SpResultRecord rec = new SpResultRecord();
		String status = isCommitted ? "committed" : "abort";
		rec.setVal("status", new VarcharConstant(status, t));
		return new SpResultSet(sch, rec);
	}

	public void loadTestbed() {
		// turn off logging set value to speed up loading process
		RecoveryMgr.logSetVal(false);

		ConservativeOrderedCcMgr ccMgr = (ConservativeOrderedCcMgr) tx
				.concurrencyMgr();
		ccMgr.executeSp(null, TABLES);

		isCommitted = true;
		try {
			System.out.println("Populate Trade Type");
			generateTradeType();
			System.out.println("Populate Broker");
			generateBroker();
			System.out.println("Populate Customer");
			generateCustomer();
			System.out.println("Populate Customer Account");
			generateCustomerAccount();
			System.out.println("Populate Company");
			generateCompany();
			System.out.println("Populate Security");
			generateSecurity();
			// System.out.println("Populate Holding");
			// generateHolding();
			// System.out.println("Populate Holding History");
			// generateHoldingHistory();
			// System.out.println("Populate Trade");
			// generateTrade();
			// System.out.println("Populate Trade History");
			// generateTradeHistory();
			System.out.println("Populate Last Trade");
			generateLastTrade();
		} catch (Exception e) {
			e.printStackTrace();
			tx.rollback();
			isCommitted = false;
		}
		tx.commit();

		RecoveryMgr.logSetVal(true);

		// create a checkpoint
		CheckpointTask cpt = new CheckpointTask();
		cpt.createCheckpoint();
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

	@Override
	public boolean isMaster() {
		// TODO Auto-generated method stub
		return true;
	}
}
