package netdb.software.benchmark.tpce;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import netdb.software.benchmark.App;
import netdb.software.benchmark.TxnExecutor;
import netdb.software.benchmark.Workload;
import netdb.software.benchmark.tpce.TpceParameterManager.Trade;
import netdb.software.benchmark.tpce.remote.SutConnection;
import netdb.software.benchmark.tpce.txnexecutor.TradeOrderTxnExecutor;
import netdb.software.benchmark.tpce.txnexecutor.TradeResultTxnExecutor;
import netdb.software.benchmark.util.RandomValueGenerator;

public class TpceWorkload extends Workload {

	private static Logger logger = Logger.getLogger(App.class.getName());

	public static TpceParameterManager paramGen;
	public static String TESTBED_DIR;
	public static double TRADE_ORDER_PERCENTAGE = 0.8;
	private RandomValueGenerator rg = new RandomValueGenerator();

	static {
		paramGen = new TpceParameterManager();
		loadSystemProperties();
		loadTpceFieldInfo();
	}

	@Override
	public void initialize() {

	}

	@Override
	public TxnExecutor nextTxn(SutConnection spc) {
		boolean isOrder = rg.randomChooseFromDistribution(
				TRADE_ORDER_PERCENTAGE, 1 - TRADE_ORDER_PERCENTAGE) == 0 ? true
				: false;

		if (isOrder)
			return new TradeOrderTxnExecutor(spc);
		else {
			Trade trade = paramGen.getOldestTrade();
			if (trade != null)
				return new TradeResultTxnExecutor(spc, trade);
			else
				return new TradeOrderTxnExecutor(spc);
		}

	}

	private static void loadSystemProperties() {
		boolean config = false;
		String path = System
				.getProperty("netdb.software.benchmark.tpce.config.file");
		if (path != null) {
			FileInputStream fis = null;
			try {
				fis = new FileInputStream(path);
				System.getProperties().load(fis);
				config = true;
			} catch (IOException e) {
				// do nothing
			} finally {
				try {
					if (fis != null)
						fis.close();
				} catch (IOException e) {
					// do nothing
				}
			}
		}
		if (!config && logger.isLoggable(Level.WARNING))
			logger.warning("error reading the config file, using defaults");
		String prop = System.getProperty(TpceWorkload.class.getName()
				+ ".TESTBED_DIR");
		TESTBED_DIR = (prop == null) ? "" : prop.trim();

		prop = System.getProperty(TpceWorkload.class.getName()
				+ ".TRADE_ORDER_PERCENTAGE");
		TRADE_ORDER_PERCENTAGE = (prop == null) ? 0.8 : Double.parseDouble(prop
				.trim());

	}

	private static void loadTpceFieldInfo() {
		try {
			BufferedReader br = new BufferedReader(new FileReader(
					TpceWorkload.TESTBED_DIR + "Customer.txt"));

			String line = null;
			while ((line = br.readLine()) != null) {
				String[] fields = line.split("\\|");
				TpceWorkload.paramGen.addCustomerId(Long.parseLong(fields[0]));
			}

			br.close();

			br = new BufferedReader(new FileReader(TpceWorkload.TESTBED_DIR
					+ "CustomerAccount.txt"));

			line = null;
			while ((line = br.readLine()) != null) {
				String[] fields = line.split("\\|");
				TpceWorkload.paramGen.addCustomerAccount(
						Long.parseLong(fields[2]), Long.parseLong(fields[0]),
						Long.parseLong(fields[1]));
			}

			br.close();

			br = new BufferedReader(new FileReader(TpceWorkload.TESTBED_DIR
					+ "Company.txt"));

			line = null;
			while ((line = br.readLine()) != null) {
				String[] fields = line.split("\\|");
				TpceWorkload.paramGen.addCompanyName(fields[2]);
			}

			br.close();

			br = new BufferedReader(new FileReader(TpceWorkload.TESTBED_DIR
					+ "Security.txt"));

			line = null;
			while ((line = br.readLine()) != null) {
				String[] fields = line.split("\\|");
				TpceWorkload.paramGen.addSecuritySymbol(fields[0]);
			}

			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
