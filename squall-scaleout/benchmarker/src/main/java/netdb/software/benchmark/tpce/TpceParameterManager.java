package netdb.software.benchmark.tpce;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import netdb.software.benchmark.App;
import netdb.software.benchmark.util.RandomValueGenerator;

/**
 * This class generates valid parameters of MicroTpce benchmark.
 * 
 * @author shaokanp
 * 
 */
public class TpceParameterManager {

	private RandomValueGenerator rg;
	private List<String> companyNames = new ArrayList<String>(
			ScaleParameters.companyCount);
	private List<String> securitySymbols = new ArrayList<String>(
			ScaleParameters.securityCount);

	// store the account information (for dd), <0 ~ account count,
	// CustomerAccount(accountId, customerId, brokerId)>
	private HashMap<Long, Customer> customerMap = new HashMap<Long, Customer>();
	private List<Trade> tradeList = new ArrayList<Trade>();

	private final long _TIdentShift = 4300000000l;

	private enum CustomerTier {
		eCustomerTier1, eCustomerTier2, eCustomerTier3
	}

	private long curAccountNumber = 0;
	private long curTradeId = 0;

	public TpceParameterManager() {
		rg = new RandomValueGenerator();
	}

	public void addCompanyName(String name) {
		companyNames.add(name);
	}

	public void addSecuritySymbol(String symbol) {
		securitySymbols.add(symbol);
	}

	public void addCustomerId(long l) {
		customerMap.put(l, new Customer(l));
	}

	public void addCustomerAccount(long customerId, long accountId,
			long brokerId) {
		customerMap.get(customerId).addAccount(
				new CustomerAccount(accountId, brokerId));
	}

	public Customer getNonUniformRandomCustomer() {
		long iCHigh, iCLow;
		int customerTier = 1;

		double fCW = rg.randomDoubleIncrRange(0.0001, 2000, 0.000000001);

		// Generate a load unit across the entire range
		iCHigh = (rg.randomLongRange(1 + _TIdentShift, _TIdentShift
				+ ScaleParameters.customerCount) - 1) / 1000;

		if (fCW <= 200) {
			iCLow = (int) Math.ceil(Math.sqrt(22500 + 500 * fCW) - 151);
			customerTier = 1;
		} else if (fCW <= 1400) {
			iCLow = (int) Math.ceil(Math.sqrt(290000 + 1000 * fCW) - 501);
			customerTier = 2;
		} else {
			iCLow = (int) Math.ceil(Math.sqrt(500 * fCW - 277500));
			customerTier = 3;
		}

		long finalCustomerId = iCHigh * 1000 + Permute(iCLow, iCHigh) + 1;
		return customerMap.get(finalCustomerId);
	}

	private long Permute(long iLow, long iHigh) {
		return ((677 * iLow + 33 * (iHigh + 1)) % 1000);
	}

	public String getRandomCompanyName() {
		int companyIdx = rg.number(0, ScaleParameters.companyCount - 1);
		return companyNames.get(companyIdx);
	}

	// TODO
	public double getRandomRequestPrice() {

		return 0;
	}

	public boolean getRandomRollback() {
		// always return false
		return false;
	}

	public String getRandomSymbol() {
		int symbolIdx = rg.number(0, ScaleParameters.securityCount - 1);
		return securitySymbols.get(symbolIdx);
	}

	// TODO
	public int getRandomTradeQuantity() {

		return 0;
	}

	// TODO
	public String getRandomTradeType() {

		return "TMB";
	}

	public synchronized long getNextTradeId() {
		return (curTradeId++) * App.NUM_NODE + App.myNodeId;
	}

	public synchronized void addNewTrade(long tradeId, long customerId,
			long customerAccountId, long brokerId) {
		tradeList.add(new Trade(tradeId, customerId, customerAccountId,
				brokerId));
	}

	/**
	 * Get a random trade. And the returned trade will be removed.
	 */
	public synchronized Trade getOldestTrade() {
		if (tradeList.size() > 0)
			return tradeList.remove(0);
		else
			return null;
	}

	public class Trade {
		private long tradeId;
		private long customerId;
		private long customerAccountId;

		private long brokerId;

		public Trade(long tradeId, long customerId, long customerAccountId,
				long brokerId) {
			this.tradeId = tradeId;
			this.customerId = customerId;
			this.customerAccountId = customerAccountId;
			this.brokerId = brokerId;
		}

		public long getTradeId() {
			return tradeId;
		}

		public long getCustomerId() {
			return customerId;
		}

		public long getCustomerAccountId() {
			return customerAccountId;
		}

		public long getBrokerId() {
			return brokerId;
		}

	}

	public class Customer {
		private long customerId;
		private List<CustomerAccount> accountList = new ArrayList<CustomerAccount>();

		public Customer(long cId) {
			this.customerId = cId;
		}

		public void addAccount(CustomerAccount account) {
			accountList.add(account);
		}

		public CustomerAccount getRandomAccount() {

			return accountList.get(rg.number(0, accountList.size() - 1));
		}

		public long getCustomerId() {
			return customerId;
		}
	}

	public class CustomerAccount {
		private long accountId;
		private long brokerId;

		public CustomerAccount(long aId, long bId) {
			accountId = aId;
			brokerId = bId;
		}

		public long getAccountId() {
			return accountId;
		}

		public long getBrokerId() {
			return brokerId;
		}

	}

}
