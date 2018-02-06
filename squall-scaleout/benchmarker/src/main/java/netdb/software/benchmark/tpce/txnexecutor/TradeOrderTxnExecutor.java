package netdb.software.benchmark.tpce.txnexecutor;

import netdb.software.benchmark.tpce.TpceParameterManager.Customer;
import netdb.software.benchmark.tpce.TpceParameterManager.CustomerAccount;
import netdb.software.benchmark.tpce.TpceWorkload;
import netdb.software.benchmark.tpce.TransactionType;
import netdb.software.benchmark.tpce.remote.SutConnection;

public class TradeOrderTxnExecutor extends TpceTxnExecutor {

	private long tradeId;
	private long customerId;
	private long customerAccountId;
	private long brokerId;

	public TradeOrderTxnExecutor(SutConnection spc) {
		super(spc);
	}

	@Override
	public Object[] generateParameter() {

		Object[] params = new Object[10];
		int idx = 0;

		// Non-uniformly random a customer
		Customer customer = TpceWorkload.paramGen.getNonUniformRandomCustomer();
		CustomerAccount account = customer.getRandomAccount();
		params[idx++] = customerAccountId = account.getAccountId();
		params[idx++] = customerId = customer.getCustomerId();
		params[idx++] = brokerId = account.getBrokerId();
		params[idx++] = TpceWorkload.paramGen.getRandomCompanyName();
		params[idx++] = TpceWorkload.paramGen.getRandomRequestPrice();
		params[idx++] = TpceWorkload.paramGen.getRandomRollback();
		params[idx++] = TpceWorkload.paramGen.getRandomSymbol();
		params[idx++] = TpceWorkload.paramGen.getRandomTradeQuantity();
		params[idx++] = TpceWorkload.paramGen.getRandomTradeType();
		params[idx++] = tradeId = TpceWorkload.paramGen.getNextTradeId();

		return params;
	}

	@Override
	public TransactionType getTxnType() {
		return TransactionType.TRADE_ORDER;
	}

	@Override
	public void onResponseReceived() {
		TpceWorkload.paramGen.addNewTrade(tradeId, customerId,
				customerAccountId, brokerId);
	}

}
