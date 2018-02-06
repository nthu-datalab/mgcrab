package netdb.software.benchmark.tpce.txnexecutor;

import netdb.software.benchmark.tpce.TpceParameterManager.Trade;
import netdb.software.benchmark.tpce.TransactionType;
import netdb.software.benchmark.tpce.remote.SutConnection;

public class TradeResultTxnExecutor extends TpceTxnExecutor {

	private Trade trade = null;

	public TradeResultTxnExecutor(SutConnection spc, Trade trade) {
		super(spc);
		this.trade = trade;
	}

	@Override
	public Object[] generateParameter() {
		Object[] params = new Object[4];
		int idxCntr = 0;

		params[idxCntr++] = trade.getTradeId();
		params[idxCntr++] = trade.getCustomerId();
		params[idxCntr++] = trade.getCustomerAccountId();
		params[idxCntr++] = trade.getBrokerId();

		return params;
	}

	@Override
	public TransactionType getTxnType() {
		return TransactionType.TRADE_RESULT;
	}

}
