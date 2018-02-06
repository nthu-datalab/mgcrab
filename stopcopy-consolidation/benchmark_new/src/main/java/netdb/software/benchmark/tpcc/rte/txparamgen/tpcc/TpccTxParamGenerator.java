package netdb.software.benchmark.tpcc.rte.txparamgen.tpcc;

import netdb.software.benchmark.tpcc.TransactionType;
import netdb.software.benchmark.tpcc.rte.txparamgen.TxParamGenerator;

public abstract class TpccTxParamGenerator implements TxParamGenerator {

	public abstract long getKeyingTime();

	public abstract long getThinkTime();

	public static TpccTxParamGenerator getParamGen(TransactionType type,
			int homeWid) {
		TpccTxParamGenerator pg = null;
		switch (type) {
		case DELIVERY:
			pg = new TpccDeliveryParamGen(homeWid);
			break;
		case NEW_ORDER:
			pg = new TpccNewOrderParamGen(homeWid);
			break;
		case ORDER_STATUS:
			pg = new TpccOrderStatusParamGen(homeWid);
			break;
		case PAYMENT:
			pg = new TpccPaymentParamGen(homeWid);
			break;
		case STOCK_LEVEL:
			pg = new TpccStockLevelParamGen(homeWid);
			break;
		default:
			break;

		}

		return pg;
	}

}
