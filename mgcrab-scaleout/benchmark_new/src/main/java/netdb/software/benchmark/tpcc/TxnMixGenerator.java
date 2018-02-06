package netdb.software.benchmark.tpcc;

import java.util.Random;

public class TxnMixGenerator {
	private static Random generator = new Random();

	public TxnMixGenerator() {

	}

	public static TransactionType nextTransactionType() {

		int index = generator.nextInt(TpccConstants.FREQUENCY_TOTAL);
		if (index < TpccConstants.RANGE_NEW_ORDER)
			return TransactionType.NEW_ORDER;
		else if (index < TpccConstants.RANGE_PAYMENT)
			return TransactionType.PAYMENT;
		else if (index < TpccConstants.RANGE_ORDER_STATUS)
			return TransactionType.ORDER_STATUS;
		else if (index < TpccConstants.RANGE_DELIVERY)
			return TransactionType.DELIVERY;
		else
			return TransactionType.STOCK_LEVEL;
	}
}
