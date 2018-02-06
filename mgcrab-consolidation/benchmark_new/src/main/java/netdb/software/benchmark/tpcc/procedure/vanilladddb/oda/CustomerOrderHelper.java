package netdb.software.benchmark.tpcc.procedure.vanilladddb.oda;

import java.util.HashMap;
import java.util.Map;

import netdb.software.benchmark.tpcc.TpccConstants;

public class CustomerOrderHelper {

	public static Map<Integer, Integer> customerLatestOrder = new HashMap<Integer, Integer>();

	static {
		for (int i = 1; i <= TpccConstants.CUSTOMERS_PER_DISTRICT; i++) {
			customerLatestOrder.put(i, i);
		}
	}

	public static void addNewOrder(int cid, int oid) {
		customerLatestOrder.put(cid, oid);
	}

	public static int getCustomerLatestOrder(int cid) {
		int oid = customerLatestOrder.get(cid);

		return oid;
	}

}
