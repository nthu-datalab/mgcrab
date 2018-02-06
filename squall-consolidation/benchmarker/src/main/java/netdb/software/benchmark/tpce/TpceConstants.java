package netdb.software.benchmark.tpce;


/** Holds TPC-C constants. */

public class TpceConstants {

	static {
		String prop = System.getProperty(TpceConstants.class.getName() + ".LOAD_UNIT");
		LOAD_UNIT = (prop == null) ? 1 : Integer.parseInt(prop
				.trim());
	}
	
	public final static int LOAD_UNIT;
	public final static int CUSTOMER_PER_LOADUNIT = 1000;
	public final static double ACCOUNT_CUSTOMER_FACTOR = 5;
	public final static double TRADE_CUSTOMER_FACTOR = 17280;
	public final static double BROKER_CUSTOMER_FACTOR = 0.01;
	public final static double COMPANY_CUSTOMER_FACTOR = 0.5;
	public final static double SECURITY_CUSTOMER_FACTOR = 0.685;

	public final static double TRADE_ROLLBACK_PERCENTAGE = 0.1;

	public final static String[] TRADE_TYPE_ELEMENT = {
			"'TLB','Limit-Buy',0,0", "'TLS','Limit-Sell',1,0",
			"'TMB','Market-Buy',0,1", "'TMS','Market-Sell',1,1",
			"'TSL','Stop-Loss',1,0" };
}
