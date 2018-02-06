package netdb.software.benchmark.tpce;

/** Stores the scaling parameters for loading and running. */
public class ScaleParameters {

	public final static int customerCount;
	public final static int customerAccoutCount;
	public final static int brokerCount;
	public final static int companyCount;
	public final static int securityCount;

	static {

		// initialize the number of element in each table
		customerCount = TpceConstants.LOAD_UNIT
				* TpceConstants.CUSTOMER_PER_LOADUNIT;
		customerAccoutCount = (int) (customerCount * TpceConstants.ACCOUNT_CUSTOMER_FACTOR);
		brokerCount = (int) (customerCount * TpceConstants.BROKER_CUSTOMER_FACTOR);
		companyCount = (int) (customerCount * TpceConstants.COMPANY_CUSTOMER_FACTOR);
		securityCount = (int) (customerCount * TpceConstants.SECURITY_CUSTOMER_FACTOR);

	}

}
