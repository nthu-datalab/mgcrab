package netdb.software.benchmark.tpcc.remote;

public interface SutResultSet {
	
	int getSender();
	
	boolean isCommitted();

	String outputMsg();
}
