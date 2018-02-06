package netdb.software.benchmark;

import java.util.ArrayList;
import java.util.List;

public abstract class Benchmark {

	protected Workload workload;
	protected List<WorkloadOperator> operators;

	public Benchmark() {
		operators = new ArrayList<WorkloadOperator>();
	}

	/**
	 * Initialize the workload and the workload operators.
	 */
	public abstract void initialize();

	public abstract void load();

	public abstract void fullTableScan();

	public abstract void start();

	public abstract void report();
	
	public abstract void startProfiling();
	public abstract void stopProfiling();

}
