package netdb.software.benchmark;

public abstract class WorkloadOperator extends Thread {
	protected Workload workload;

	public WorkloadOperator(Workload workload) {
		this.workload = workload;
	}
}
