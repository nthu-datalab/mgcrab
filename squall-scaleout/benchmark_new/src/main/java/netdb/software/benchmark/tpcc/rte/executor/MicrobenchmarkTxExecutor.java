package netdb.software.benchmark.tpcc.rte.executor;

import org.vanilladb.dd.storage.metadata.PartitionMetaMgr;

import netdb.software.benchmark.tpcc.TxnResultSet;
import netdb.software.benchmark.tpcc.remote.SutConnection;
import netdb.software.benchmark.tpcc.remote.SutResultSet;
import netdb.software.benchmark.tpcc.rte.txparamgen.TxParamGenerator;
import netdb.software.benchmark.tpcc.vanilladddb.migration.MicroMigrationManager;

public class MicrobenchmarkTxExecutor extends TransactionExecutor {
	
	private static NodeStatisticsRecorder recorder;
	private static long START_TIME = System.currentTimeMillis();
	
	static {
		recorder = new NodeStatisticsRecorder(PartitionMetaMgr.NUM_PARTITIONS, System.currentTimeMillis(),
				MicroMigrationManager.RECORD_PERIOD);
		recorder.start();
	}
	
	private int mainPartition;
	
	public MicrobenchmarkTxExecutor(TxParamGenerator pg, int mainPartition) {
		this.pg = pg;
		this.mainPartition = mainPartition;
	}

	public TxnResultSet execute(SutConnection conn) {
		try {
			TxnResultSet rs = new TxnResultSet();
			rs.setTxnType(pg.getTxnType());

			// generate parameters
			Object[] params = pg.generateParameter();

			// send txn request and start measure txn response time
			long txnRT = System.nanoTime();
			SutResultSet result = callStoredProc(conn, params);

			// measure txn response time
			txnRT = System.nanoTime() - txnRT;
			
			int sender = result.getSender();
			if (sender >= 0) {
				recorder.addTxResult(sender, txnRT / 1000);
			}

			// display output
			// System.out.println(pg.getTxnType() + " " + result.outputMsg());

			rs.setTxnIsCommited(result.isCommitted());
//			rs.setOutMsg(result.outputMsg());
			
			long elaspedTime = System.currentTimeMillis() - START_TIME;
			rs.setOutMsg(String.format("%d, %d, %d", elaspedTime, txnRT / 1000, sender));
			
			rs.setTxnResponseTimeNs(txnRT);

			return rs;
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e.getMessage());
		}
	}
}
