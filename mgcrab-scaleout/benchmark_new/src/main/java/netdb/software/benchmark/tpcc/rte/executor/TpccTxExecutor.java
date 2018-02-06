package netdb.software.benchmark.tpcc.rte.executor;

import org.vanilladb.dd.storage.metadata.PartitionMetaMgr;

import netdb.software.benchmark.tpcc.TxnResultSet;
import netdb.software.benchmark.tpcc.remote.SutConnection;
import netdb.software.benchmark.tpcc.remote.SutResultSet;
import netdb.software.benchmark.tpcc.rte.TpccTxnExecutor;
import netdb.software.benchmark.tpcc.rte.txparamgen.tpcc.TpccTxParamGenerator;
import netdb.software.benchmark.tpcc.vanilladddb.migration.TpccMigrationManager;

public class TpccTxExecutor extends TransactionExecutor {
	
	protected final static boolean ENABLE_THINK_AND_KEYING_TIME;
	
	private static final long START_TIME = System.currentTimeMillis();
	private static NodeStatisticsRecorder recorder;
	
	static {
		String prop = System.getProperty(TpccTxnExecutor.class.getName()
				+ ".ENABLE_THINK_AND_KEYING_TIME");
		ENABLE_THINK_AND_KEYING_TIME = (prop == null) ? false : Boolean
				.parseBoolean(prop.trim());
		
		recorder = new NodeStatisticsRecorder(PartitionMetaMgr.NUM_PARTITIONS, System.currentTimeMillis(),
				TpccMigrationManager.RECORD_PERIOD);
		recorder.start();
	}

	private TpccTxParamGenerator tpccPg;

	public TpccTxExecutor(TpccTxParamGenerator pg) {
		this.pg = pg;
		tpccPg = pg;
	}

	@Override
	public TxnResultSet execute(SutConnection conn) {
		try {
			TxnResultSet rs = new TxnResultSet();
			rs.setTxnType(pg.getTxnType());

			// keying
			if (ENABLE_THINK_AND_KEYING_TIME) {
				// wait for a keying time and generate parameters
				long t = tpccPg.getKeyingTime();
				rs.setKeyingTime(t);
				Thread.sleep(t);
			} else
				rs.setKeyingTime(0);

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
			
			rs.setCommitTime(System.currentTimeMillis() - START_TIME);
			rs.setTxnIsCommited(result.isCommitted());
//			rs.setOutMsg(result.outputMsg());
			
			long elaspedTime = System.currentTimeMillis() - START_TIME;
			rs.setOutMsg(String.format("%d, %d, %d", elaspedTime, txnRT / 1000,	sender));
			
			rs.setTxnResponseTimeNs(txnRT);

			// thinking
			if (ENABLE_THINK_AND_KEYING_TIME) {
				// wait for a think time
				long t = tpccPg.getThinkTime();
				Thread.sleep(t);
				rs.setThinkTime(t);
			} else
				rs.setThinkTime(0);

			return rs;
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e.getMessage());
		}
	}

}
