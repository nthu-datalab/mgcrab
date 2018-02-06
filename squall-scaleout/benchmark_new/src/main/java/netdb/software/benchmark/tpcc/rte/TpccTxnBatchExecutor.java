package netdb.software.benchmark.tpcc.rte;

import java.util.List;

import netdb.software.benchmark.tpcc.TransactionType;
import netdb.software.benchmark.tpcc.TxnResultSet;
import netdb.software.benchmark.tpcc.remote.SutConnection;

public class TpccTxnBatchExecutor {

	public static TxnResultSet[] executeBatch(SutConnection conn,
			TransactionType[] types, List<Object[]> pars, long keyingTime,
			long thinkingTime) {
		TxnResultSet[] result = new TxnResultSet[types.length];
		try {
			if (TpccTxnExecutor.ENABLE_THINK_AND_KEYING_TIME) {
				// wait for an average keying time
				Thread.sleep(keyingTime);
			}

			// int[] pids = new int[types.length];
			// for (int i = 0; i < types.length; i++)
			// pids[i] = types[i].pid();
			//
			// // send batch request and start measure txn response time
			// long singleTxnRT = System.nanoTime();
			// SutResultSet[] remoteRs = conn.callBatchedStoredProc(pids, pars);
			//
			// // measure avg single txn response time
			// singleTxnRT = (System.nanoTime() - singleTxnRT) / types.length;
			//
			// for (int i = 0; i < types.length; i++) {
			// TxnResultSet rs = new TxnResultSet();
			// result[i] = rs;
			// rs.setTxnType(types[i]);
			// if (TpccTxnExecutor.ENABLE_THINK_AND_KEYING_TIME) {
			// rs.setKeyingTime(keyingTime);
			// rs.setThinkTime(thinkingTime);
			// } else {
			// rs.setKeyingTime(0);
			// rs.setThinkTime(0);
			// }
			//
			// // display output
			// System.out.println(types[i] + " " + remoteRs[i].outputMsg());
			//
			// rs.setTxnIsCommited(remoteRs[i].isCommitted());
			// rs.setOutMsg(remoteRs[i].outputMsg());
			// rs.setTxnResponseTimeNs(singleTxnRT);
			// }

			if (TpccTxnExecutor.ENABLE_THINK_AND_KEYING_TIME) {
				// wait for an average think time
				Thread.sleep(thinkingTime);
			}
			return result;
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e.getMessage());
		}
	}
}