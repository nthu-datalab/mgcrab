package org.vanilladb.dd.server.task.calvin;

import java.io.BufferedWriter;
import java.util.concurrent.locks.ReentrantLock;

import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.dd.schedule.DdStoredProcedure;
import org.vanilladb.dd.schedule.calvin.CalvinStoredProcedure;
import org.vanilladb.dd.server.VanillaDdDb;
import org.vanilladb.dd.server.task.StoredProcedureTask;
import org.vanilladb.dd.util.PeriodicalJob;

public class CalvinStoredProcedureTask extends StoredProcedureTask {

	private CalvinStoredProcedure<?> csp;

	private static BufferedWriter output;

	public static long txStartTime = 0;

	private static int count = 0;
	private static double latSum = 0; // in us.
	private static ReentrantLock lock = new ReentrantLock();
	
	static {
//		new PeriodicalJob(3000, 500000, new Runnable() {
//
//			@Override
//			public void run() {
//				lock.lock();
//				
//				if (count == 0)
//					System.out.println("Avg. Latency: 0 ms");
//				else {
//					latSum /= 1000; // to ms.
//					latSum /= count; // to average
//					System.out.println("Avg. Latency: " + latSum + " ms");
//				}
//				
//				count = 0;
//				latSum = 0;
//				
//				lock.unlock();
//			}
//			
//		}).start();
	}
//	
	public static void touch() {
		
	}
//	
	private static void recordLatency(double latency) {
		lock.lock();

		count++;
		latSum += latency;
		
		lock.unlock();
	}

	public CalvinStoredProcedureTask(int cid, int rteId, long txNum,
			DdStoredProcedure sp) {
		super(cid, rteId, txNum, sp);

		csp = (CalvinStoredProcedure<?>) sp;
	}

	public void run() {
		
		if (txStartTime == 0)
			txStartTime = System.currentTimeMillis();
		
//		 Timers.createTimer(txNum);
		SpResultSet rs = null;
//		 Timers.getTimer().startExecution();
		
		if (txNum % 10000 == 0)
			System.out.println("Tx." + txNum + " start executing.");

//		 try {
//		long start = System.nanoTime();
		rs = csp.execute();
//		long latency = System.nanoTime() - start;
		
//		 } finally {
//		 Timers.getTimer().stopExecution();
//		 }

		if (csp.isMasterNode() && cid != -1) {
			// System.out.println("Commit: " + (System.nanoTime() - startTime));
			VanillaDdDb.connectionMgr().sendClientResponse(cid, rteId, txNum,
					rs);
//			recordLatency(((double) latency / 1000));
		}
		
//		if (Timers.getTimer().getExecutionTime() > 100000)
//			Timers.reportTime();
	}

	public void lockConservatively() {
		csp.requestConservativeLocks();
	}
}
