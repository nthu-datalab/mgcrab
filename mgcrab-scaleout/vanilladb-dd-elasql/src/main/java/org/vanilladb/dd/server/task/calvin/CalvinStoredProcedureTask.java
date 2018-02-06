package org.vanilladb.dd.server.task.calvin;

import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.util.Timer;
import org.vanilladb.core.util.TimerStatistics;
import org.vanilladb.dd.schedule.DdStoredProcedure;
import org.vanilladb.dd.schedule.calvin.CalvinStoredProcedure;
import org.vanilladb.dd.server.VanillaDdDb;
import org.vanilladb.dd.server.task.StoredProcedureTask;

public class CalvinStoredProcedureTask extends StoredProcedureTask {

	private CalvinStoredProcedure<?> csp;

	public static long txStartTime = 0;
	
	static {
//		TimerStatistics.startReporting();
		
		new Thread(new Runnable() {

			@Override
			public void run() {
				try {
					Thread.sleep(400_000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				
//				TimerStatistics.stopReporting();
			}
			
		}).start();
	}

	// private static int count = 0;
	// private static double latSum = 0; // in us.
	// private static ReentrantLock lock = new ReentrantLock();
	//
	// static {
	// new PeriodicalJob(3000, 500000, new Runnable() {
	//
	// @Override
	// public void run() {
	// lock.lock();
	//
	// if (count == 0)
	// System.out.println("Avg. Latency: 0 ms");
	// else {
	// latSum /= 1000; // to ms.
	// latSum /= count; // to average
	// System.out.println("Avg. Latency: " + latSum + " ms");
	// }
	//
	// count = 0;
	// latSum = 0;
	//
	// lock.unlock();
	// }
	//
	// }).start();
	// }
	//
	public static void touch() {

	}
	//
	// private static void recordLatency(double latency) {
	// lock.lock();
	//
	// count++;
	// latSum += latency;
	//
	// lock.unlock();
	// }

	public CalvinStoredProcedureTask(int cid, int rteId, long txNum, DdStoredProcedure sp) {
		super(cid, rteId, txNum, sp);

		csp = (CalvinStoredProcedure<?>) sp;
	}

	public void run() {

		if (txStartTime == 0)
			txStartTime = System.currentTimeMillis();

		Timer timer = Timer.getLocalTimer();
		SpResultSet rs = null;

		timer.reset();
		timer.startExecution();

		if (txNum % 10000 == 0)
			System.out.println("Tx." + txNum + " start executing.");

		try {
			rs = csp.execute();
		} finally {
			timer.stopExecution();
		}

		if (csp.isMasterNode() && cid != -1) {
			// System.out.println("Commit: " + (System.nanoTime() - startTime));
			VanillaDdDb.connectionMgr().sendClientResponse(cid, rteId, txNum, rs);
			// recordLatency(((double) latency / 1000));
		}

		/*
		 * XXX: this code is used in breakdown
		 * if(!csp.getClass().getName().equals(
		 * "netdb.software.benchmark.tpcc.procedure.vanilladddb.calvin.AsyncMigrateProc"
		 * )){ long now_time = System.currentTimeMillis() -
		 * CalvinStoredProcedureTask.txStartTime; long time_offset = 30*1000;
		 * long warm_up = 210*1000; long normal_time = 150*1000; long end_time =
		 * 1000*1000;
		 * 
		 * if(now_time >= normal_time && now_time < normal_time + time_offset){
		 * 
		 * Timers.reportTime("Normal Profile Start");
		 * 
		 * }
		 * 
		 * if(now_time >= warm_up && now_time < warm_up + time_offset){
		 * 
		 * Timers.reportTime("Migrate Starting Profile Start");
		 * 
		 * }
		 * 
		 * if(now_time >= end_time && now_time < end_time + time_offset){
		 * 
		 * Timers.reportTime("Migrate Ending Profile Start");
		 * 
		 * } }
		 */

		// For Debugging
		// System.out.println("Tx:" + txNum + "'s Timer:\n" + timer.toString());
		timer.addToGlobalStatistics();
	}

	public void lockConservatively() {
		csp.requestConservativeLocks();
	}
}
