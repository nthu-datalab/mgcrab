package org.vanilladb.dd.server.task.calvin;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.dd.schedule.DdStoredProcedure;
import org.vanilladb.dd.schedule.calvin.CalvinStoredProcedure;
import org.vanilladb.dd.server.VanillaDdDb;
import org.vanilladb.dd.server.task.StoredProcedureTask;

public class CalvinStoredProcedureTask extends StoredProcedureTask {

	private CalvinStoredProcedure<?> csp;
	
	private static BufferedWriter output;
	
	static {
		// Open the file
		try {
			output = new BufferedWriter(new FileWriter("commit_log_" + VanillaDdDb.serverId()));
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		// Use another thread to close the file long time after
		new Thread(new Runnable() {

			@Override
			public void run() {
				try {
					Thread.sleep(300000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				
				closeFile();
			}
			
		}).start();
	}
	
	private synchronized static void closeFile() {
		try {
			output.close();
			output = null;
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private synchronized static void recordLatency(double commitTime, double latency) {
		try {
			if (output != null) {
				output.append(commitTime + ", " + latency);
				output.newLine();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public CalvinStoredProcedureTask(int cid, int rteId, long txNum,
			DdStoredProcedure sp) {
		super(cid, rteId, txNum, sp);

		csp = (CalvinStoredProcedure<?>) sp;
	}

	public void run() {
		// Timers.createTimer(txNum);
		SpResultSet rs = null;
		// Timers.getTimer().startExecution();
		
		if (txNum % 10000 == 0)
			System.out.println("Tx." + txNum + " start executing.");

		// try {
		long start = System.nanoTime();
		rs = csp.execute();
		long latency = System.nanoTime() - start;
		recordLatency(((double) (System.nanoTime() - VanillaDdDb.START_TIME) / 1000000), ((double) latency / 1000000));
		// } finally {
		// Timers.getTimer().stopExecution();
		// }

		if (csp.isMasterNode() && cid != -1) {
			// System.out.println("Commit: " + (System.nanoTime() - startTime));
			VanillaDdDb.connectionMgr().sendClientResponse(cid, rteId, txNum,
					rs);
		}

		// Timers.reportTime();
	}

	public void lockConservatively() {
		csp.requestConservativeLocks();
	}
}
