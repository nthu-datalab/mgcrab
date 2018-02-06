package org.vanilladb.dd.schedule.tpart.replication;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.vanilladb.core.server.task.Task;
import org.vanilladb.dd.remote.groupcomm.StoredProcedureCall;
import org.vanilladb.dd.schedule.Scheduler;
import org.vanilladb.dd.schedule.tpart.TPartStoredProcedure;
import org.vanilladb.dd.schedule.tpart.TPartStoredProcedureFactory;
import org.vanilladb.dd.schedule.tpart.sink.ReplicationSinker;
import org.vanilladb.dd.server.VanillaDdDb;
import org.vanilladb.dd.server.task.tpart.TPartStoredProcedureTask;
import org.vanilladb.dd.storage.tx.recovery.DdRecoveryMgr;

public class ReplicatedTPartPartitioner extends Task implements Scheduler {
	public static final int NUM_PARTITIONS;

	public static final int NUM_REPLICATIONS;

	public static ReplicatedCostFunctionCalculator costFuncCal;

	private static Logger logger = Logger
			.getLogger(ReplicatedTPartPartitioner.class.getName());

	private static final String FACTORY_CLS, COST_FUNC_CLS;

	private static final int NUM_TASK_PER_SINK;

	private TPartStoredProcedureFactory factory;

	static {
		String prop = System.getProperty(ReplicatedTPartPartitioner.class
				.getName() + ".FACTORY_CLASS");

		if (prop != null && !prop.isEmpty())
			FACTORY_CLS = prop.trim();
		else
			FACTORY_CLS = "org.vanilladb.dd.schedule.tpart.TPartStoredProcedureFactory";

		prop = System.getProperty(ReplicatedTPartPartitioner.class.getName()
				+ ".NUM_PARTITIONS");

		NUM_PARTITIONS = prop == null ? 1 : Integer.valueOf(prop.trim());

		prop = System.getProperty(ReplicatedTPartPartitioner.class.getName()
				+ ".NUM_REPLICATIONS");

		NUM_REPLICATIONS = prop == null ? 1 : Integer.valueOf(prop.trim());

		prop = System.getProperty(ReplicatedTPartPartitioner.class.getName()
				+ ".COST_FUNC_CLS");

		if (prop != null && !prop.isEmpty())
			COST_FUNC_CLS = prop.trim();
		else
			COST_FUNC_CLS = "org.vanilladb.dd.schedule.tpart.replication.ReplicatedCostFunctionCalculator";

		try {
			costFuncCal = (ReplicatedCostFunctionCalculator) Class.forName(
					COST_FUNC_CLS).newInstance();
		} catch (Exception e) {
			e.printStackTrace();
		}

		prop = System.getProperty(ReplicatedTPartPartitioner.class.getName()
				+ ".NUM_TASK_PER_SINK");

		NUM_TASK_PER_SINK = prop == null ? 10 : Integer.valueOf(prop.trim());
	}

	private BlockingQueue<TPartStoredProcedureTask> taskQueue;
	private ReplicatedNodeInserter inserter;
	private ReplicationSinker sinker;
	private ReplicatedTGraph graph;

	public ReplicatedTPartPartitioner(ReplicatedNodeInserter inserter,
			ReplicationSinker sinker, ReplicatedTGraph graph) {
		this.inserter = inserter;
		this.sinker = sinker;
		this.graph = graph;
		taskQueue = new LinkedBlockingQueue<TPartStoredProcedureTask>();

		Class<?> c;
		try {
			c = Class.forName(FACTORY_CLS);
			if (c != null)
				factory = (TPartStoredProcedureFactory) c.newInstance();
		} catch (Exception e) {
			if (logger.isLoggable(Level.WARNING))
				logger.warning("no factory class found, using default");
		}

		if (factory == null) {
			factory = new TPartStoredProcedureFactory();
		}
	}

	public void schedule(StoredProcedureCall... calls) {
		for (int i = 0; i < calls.length; i++) {
			StoredProcedureCall call = calls[i];
			// log request
			DdRecoveryMgr.logRequest(call);

			TPartStoredProcedureTask spt;
			if (call.isNoOpStoredProcCall()) {
				spt = new TPartStoredProcedureTask(call.getClientId(),
						call.getTxNum(), null);
			} else {
				TPartStoredProcedure sp = factory.getStoredProcedure(
						call.getPid(), call.getTxNum());
				sp.prepare(call.getPars());
				spt = new TPartStoredProcedureTask(call.getClientId(),
						call.getTxNum(), sp);
			}
			try {
				taskQueue.put(spt);
			} catch (InterruptedException ex) {
				if (logger.isLoggable(Level.SEVERE))
					logger.severe("fail to insert task to queue");
			}
		}
	}

	public void run() {
		long insertedTxNum, lastSunkTxNum = -1;
		while (true) {
			try {

				TPartStoredProcedureTask task = taskQueue.take();
				// long w = System.currentTimeMillis();

				System.out.println("insert......." + task.getTxNum());
				if (task.getProcedureType() == TPartStoredProcedure.POPULATE
						|| task.getProcedureType() == TPartStoredProcedure.PRE_LOAD
						|| task.getProcedureType() == TPartStoredProcedure.PROFILE) {
					lastSunkTxNum = task.getTxNum();
					List<TPartStoredProcedureTask> list = new ArrayList<TPartStoredProcedureTask>();
					list.add(task);
					VanillaDdDb.tPartScheduler().addTask(list.iterator());
					continue;
				}

				if (task.getProcedureType() == TPartStoredProcedure.KEY_ACCESS) {
					ReplicatedNode node = new ReplicatedNode(task);
					System.out.println("berfore insert......."
							+ task.getTxNum());
					inserter.insert(graph, node);
					System.out.println("fin insert......." + task.getTxNum());
				}

				insertedTxNum = task.getTxNum();
				// w = System.currentTimeMillis() - w;
				// System.out.println("insert time:" + w);
				// sink current t-graph if # pending tx exceeds threshold
				if (insertedTxNum == lastSunkTxNum + NUM_TASK_PER_SINK) {

					// w = System.currentTimeMillis();
					// XXX sink all or sink partial?
					System.out.println("sink.......");
					lastSunkTxNum = insertedTxNum;
					if (graph.getNodes().size() != 0) {
						Iterator<TPartStoredProcedureTask> plansTter = sinker
								.sink(graph);
						VanillaDdDb.tPartScheduler().addTask(plansTter);
					}
					// w = System.currentTimeMillis() - w;
					// System.out.println("sink time:" + w);
				}
			} catch (InterruptedException ex) {
				if (logger.isLoggable(Level.SEVERE))
					logger.severe("fail to dequeue task");
			}
		}
	}
}
