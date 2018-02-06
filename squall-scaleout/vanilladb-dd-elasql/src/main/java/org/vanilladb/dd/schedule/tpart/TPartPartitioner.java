package org.vanilladb.dd.schedule.tpart;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.vanilladb.core.server.task.Task;
import org.vanilladb.dd.remote.groupcomm.StoredProcedureCall;
import org.vanilladb.dd.schedule.Scheduler;
import org.vanilladb.dd.schedule.tpart.sink.Sinker;
import org.vanilladb.dd.server.VanillaDdDb;
import org.vanilladb.dd.server.task.tpart.TPartStoredProcedureTask;
import org.vanilladb.dd.sql.RecordKey;
import org.vanilladb.dd.storage.metadata.PartitionMetaMgr;
import org.vanilladb.dd.storage.tx.recovery.DdRecoveryMgr;

public class TPartPartitioner extends Task implements Scheduler {
	public static final int NUM_PARTITIONS;

	public static CostFunctionCalculator costFuncCal;

	private static Logger logger = Logger.getLogger(TPartPartitioner.class
			.getName());

	private static final String FACTORY_CLS, COST_FUNC_CLS;

	private static final int NUM_TASK_PER_SINK;

	private static final int HAS_REORDERING;

	private List<TPartStoredProcedureTask> remoteTasks = new LinkedList<TPartStoredProcedureTask>();

	private TPartStoredProcedureFactory factory;

	private PartitionMetaMgr parMetaMgr = VanillaDdDb.partitionMetaMgr();

	static {
		String prop = System.getProperty(TPartPartitioner.class.getName()
				+ ".FACTORY_CLASS");

		if (prop != null && !prop.isEmpty())
			FACTORY_CLS = prop.trim();
		else
			FACTORY_CLS = "org.vanilladb.dd.schedule.tpart.TPartStoredProcedureFactory";

		prop = System.getProperty(TPartPartitioner.class.getName()
				+ ".NUM_PARTITIONS");

		NUM_PARTITIONS = prop == null ? 1 : Integer.valueOf(prop.trim());

		prop = System.getProperty(TPartPartitioner.class.getName()
				+ ".HAS_REORDERING");

		HAS_REORDERING = prop == null ? 0 : Integer.valueOf(prop.trim());

		prop = System.getProperty(TPartPartitioner.class.getName()
				+ ".COST_FUNC_CLS");

		if (prop != null && !prop.isEmpty())
			COST_FUNC_CLS = prop.trim();
		else
			COST_FUNC_CLS = "org.vanilladb.dd.schedule.tpart.CostFunctionCalculator";

		try {
			costFuncCal = (CostFunctionCalculator) Class.forName(COST_FUNC_CLS)
					.newInstance();
		} catch (Exception e) {
			e.printStackTrace();
		}

		prop = System.getProperty(TPartPartitioner.class.getName()
				+ ".NUM_TASK_PER_SINK");

		NUM_TASK_PER_SINK = prop == null ? 10 : Integer.valueOf(prop.trim());
	}

	private BlockingQueue<TPartStoredProcedureTask> taskQueue;
	private NodeInserter inserter;
	private Sinker sinker;
	private TGraph graph;

	public TPartPartitioner(NodeInserter inserter, Sinker sinker, TGraph graph) {
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

			TPartStoredProcedureTask spt;
			if (call.isNoOpStoredProcCall()) {
				spt = new TPartStoredProcedureTask(call.getClientId(),
						call.getRteId(), call.getTxNum(), null);
			} else {
				TPartStoredProcedure sp = factory.getStoredProcedure(
						call.getPid(), call.getTxNum());
				sp.prepare(call.getPars());
				sp.requestConservativeLocks();
				spt = new TPartStoredProcedureTask(call.getClientId(),
						call.getRteId(), call.getTxNum(), sp);

				if (!sp.isReadOnly())
					DdRecoveryMgr.logRequest(call);
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
				// blocked if the task queue is empty
				TPartStoredProcedureTask task = taskQueue.take();
				// schedules the utility procedures directly without T-Part
				// module
				if (task.getProcedureType() == TPartStoredProcedure.POPULATE
						|| task.getProcedureType() == TPartStoredProcedure.PRE_LOAD
						|| task.getProcedureType() == TPartStoredProcedure.PROFILE) {
					lastSunkTxNum = task.getTxNum();
					List<TPartStoredProcedureTask> list = new ArrayList<TPartStoredProcedureTask>();
					list.add(task);
					VanillaDdDb.tpartTaskScheduler().addTask(list.iterator());
					continue;
				}

				if (task.getProcedureType() == TPartStoredProcedure.KEY_ACCESS) {

					boolean isRemote = false;
					int lastPart = -1;
					for (RecordKey key : task.getReadSet()) {
						int thisPart = parMetaMgr.getPartition(key);
						if (lastPart != -1 && lastPart != thisPart) {
							isRemote = true;
							break;
						}
						lastPart = thisPart;
					}

					if (isRemote && HAS_REORDERING == 1) {
						remoteTasks.add(task);
					} else {
						Node node = new Node(task);
						inserter.insert(graph, node);
					}

				}

				insertedTxNum = task.getTxNum();
				// sink current t-graph if # pending tx exceeds threshold
				if (insertedTxNum == lastSunkTxNum + NUM_TASK_PER_SINK) {

					long starttime = System.currentTimeMillis();
					lastSunkTxNum = insertedTxNum;
					if (HAS_REORDERING == 1) {
						for (TPartStoredProcedureTask rtask : remoteTasks) {
							Node node = new Node(rtask);
							inserter.insert(graph, node);
						}
					}

					if (graph.getNodes().size() != 0) {
						Iterator<TPartStoredProcedureTask> plansTter = sinker
								.sink(graph);
						VanillaDdDb.tpartTaskScheduler().addTask(plansTter);
					}

					if (HAS_REORDERING == 1) {
						remoteTasks.clear();
					}
				}

			} catch (InterruptedException ex) {
				if (logger.isLoggable(Level.SEVERE))
					logger.severe("fail to dequeue task");
			}
		}
	}
}
