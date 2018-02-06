package netdb.software.benchmark.tpcc.rte.txparamgen;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.vanilladb.comm.client.ClientAppl;

import netdb.software.benchmark.tpcc.App;
import netdb.software.benchmark.tpcc.TpccConstants;
import netdb.software.benchmark.tpcc.TransactionType;
import netdb.software.benchmark.tpcc.util.RandomValueGenerator;
import netdb.software.benchmark.tpcc.util.YcsbLatestGenerator;

public class YcsbParamGen implements TxParamGenerator {
	
	private static final double RW_TX_RATE = 0.15;
	private static final double SKEW_PARAMETER = 0.5;
	
	private static final AtomicInteger[] GLOBAL_COUNTERS;
	
	static {
		if (ClientAppl.CLIENT_COUNT == -1)
			throw new RuntimeException("it's -1 !!!!");
		
		GLOBAL_COUNTERS = new AtomicInteger[ClientAppl.CLIENT_COUNT];
		for (int i = 0; i < ClientAppl.CLIENT_COUNT; i++)
			GLOBAL_COUNTERS[i] = new AtomicInteger(0);
	}
	
	private static int getNextInsertId(int partitionId) {
		int id = GLOBAL_COUNTERS[partitionId].getAndIncrement();
		
		return id * ClientAppl.CLIENT_COUNT + App.myNodeId
				+ getStartId(partitionId) + getRecordCount(partitionId);
	}
	
	private static int getStartId(int partitionId) {
		return partitionId * TpccConstants.YCSB_MAX_RECORD_PER_PART + 1;
	}
	
	private static int getRecordCount(int partitionId) {
//		int recordPerPart = TpccConstants.YCSB_RECORD_PER_PART;
//		int migrationSize = (int) (MicroMigrationManager.MIGRATE_PERCENTAGE * recordPerPart);
//		
//		// XXX: Hard code node ids
//		if (partitionId == 2)
//			return migrationSize;
//		else if (partitionId == 1)
//			return recordPerPart - migrationSize;
//		else
//			return recordPerPart;
		
		return TpccConstants.YCSB_RECORD_PER_PART;
	}
	
	private int mainPart;
	private YcsbLatestGenerator latestRandom;
	private Random uniformRandom = new Random();
	private RandomValueGenerator rvg = new RandomValueGenerator();
	
	private int partitionStartId, partitionSize;
	
	public static void main(String[] args) {
		int rteCount = 100;
		int eachRun = 10;
		YcsbParamGen[] rtes = new YcsbParamGen[rteCount];
		YcsbLatestGenerator rteMainPartitionRandom = new YcsbLatestGenerator(3, 0.9);
		
		// Create RTEs
		for (int i = 0; i < rteCount; i++) {
			rtes[i] = new YcsbParamGen((int) rteMainPartitionRandom.nextValue() - 1);
		}
		
		// Generate parameters
		for (int i = 0; i < eachRun; i++) {
//			for (int rteI = 0; rteI < rteCount; rteI++) {
				System.out.println(Arrays.toString(rtes[0].generateParameter()));
//				System.out.println(rtes[rteI].generateParameter()[1]);
//			}
		}
	}
	
	public YcsbParamGen(int mainPartition) {
		mainPart = mainPartition;
		partitionStartId = getStartId(mainPartition);
		partitionSize = getRecordCount(mainPartition);
		latestRandom = new YcsbLatestGenerator(partitionSize, SKEW_PARAMETER);
	}
	
	@Override
	public TransactionType getTxnType() {
		return TransactionType.YCSB;
	}

	@Override
	public Object[] generateParameter() {
		// Move to the next partition
//		mainPart = (mainPart + 1) % 3;
//		partitionStartId = getStartId(mainPart);
		
		List<Object> params = new LinkedList<Object>();
		
		// Decide the type of the transaction
		boolean isReadWriteTx = (rvg.randomChooseFromDistribution(RW_TX_RATE,
				1 - RW_TX_RATE) == 0) ? true : false;
		
		if (isReadWriteTx) {
			int readWriteId = chooseARecordInMainPartition();
			int insertId = getNextInsertId(mainPart);
//			int readWriteId = uniformlyChooseARecord();
//			int insertId = uniformlyChooseAnInsertId();
			
			// Read count
			params.add(1);
			
			// Read ids (in integer)
			params.add(readWriteId);
			
			// Write count
			params.add(1);
			
			// Write ids (in integer)
			params.add(readWriteId);
			
			// Write values
			params.add(rvg.randomAString(TpccConstants.YCSB_CHARS_PER_FIELD));
			
			// Insert count
			params.add(1);
			
			// Insert ids (in integer)
			params.add(insertId);
			
			// Insert values
			params.add(rvg.randomAString(TpccConstants.YCSB_CHARS_PER_FIELD));
			
		} else {
			int rec1Id = chooseARecordInMainPartition();
			int rec2Id = rec1Id;
			while (rec1Id == rec2Id)
				rec2Id = chooseARecordInMainPartition();
			
//			int rec1Id = uniformlyChooseARecord();
//			int rec2Id = rec1Id;
//			while (rec1Id == rec2Id)
//				rec2Id = uniformlyChooseARecord();
			
			// Read count
			params.add(2);
			
			// Read ids (in integer)
			params.add(rec1Id);
			params.add(rec2Id);
			
			// Write count
			params.add(0);
			
			// Insert count
			params.add(0);
		}
		
		return params.toArray(new Object[0]);
	}
	
	private int chooseARecordInMainPartition() {
		return (int) latestRandom.nextValue() + partitionStartId - 1;
	}
	
	private int uniformlyChooseARecord() {
		int partId = uniformRandom.nextInt(3);
		return getStartId(partId) + uniformRandom.nextInt(getRecordCount(partId));
	}
	
	private int uniformlyChooseAnInsertId() {
		int partId = uniformRandom.nextInt(3);
		return getNextInsertId(partId);
	}

//	private int chooseARecordInUniformRandom() {
//		return uniformRandom.nextInt(TpccConstants.YCSB_NUM_RECORDS) + 1;
//	}
//	
//	private int chooseARecordInLatestRandom() {
//		return (int) latestRandom.nextValue();
//	}
}
