package netdb.software.benchmark.tpcc.rte.txparamgen;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import netdb.software.benchmark.tpcc.TpccConstants;
import netdb.software.benchmark.tpcc.TransactionType;
import netdb.software.benchmark.tpcc.remote.SutConnection;
import netdb.software.benchmark.tpcc.rte.MicroBenchmarkTxnExecutor;
import netdb.software.benchmark.tpcc.util.RandomNonRepeatGenerator;
import netdb.software.benchmark.tpcc.util.RandomValueGenerator;
import netdb.software.benchmark.tpcc.vanilladddb.migration.MicroMigrationManager;

public class MicrobenchmarkParamGen implements TxParamGenerator {
	// private static final double LOCALITY;
	// private static final double HOTNESS;
	private static final double REMOTE_RATE;
	private static final int REMOTE_HOT_COUNT;
	private static final int REMOTE_COLD_COUNT;
	private static final double WRITE_PERCENTAGE;
	private static final double CONFLICT_RATE;
	private static final double SKEWNESS;
	private static final double SKEW_PERCENTAGE;
	private static final double LONG_READ_PERCENTAGE;

	private static final int PARTITION_NUM;
	// private static double[] SKEWNESS_DISTRIBUTION;
	private static final int DATA_SIZE_PER_PART;
	private static final int HOT_DATA_SIZE_PER_PART;
	private static final int COLD_DATA_SIZE_PER_PART;
	private static final int COLD_DATA_PER_TX = 9;
	private static final int HOT_DATA_PER_TX = 1;

	private static final int USER_COUNT;
	private static final int DATA_PER_USER;
	private static final int USER_SESSION_PERIOD = 50;

	private static Map<Integer, Integer> itemRandomMap;
	static {

		String prop = null;

		prop = System.getProperty(MicroBenchmarkTxnExecutor.class.getName()
				+ ".CONFLICT_RATE");
		CONFLICT_RATE = (prop == null ? 0.01 : Double.parseDouble(prop.trim()));

		prop = System.getProperty(MicroBenchmarkTxnExecutor.class.getName()
				+ ".SKEWNESS");
		SKEWNESS = (prop == null ? 1 : Double.parseDouble(prop.trim()));

		prop = System.getProperty(MicroBenchmarkTxnExecutor.class.getName()
				+ ".REMOTE_RATE");
		REMOTE_RATE = (prop == null ? 0 : Double.parseDouble(prop.trim()));

		prop = System.getProperty(MicroBenchmarkTxnExecutor.class.getName()
				+ ".REMOTE_HOT_COUNT");
		REMOTE_HOT_COUNT = (prop == null ? 1 : Integer.parseInt(prop.trim()));

		prop = System.getProperty(MicroBenchmarkTxnExecutor.class.getName()
				+ ".REMOTE_COLD_COUNT");
		REMOTE_COLD_COUNT = (prop == null ? 0 : Integer.parseInt(prop.trim()));

		prop = System.getProperty(MicroBenchmarkTxnExecutor.class.getName()
				+ ".WRITE_PERCENTAGE");
		WRITE_PERCENTAGE = (prop == null ? 0.2 : Double
				.parseDouble(prop.trim()));

		prop = System.getProperty(MicroBenchmarkTxnExecutor.class.getName()
				+ ".PARTITION_NUM");
		PARTITION_NUM = (prop == null ? 1 : Integer.parseInt(prop.trim()));

		prop = System.getProperty(MicroBenchmarkTxnExecutor.class.getName()
				+ ".LONG_READ_PERCENTAGE");
		LONG_READ_PERCENTAGE = (prop == null ? 0 : Double.parseDouble(prop
				.trim()));

		DATA_SIZE_PER_PART = TpccConstants.NUM_ITEMS / PARTITION_NUM;
		HOT_DATA_SIZE_PER_PART = (int) (1.0 / CONFLICT_RATE);
		COLD_DATA_SIZE_PER_PART = DATA_SIZE_PER_PART - HOT_DATA_SIZE_PER_PART;
		DATA_PER_USER = COLD_DATA_SIZE_PER_PART; // XXX quick fix to disable
													// user session
		USER_COUNT = COLD_DATA_SIZE_PER_PART / DATA_PER_USER;
		SKEW_PERCENTAGE = 0.2;

		// initialize random item mapping map
		itemRandomMap = new HashMap<Integer, Integer>(TpccConstants.NUM_ITEMS);

		RandomNonRepeatGenerator rg = new RandomNonRepeatGenerator(
				TpccConstants.NUM_ITEMS);

		for (int i = 1; i <= TpccConstants.NUM_ITEMS; i++)
			itemRandomMap.put(i, i);

	}

	private SutConnection spc;
	private Object[] params;
	private int[] lastWindowOffset = new int[PARTITION_NUM];
	private int sessionUser = 0;
	private int sessionCountDown = -1;
	private long startTime = System.currentTimeMillis();
	private int wid;

	public MicrobenchmarkParamGen() {

	}

	@Override
	public TransactionType getTxnType() {
		return TransactionType.MICRO_BENCHMARK;
	}

	// a main application for debugging
	public static void main(String[] args) {
		MicrobenchmarkParamGen executor = new MicrobenchmarkParamGen();

		System.out.println("Parameters:");
		System.out.println("Remote Rate: " + REMOTE_RATE);
		System.out.println("Remote Hot Count: " + REMOTE_HOT_COUNT);
		System.out.println("Remote Cold Count: " + REMOTE_COLD_COUNT);
		System.out.println("Write Rate: " + WRITE_PERCENTAGE);
		System.out.println("Conflict Rate: " + CONFLICT_RATE);
		System.out.println("Skewness: " + SKEWNESS);
		System.out.println("Partition Number: " + PARTITION_NUM);
		System.out.println("Long Read percentage: " + LONG_READ_PERCENTAGE);
		System.out.println("User Count: " + USER_COUNT);
		System.out.println("User Data Size: " + DATA_PER_USER);

		System.out.println();

		for (int i = 0; i < 1000; i++)
			System.out.println(Arrays.toString(executor.generateParameter()) + "\n");
	}

	@Override
	public Object[] generateParameter() {
		RandomValueGenerator rvg = new RandomValueGenerator();
		LinkedList<Object> paramList = new LinkedList<Object>();

		updateSessionUser(rvg);
		// System.out.println("sessionUser: "+sessionUser);
		// decide if this txn is regular one or skewness txn
		boolean isRegularTx = (rvg.randomChooseFromDistribution(1 - SKEWNESS,
				SKEWNESS) == 0) ? true : false;

		// decide there is remote access or not
		boolean isRemote = (rvg.randomChooseFromDistribution(REMOTE_RATE,
				(1 - REMOTE_RATE)) == 0) ? true : false;

		// decide there is write or not
		boolean isWrite = (rvg.randomChooseFromDistribution(WRITE_PERCENTAGE,
				(1 - WRITE_PERCENTAGE)) == 0) ? true : false;

		// if (isRemote)
		// isWrite = true;

		// decide if this is a long read tx
		boolean isLongRead = (rvg.randomChooseFromDistribution(
				LONG_READ_PERCENTAGE, (1 - LONG_READ_PERCENTAGE)) == 0) ? true
				: false;

		// **********************
		// Start prepare params
		// **********************

		// randomly choose the main partition
		int mainPartition = 0;
		if (isRegularTx) {
			mainPartition = rvg.number(0, PARTITION_NUM - 1);
		} else {
			int availablePartition = (int) (SKEW_PERCENTAGE * PARTITION_NUM) - 1;
			if (availablePartition < 1)
				mainPartition = 0;
			else
				mainPartition = rvg.number(0, availablePartition);
		}

		// set read count

		// int local_cold_count = rvg.number(COLD_DATA_PER_TX - 5,
		// COLD_DATA_PER_TX + 5);
		int local_hot_count = HOT_DATA_PER_TX;
		int local_cold_count = COLD_DATA_PER_TX;
		int remote_hot_count = REMOTE_HOT_COUNT;
		int remote_cold_count = REMOTE_COLD_COUNT;

		// if (isRemote)
		// local_hot_count = 3;

		// create long read only transaction
		if (!isWrite && isLongRead) {
			// local_cold_count += 20;
			sessionUser = rvg.number(0, USER_COUNT - 100) + 100;
		}

		int totalReadCount = local_cold_count + local_hot_count
				+ (isRemote ? (remote_hot_count + remote_cold_count) : 0);

		paramList.add(totalReadCount);

		// randomly choose a hot data
		chooseHotData(paramList, mainPartition, local_hot_count);

		// randomly choose COLD_DATA_PER_TX data from cold dataset
		chooseColdData(paramList, mainPartition, local_cold_count);

		// remote
		if (isRemote) {

			// randomly choose hot data from other partitions
			int[] partitionHotCount = new int[PARTITION_NUM];
			partitionHotCount[mainPartition] = 0;
			RandomNonRepeatGenerator rnpg = new RandomNonRepeatGenerator(
					PARTITION_NUM - 1);
			for (int i = 0; i < remote_hot_count; i++) {
				int remotePartition = randomChooseOtherPartition(mainPartition,
						rvg);
				partitionHotCount[remotePartition]++;
			}

			for (int i = 0; i < PARTITION_NUM; i++)
				chooseHotData(paramList, i, partitionHotCount[i]);

			// randomly choose cold data from other partitions
			int[] partitionColdCount = new int[PARTITION_NUM];
			partitionColdCount[mainPartition] = 0;

			for (int i = 0; i < remote_cold_count; i++) {
				int remotePartition = randomChooseOtherPartition(mainPartition,
						rvg);
				partitionColdCount[remotePartition]++;
			}

			for (int i = 0; i < PARTITION_NUM; i++)
				chooseColdData(paramList, i, partitionColdCount[i]);
		}

		// write
		if (isWrite) {

			totalReadCount = paramList.size() - 1;

			// set write count = read count
			paramList.add((Integer) paramList.get(0));

			// for each item been read, set their item id to be written
			for (int i = 0; i < totalReadCount; i++)
				paramList.add(paramList.get(i + 1));

			// set the update value
			for (int i = 0; i < totalReadCount; i++)
				paramList.add(rvg.nextDouble() * 100000);

		} else {
			// set write count to 0
			paramList.add(0);
		}

		params = paramList.toArray();
		return params;
	}

	private void updateSessionUser(RandomValueGenerator rvg) {

		// simulate the workload changing
		// int rteNum = wid;
		// if (System.currentTimeMillis() - startTime >= 4 * 60 * 1000) {
		// // 0 - 100
		// sessionUser = rteNum + 100;
		// } else if (System.currentTimeMillis() - startTime >= 3 * 60 * 1000) {
		// // 50 - 50
		// if (rvg.randomChooseFromDistribution(0.25, 0.75) == 1) {
		// sessionUser = rteNum + 100;
		// } else {
		// sessionUser = rteNum;
		// }
		// } else if (System.currentTimeMillis() - startTime >= 2 * 60 * 1000) {
		// // 50 - 50
		// if (rvg.randomChooseFromDistribution(0.5, 0.5) == 1) {
		// sessionUser = rteNum + 100;
		// } else {
		// sessionUser = rteNum;
		// }
		// } else if (System.currentTimeMillis() - startTime >= 1 * 60 * 1000) {
		// // 50 - 50
		// if (rvg.randomChooseFromDistribution(0.75, 0.25) == 1) {
		// sessionUser = rteNum + 100;
		// } else {
		// sessionUser = rteNum;
		// }
		// } else {
		// // 100 - 0
		// sessionUser = rteNum;
		// }
		// sessionUser = rteNum;
		// System.out.println("sessionUser: " + sessionUser);

		// change the user every 10 rounds
		// if (sessionCountDown-- < 0) {
		// sessionCountDown = USER_SESSION_PERIOD;
		// sessionUser = rvg.number(0, USER_COUNT - 1);
		// }

	}

	private int randomChooseOtherPartition(int mainPartition,
			RandomValueGenerator rvg) {
		return ((mainPartition + rvg.number(1, PARTITION_NUM - 1)) % PARTITION_NUM);
	}

	private void chooseHotData(List<Object> paramList, int partition, int count) {
		int minMainPart = partition * DATA_SIZE_PER_PART;
		RandomNonRepeatGenerator rg = new RandomNonRepeatGenerator(
				HOT_DATA_SIZE_PER_PART);
		
		// Adjust for migration testing
		if(partition == PARTITION_NUM-1){
			minMainPart -= (DATA_SIZE_PER_PART * MicroMigrationManager.MIGRATE_PERCENTAGE); 
		}
		
		for (int i = 0; i < count; i++) {
			int tmp = rg.next(); // 1 ~ size
			int itemId = minMainPart + tmp;
			itemId = itemRandomMap.get(itemId);
			paramList.add(itemId);
		}

	}

	private void chooseColdData(List<Object> paramList, int partition, int count) {
		int minMainPartColdData = partition * DATA_SIZE_PER_PART
				+ HOT_DATA_SIZE_PER_PART;
		RandomNonRepeatGenerator rg = new RandomNonRepeatGenerator(
				DATA_PER_USER);
		
		// Adjust for migration testing
		if(partition == PARTITION_NUM-2){
			rg = new RandomNonRepeatGenerator((int)(DATA_SIZE_PER_PART* (1 - MicroMigrationManager.MIGRATE_PERCENTAGE)) - HOT_DATA_SIZE_PER_PART);
		}
		
		if(partition == PARTITION_NUM-1){
			minMainPartColdData -= (DATA_SIZE_PER_PART * MicroMigrationManager.MIGRATE_PERCENTAGE); 
			rg = new RandomNonRepeatGenerator((int)(DATA_SIZE_PER_PART * MicroMigrationManager.MIGRATE_PERCENTAGE) - HOT_DATA_SIZE_PER_PART);
		}
		
		for (int i = 0; i < count; i++) {
			int tmp = rg.next(); // 1 ~ size
			int itemId = minMainPartColdData + sessionUser * DATA_PER_USER
					+ tmp;

			itemId = itemRandomMap.get(itemId);
			paramList.add(itemId);
		}
	}
}
