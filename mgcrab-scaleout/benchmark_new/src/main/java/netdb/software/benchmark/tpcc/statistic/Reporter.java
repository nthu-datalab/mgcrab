package netdb.software.benchmark.tpcc.statistic;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import netdb.software.benchmark.tpcc.TransactionType;
import netdb.software.benchmark.tpcc.procedure.vanilladb.DeliveryProc;

public class Reporter {

	private HashMap<TransactionType, TxnStatistic> txnStatistics = new HashMap<TransactionType, TxnStatistic>();

	private void calculateDeliverTxnLatency() throws IOException {
		String homedir = System.getProperty("user.home");
		File dbDirectory = new File(homedir, StartReporter.dirName);
		File file = new File(dbDirectory, DeliveryProc.resultFileName);

		if (!file.exists())
			return;

		BufferedReader br = new BufferedReader(new FileReader(file));

		String line = null;
		long totalLatency = 0;
		int txnCount = 0;
		// read all record line by line
		while ((line = br.readLine()) != null) {
			String[] tokens = line.split(" ");
			txnCount++;
			// latency is calculated by the last number - first number in a line
			totalLatency += (Long.parseLong(tokens[tokens.length - 1]) - Long
					.parseLong(tokens[0]));
		}

		totalLatency = TimeUnit.NANOSECONDS.toMillis(totalLatency);

		txnStatistics.get(TransactionType.DELIVERY).addStatistic(txnCount,
				totalLatency);

		br.close();

		file.deleteOnExit();
	}

	public void report() throws IOException {
		txnStatistics.put(TransactionType.NEW_ORDER, new TxnStatistic(
				TransactionType.NEW_ORDER));
		txnStatistics.put(TransactionType.DELIVERY, new TxnStatistic(
				TransactionType.DELIVERY));
		txnStatistics.put(TransactionType.STOCK_LEVEL, new TxnStatistic(
				TransactionType.STOCK_LEVEL));
		txnStatistics.put(TransactionType.PAYMENT, new TxnStatistic(
				TransactionType.PAYMENT));
		txnStatistics.put(TransactionType.ORDER_STATUS, new TxnStatistic(
				TransactionType.ORDER_STATUS));

		for (final File file : StartReporter.files) {
			BufferedReader br = new BufferedReader(new FileReader(file));
			// read first line to know how many txn are record in this file, and
			// then skip the log
			int txnCount = Integer.parseInt(br.readLine());
			for (int i = 0; i < txnCount; i++)
				br.readLine();
			// skip one line
			br.readLine();

			String line = null;
			while ((line = br.readLine()) != null) {
				String[] tokens = line.split(" ");
				if (TransactionType.valueOf(tokens[0]) == TransactionType.DELIVERY)
					continue;
				TxnStatistic statistic = txnStatistics.get(TransactionType
						.valueOf(tokens[0]));
				statistic.addStatistic(Integer.parseInt(tokens[1]),
						Long.parseLong(tokens[2]));
			}

			br.close();
			file.deleteOnExit();
		}

		calculateDeliverTxnLatency();

		FileWriter wrFile;
		wrFile = new FileWriter(StartReporter.outputPath);
		BufferedWriter bwrFile = new BufferedWriter(wrFile);

		for (Entry<TransactionType, TxnStatistic> entry : txnStatistics
				.entrySet()) {
			TxnStatistic value = entry.getValue();

			bwrFile.newLine();
			bwrFile.write("**" + value.getmType() + "**");
			bwrFile.newLine();
			bwrFile.write("	Txn Number: " + value.getTxnCount());
			bwrFile.newLine();

			// if txn number = 0, no need to print other statistics
			if (value.getTxnCount() <= 0)
				continue;

			bwrFile.write("	Average Txn Latency: "
					+ TimeUnit.NANOSECONDS.toMillis(value
							.getTotalResponseTime() / value.getTxnCount())
					+ " ms");
			bwrFile.newLine();
		}
		bwrFile.close();
	}

	private static class TxnStatistic {
		private TransactionType mType;
		private int txnCount = 0;
		private long totalResponseTime = 0;

		private static int globalTxnCount = 0;

		public TxnStatistic(TransactionType txnType) {
			this.mType = txnType;
		}

		public TransactionType getmType() {
			return mType;
		}

		public void addStatistic(int count, long responseTime) {
			txnCount += count;
			globalTxnCount += count;
			totalResponseTime += responseTime;
		}

		public int getTxnCount() {
			return txnCount;
		}

		public long getTotalResponseTime() {
			return totalResponseTime;
		}
	}
}