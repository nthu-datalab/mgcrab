package netdb.software.benchmark.tpcc.util;

import netdb.software.benchmark.tpcc.TpccConstants;

public class YcsbLatestGenerator {
	
	private final YcsbZipfianGenerator zipfian;
	private int recordCount;

	public YcsbLatestGenerator(int recordCount, double skewParameter) {
		this.recordCount = recordCount;
		zipfian = new YcsbZipfianGenerator(1, recordCount, skewParameter);
		nextValue();
	}

	/**
	 * Generate the next string in the distribution, skewed Zipfian favoring the
	 * items most recently returned by the basis generator.
	 */
	public long nextValue() {
		long next = recordCount - zipfian.nextLong(recordCount) + 1;
		return next;
	}
	
	public static void main(String[] args) {
		YcsbLatestGenerator gen = new YcsbLatestGenerator(TpccConstants.YCSB_RECORD_PER_PART, 0.9);
		
		int numOfTimes = 100;
		int countPerSeg = TpccConstants.YCSB_RECORD_PER_PART / 100;
		int[] times = new int[numOfTimes];
		
		for (int i = 0; i < TpccConstants.YCSB_RECORD_PER_PART; i++) {
			int seg = (int) ((gen.nextValue() - 1) / countPerSeg);
			times[seg]++;
		}
		
		for (int i = 0; i < times.length; i++)
			System.out.println((i * countPerSeg) + "\t" + times[i]);
	}
}
