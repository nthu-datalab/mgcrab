package netdb.software.benchmark.tpcc.util;

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
		YcsbLatestGenerator gen = new YcsbLatestGenerator(3, 0.9);
		int[] times = new int[3];
		for (int i = 0; i < 300; i++) {
			times[((int) gen.nextValue()) - 1]++;
		}
		
		for (int i = 0; i < 3; i++)
			System.out.println(times[i]);
	}
}
