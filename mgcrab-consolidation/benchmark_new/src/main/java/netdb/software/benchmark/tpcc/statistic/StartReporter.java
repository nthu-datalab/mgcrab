package netdb.software.benchmark.tpcc.statistic;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class StartReporter {

	public static File[] files = null;
	public static String outputPath = null;
	public static String dirName;

	public static void main(String args[]) {
		Reporter reporter = new Reporter();
		dirName = args[0];

		try {
			// load benchmark property file
			Properties props = new Properties();
			String path = System
					.getProperty("netdb.software.benchmark.tpcc.config.file");
			props.load(new FileInputStream(path));

			// get the input file folder, and set the input files
			String inputDir = props
					.getProperty("StatisticParameters.outputDir");
			setFileList(new File(inputDir));

			// get the output file path
			outputPath = props
					.getProperty("StatisticParameters.fianlOutputPath");

			reporter.report();

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public static void setFileList(final File folder) {
		files = folder.listFiles();
	}
}
