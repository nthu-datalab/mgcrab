package org.vanilladb.core.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.storage.file.FileMgr;
import org.vanilladb.core.storage.log.LogMgr;
import org.vanilladb.core.storage.tx.recovery.LogReader;

public class LogFileTranslator {
	private static Logger logger = Logger.getLogger(LogFileTranslator.class.getName());
	private static String LOG_FILE_BASE_DIR;
	private static String LOG_FILE_NAME;
	
	// Read property file
	static {
		String path = System.getProperty("org.vanilladb.core.config.file");
		if (path != null) {
			FileInputStream fis = null;
			try {
				fis = new FileInputStream(path);
				System.getProperties().load(fis);
			} catch (IOException e) {
				System.out.println("Can't read config file.");
			} finally {
				try {
					if (fis != null)
						fis.close();
				} catch (IOException e) {
					// do nothing
				}
			}
		}
		
		String prop = System.getProperty(FileMgr.class.getName()
				+ ".LOG_FILE_BASE_DIR");
		LOG_FILE_BASE_DIR = (prop == null || prop.isEmpty()) ? 
				System.getProperty("user.home") : prop.trim();
		prop = System.getProperty(LogMgr.class.getName() + ".LOG_FILE");
		LOG_FILE_NAME = (prop == null ? "vanilladb.log" : prop.trim());
	}
	
	public static void main(String[] args) {
		if (logger.isLoggable(Level.INFO))
			logger.info("Initializing...");
		
		// check argument
		if (args.length < 1)
			System.out.println("Please enter the name of database directory.");
		
		// initialize basic components
		VanillaDb.initFileMgr(args[0]);

		translateLogFile(args[0], "plaintextLogFile.txt");
		
		if (logger.isLoggable(Level.INFO))
			logger.info("Translation completed");
	}
	
	private static void translateLogFile(String dirName, String outputFileName) {
		LogReader reader = new LogReader(LOG_FILE_NAME);
		File dir = new File(LOG_FILE_BASE_DIR);
		dir = new File(dir, dirName);
		
		File logFile = new File(dir, LOG_FILE_NAME);
		if (logger.isLoggable(Level.INFO))
			logger.info("Read log data from \"" + logFile.toString() + "\"");

		try {
			// Translate log file
			File outputFile = new File(dir, outputFileName);
			BufferedWriter out = new BufferedWriter(new FileWriter(outputFile));
			while (reader.nextRecord()) {
				out.write(reader.getLogString());
				out.newLine();
			}
			out.close();

		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
