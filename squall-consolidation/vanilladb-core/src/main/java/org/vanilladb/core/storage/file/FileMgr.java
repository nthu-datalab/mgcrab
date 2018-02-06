package org.vanilladb.core.storage.file;

import static org.vanilladb.core.storage.file.Page.BLOCK_SIZE;
import static org.vanilladb.core.storage.log.LogMgr.LOG_FILE;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import net.smacke.jaydio.DirectIoLib;
import net.smacke.jaydio.buffer.AlignedDirectByteBuffer;
import net.smacke.jaydio.channel.BufferedChannel;
import net.smacke.jaydio.channel.DirectIoByteChannel;

import org.vanilladb.core.server.VanillaDb;

/**
 * 
 * The VanillaDb file manager. The database system stores its data as files
 * 
 * within a specified directory. The file manager provides methods for reading
 * 
 * the contents of a file block to a Java byte buffer, writing the contents of a
 * 
 * byte buffer to a file block, and appending the contents of a byte buffer to
 * 
 * the end of a file. These methods are called exclusively by the class
 * 
 * {@link org.vanilladb.core.storage.file.Page Page}, and are thus
 * 
 * package-private. The class also contains two public methods: Method
 * 
 * {@link #isNew() isNew} is called during system initialization by
 * 
 * {@link VanillaDb#init}. Method {@link #size(String) size} is called by the
 * 
 * log manager and transaction manager to determine the end of the file.
 */

public class FileMgr {

	private static final String HOME_DIR, LOG_FILE_BASE_DIR;
	private static Logger logger = Logger.getLogger(FileMgr.class.getName());

	private File dbDirectory, logDirectory;
	private boolean isNew;
	private Map<String, BufferedChannel<AlignedDirectByteBuffer>> openFiles = new ConcurrentHashMap<String, BufferedChannel<AlignedDirectByteBuffer>>();
	private Map<String, Lock> openFileLocks = new ConcurrentHashMap<String, Lock>();
	private Map<String, AlignedDirectByteBuffer> transferBuffers = new ConcurrentHashMap<String, AlignedDirectByteBuffer>();

	// Optimization: store the size of each table
	private Map<String, Long> fileSizeMap = new ConcurrentHashMap<String, Long>();

	static {

		String prop = System.getProperty(FileMgr.class.getName()

		+ ".LOG_FILE_BASE_DIR");

		LOG_FILE_BASE_DIR = (prop == null ? null : prop.trim());

		prop = System.getProperty(FileMgr.class.getName() + ".HOME_DIR");

		HOME_DIR = (prop == null ? null : prop.trim());

	}

	private final Object[] anchors = new Object[1009];

	private Object prepareAnchor(Object o) {

		int code = o.hashCode() % anchors.length;

		if (code < 0) {

			code += anchors.length;

		}

		return anchors[code];

	}

	/**
	 * 
	 * Creates a file manager for the specified database. The database will be
	 * 
	 * stored in a folder of that name in the user's home directory. If the
	 * 
	 * folder does not exist, then a folder containing an empty database is
	 * 
	 * created automatically. Files for all temporary tables (i.e. tables
	 * 
	 * beginning with "_temp") are deleted.
	 * 
	 * 
	 * 
	 * @param dbName
	 * 
	 *            the name of the directory that holds the database
	 */

	public FileMgr(String dbName) {

		String homeDir = (HOME_DIR == null || HOME_DIR.isEmpty()) ? System

		.getProperty("user.home") : HOME_DIR;

		dbDirectory = new File(homeDir, dbName);

		// the log file can be specified to be stored in different location

		String logdir = (LOG_FILE_BASE_DIR == null || LOG_FILE_BASE_DIR

		.isEmpty()) ? homeDir : LOG_FILE_BASE_DIR;

		logDirectory = new File(logdir, dbName);

		isNew = !dbDirectory.exists();

		// deal with the log folder in new database

		if (isNew && !dbDirectory.equals(logDirectory)) {

			// delete the old log file if db is new

			if (logDirectory.exists()) {

				deleteLogFiles();

			} else if (!logDirectory.mkdir())

				throw new RuntimeException("cannot create log file for"

				+ dbName);

		}

		// check the existence of log folder

		if (!isNew && !logDirectory.exists())

			throw new RuntimeException("log file for the existed " + dbName

			+ " is missing");

		// create the directory if the database is new

		if (isNew && (!dbDirectory.mkdir()))

			throw new RuntimeException("cannot create " + dbName);

		// remove any leftover temporary tables

		for (String filename : dbDirectory.list())

			if (filename.startsWith("_temp"))

				new File(dbDirectory, filename).delete();

		if (logger.isLoggable(Level.INFO))

			logger.info("block size " + Page.BLOCK_SIZE);

		for (int i = 0; i < anchors.length; ++i) {

			anchors[i] = new Object();

		}

	}

	/**
	 * Reads the contents of a disk block into a bytebuffer.
	 * 
	 * @param blk
	 *            a block ID
	 * @param bb
	 *            the bytebuffer
	 */
	void read(BlockId blk, byte[] dst) {
		try {
			BufferedChannel<AlignedDirectByteBuffer> fc = getFile(blk
					.fileName());
			AlignedDirectByteBuffer buffer = transferBuffers
					.get(blk.fileName());
			Lock lock = openFileLocks.get(blk.fileName());

			lock.lock();
			try {
				fc.read(buffer, blk.number() * BLOCK_SIZE);
				buffer.rewind();
				buffer.get(dst, 0, dst.length);
			} finally {
				lock.unlock();
			}
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException("cannot read block " + blk);
		}
	}

	/**
	 * Writes the contents of a bytebuffer into a disk block.
	 * 
	 * @param blk
	 *            a block ID
	 * @param bb
	 *            the bytebuffer
	 */
	void write(BlockId blk, byte[] dst) {
		try {
			BufferedChannel<AlignedDirectByteBuffer> fc = getFile(blk
					.fileName());
			AlignedDirectByteBuffer buffer = transferBuffers
					.get(blk.fileName());
			Lock lock = openFileLocks.get(blk.fileName());

			lock.lock();
			try {
				buffer.rewind();
				buffer.put(dst, 0, dst.length);
				buffer.rewind();
				fc.write(buffer, blk.number() * BLOCK_SIZE);

				// Optimization:
				if (blk.number() + 1 > fileSizeMap.get(blk.fileName()))
					fileSizeMap.put(blk.fileName(), blk.number() + 1);
			} finally {
				lock.unlock();
			}
		} catch (IOException e) {
			throw new RuntimeException("cannot write block" + blk);
		}
	}

	/**
	 * Appends the contents of a bytebuffer to the end of the specified file.
	 * 
	 * @param fileName
	 *            the name of the file
	 * @param bb
	 *            the bytebuffer
	 * @return a block ID refers to the newly-created block.
	 */
	BlockId append(String fileName) {
		// Optimization: Only returning reference to a new block
		// BufferedChannel<AlignedDirectByteBuffer> fc = getFile(fileName);
		Lock lock = openFileLocks.get(fileName);
		lock.lock();
		try {
			// Optimization:
			// long newblknum = fc.size() / BLOCK_SIZE;
			long newblknum = fileSizeMap.get(fileName);
			BlockId blk = new BlockId(fileName, newblknum);

			// fc.write(bb, blk.number() * BLOCK_SIZE);
			// ? fileSizeMap.put(fileName, fc.size()/BLOCK_SIZE);
			fileSizeMap.put(fileName, ++newblknum);
			return blk;
		} finally {
			lock.unlock();
		}
	}

	/**
	 * 
	 * Returns the number of blocks in the specified file.
	 * 
	 * 
	 * 
	 * @param fileName
	 * 
	 *            the name of the file
	 * 
	 * @return the number of blocks in the file
	 */

	public long size(String fileName) {

		try {

			BufferedChannel<AlignedDirectByteBuffer> fc = getFile(fileName);

			// synchronized (fc) {

			// return fc.size() / BLOCK_SIZE;

			// }

			// Optimization:

			Lock lock = openFileLocks.get(fileName);

			lock.lock();

			try {

				return fileSizeMap.get(fileName);

			} finally {

				lock.unlock();

			}

		} catch (IOException e) {

			throw new RuntimeException("cannot access " + fileName);

		}

	}

	/**
	 * 
	 * Returns a boolean indicating whether the file manager had to create a new
	 * 
	 * database directory.
	 * 
	 * 
	 * 
	 * @return true if the database is new
	 */

	public boolean isNew() {

		return isNew;

	}

	/**
	 * 
	 * Delete all old log files and build new one.
	 */

	public void rebuildLogFile() {

		try {

			deleteLogFiles();

			// Create new log file

			File logFile = new File(logDirectory, LOG_FILE);

			RandomAccessFile f = new RandomAccessFile(logFile, "rws");

			BufferedChannel<AlignedDirectByteBuffer> fc = DirectIoByteChannel

			.getChannel(logFile, false);

			openFiles.put(LOG_FILE, fc);

			openFileLocks.put(LOG_FILE, new ReentrantLock());

			fileSizeMap.put(LOG_FILE, fc.size() / BLOCK_SIZE);

		} catch (IOException e) {

			throw new RuntimeException("rebuild log file fail");

		}

		try {

			// Create new dd log file

			File logFile = new File(logDirectory, "vanilladddb.log");

			RandomAccessFile f = new RandomAccessFile(logFile, "rws");

			BufferedChannel<AlignedDirectByteBuffer> fc = DirectIoByteChannel

			.getChannel(logFile, false);

			openFiles.put("vanilladddb.log", fc);

			openFileLocks.put("vanilladddb.log", new ReentrantLock());

			fileSizeMap.put("vanilladddb.log", fc.size() / BLOCK_SIZE);

		} catch (IOException e) {

			throw new RuntimeException("rebuild log file fail");

		}

	}

	/**
	 * Returns the file channel for the specified filename. The file channel is
	 * stored in a map keyed on the filename. If the file is not open, then it
	 * is opened and the file channel is added to the map.
	 * 
	 * @param fileName
	 *            the specified filename
	 * @return the file channel associated with the open file.
	 * @throws IOException
	 */
	private BufferedChannel<AlignedDirectByteBuffer> getFile(String fileName)
			throws IOException {
		synchronized (prepareAnchor(fileName)) {
			BufferedChannel<AlignedDirectByteBuffer> fc = openFiles
					.get(fileName);

			if (fc == null) {
				File dbFile = fileName.equals(LOG_FILE) ? new File(
						logDirectory, fileName) : new File(dbDirectory,
						fileName);

				fc = DirectIoByteChannel.getChannel(dbFile, false);
				openFiles.put(fileName, fc);
				openFileLocks.put(fileName, new ReentrantLock());

				// Create a new transfer buffer
				AlignedDirectByteBuffer buffer = AlignedDirectByteBuffer
						.allocate(DirectIoLib.getLibForPath("/ssd"), BLOCK_SIZE);
				transferBuffers.put(fileName, buffer);

				// Optimization:
				fileSizeMap.put(fileName, fc.size() / BLOCK_SIZE);
			}

			return fc;
		}
	}

	/**
	 * Delete all log files in log directory.
	 */
	private void deleteLogFiles() {
		try {
			for (String fileName : logDirectory.list())
				if (fileName.endsWith(".log")) {
					// Close file, if it opened
					BufferedChannel<AlignedDirectByteBuffer> fc = openFiles
							.remove(fileName);
					openFileLocks.remove(fileName);
					if (fc != null)
						fc.close();

					// Actually delete file
					boolean hasDeleted = new File(logDirectory, fileName)
							.delete();
					if (!hasDeleted && logger.isLoggable(Level.WARNING))
						logger.warning("cannot deleted old log file");
				}
		} catch (IOException e) {
			if (logger.isLoggable(Level.WARNING))
				logger.warning("cannot deleted old log file");
		}
	}
}