package org.vanilladb.core.storage.file;

import java.nio.BufferOverflowException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.util.ByteHelper;

/**
 * The contents of a disk block in memory. A page is treated as an array of
 * BLOCK_SIZE bytes. There are methods to get/set values into this array, and to
 * read/write the contents of this array to a disk block. For an example of how
 * to use Page and {@link BlockId} objects, consider the following code
 * fragment. The first portion increments the integer at offset 792 of block 6
 * of file junk. The second portion stores the string "hello" at offset 20 of a
 * page, and then appends it to a new block of the file. It then reads that
 * block into another page and extracts the value "hello" into variable s.
 * 
 * <pre>
 * Page p1 = new Page();
 * BlockId blk = new BlockId(&quot;junk&quot;, 6);
 * p1.read(blk);
 * Constant c = p1.getVal(792, new Type.INTEGER);
 * int n = (Integer) c.asJavaVal();
 * Constant v1 = new IntegerConstant(n);
 * p1.setVal(792, v1);
 * p1.write(blk);
 * 
 * Page p2 = new Page();
 * Constant v2 = new VarcharConstant(&quot;hello&quot;);
 * p2.setVal(20, v2);
 * blk = p2.append(&quot;junk&quot;);
 * Page p3 = new Page();
 * p3.read(blk);
 * String s = (String) p3.getVal(20).asJavaVal();
 * </pre>
 */

public class Page {

	/**
	 * The number of bytes in a block. A reasonable value would be 4K.
	 */

	public static final int BLOCK_SIZE;
	static {
		String prop = System.getProperty(Page.class.getName() + ".BLOCK_SIZE");
		BLOCK_SIZE = (prop == null ? 4000 : Integer.parseInt(prop.trim()));
	}

	/**
	 * Calculates the maximum number of bytes required to store a value of a
	 * particular {@link Type type} in disk.
	 * 
	 * @return the number of bytes required
	 */

	public static int maxSize(Type type) {
		return type.isFixedSize() ? type.maxSize() : ByteHelper.INT_SIZE
				+ type.maxSize();
	}

	/**
	 * Calculates the number of bytes required to store a {@link Constant
	 * constant} in disk.
	 * 
	 * @return the number of bytes required
	 */

	public static int size(Constant val) {
		return val.getType().isFixedSize() ? val.size() : ByteHelper.INT_SIZE
				+ val.size();
	}

	private byte[] contents = new byte[BLOCK_SIZE];
	private final ReentrantReadWriteLock wrLock = new ReentrantReadWriteLock();
	private final Lock rLock = wrLock.readLock();
	private final Lock wLock = wrLock.writeLock();

	private FileMgr fileMgr = VanillaDb.fileMgr();

	private static void ArrayCopy(byte[] src, int srcOffset, byte[] dst,
			int dstOffset, int copyLen) {
		try {
			for (int i = 0; i < copyLen; i++)
				dst[dstOffset + i] = src[srcOffset + i];
		} catch (ArrayIndexOutOfBoundsException e) {
			new RuntimeException("S offset: " + srcOffset + ", d offset: "
					+ dstOffset + ", len: " + copyLen);
		}
	}

	/**
	 * Creates a new page. Although the constructor takes no arguments, it
	 * depends on a {@link FileMgr} object that it gets from the method
	 * {@link VanillaDb#fileMgr()}. That object is created during system
	 * initialization. Thus this constructor cannot be called until either
	 * {@link VanillaDb#init(String)} or {@link VanillaDb#initFileMgr(String)}
	 * or {@link VanillaDb#initFileAndLogMgr(String)} or
	 * {@link VanillaDb#initFileLogAndBufferMgr(String)} is called first.
	 */
	public Page() {
	}

	/**
	 * Populates the page with the contents of the specified disk block.
	 * 
	 * @param blk
	 *            a block ID
	 */

	public void read(BlockId blk) {
		wLock.lock();
		try {
			fileMgr.read(blk, contents);
		} finally {
			wLock.unlock();
		}
	}

	/**
	 * Writes the contents of the page to the specified disk block.
	 * 
	 * @param blk
	 *            a block ID
	 */

	public void write(BlockId blk) {
		wLock.lock();
		try {
			fileMgr.write(blk, contents);
		} finally {
			wLock.unlock();
		}
	}

	/**
	 * Appends the contents of the page to the specified file.
	 * 
	 * @param fileName
	 *            the name of the file
	 * 
	 * @return the reference to the newly-created disk block
	 */

	public BlockId append(String fileName) {
		wLock.lock();
		try {
			return fileMgr.append(fileName);
		} finally {
			wLock.unlock();
		}
	}

	/**
	 * Returns the value at a specified offset of this page. If a constant was
	 * not stored at that offset, the behavior of the method is unpredictable.
	 * 
	 * @param offset
	 *            the byte offset within the page
	 * 
	 * @param type
	 *            the type of the value
	 * 
	 * @return the constant value at that offset
	 */

	public Constant getVal(int offset, Type type) {
		rLock.lock();
		try {
			int size;
			byte[] byteVal = null;

			// Check the length of bytes
			if (type.isFixedSize()) {
				size = type.maxSize();
			} else {
				byteVal = new byte[ByteHelper.INT_SIZE];
				ArrayCopy(contents, offset, byteVal, 0, ByteHelper.INT_SIZE);
				size = ByteHelper.toInteger(byteVal);
				offset += ByteHelper.INT_SIZE;
			}

			// Get bytes and translate it to Constant
			byteVal = new byte[size];
			ArrayCopy(contents, offset, byteVal, 0, size);
			return Constant.newInstance(type, byteVal);
		} finally {
			rLock.unlock();
		}
	}

	/**
	 * Writes a constant value to the specified offset on the page.
	 * 
	 * @param offset
	 *            the byte offset within the page
	 * 
	 * @param val
	 *            the constant value to be written to the page
	 */
	public void setVal(int offset, Constant val) {
		wLock.lock();
		try {
			byte[] byteval = val.asBytes();

			// Append the size of value if it is not fixed size
			if (!val.getType().isFixedSize()) {
				// check the field capacity and value size
				if (offset + ByteHelper.INT_SIZE + byteval.length > BLOCK_SIZE)
					throw new BufferOverflowException();

				byte[] sizeBytes = ByteHelper.toBytes(byteval.length);
				ArrayCopy(sizeBytes, 0, contents, offset, ByteHelper.INT_SIZE);
				offset += sizeBytes.length;
			}

			// Put bytes
			ArrayCopy(byteval, 0, contents, offset, byteval.length);
		} finally {
			wLock.unlock();
		}
	}

	public void close() {
		// contents.close();
	}
}