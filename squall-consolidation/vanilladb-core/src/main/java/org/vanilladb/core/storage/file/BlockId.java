package org.vanilladb.core.storage.file;

/**
 * A reference to a disk block. A BlockId object consists of a fileName and a
 * block number. It does not hold the contents of the block; instead, that is
 * the job of a {@link Page} object.
 */
public class BlockId {
	private String fileName;
	private long blkNum;
	// Optimization: Materialize the toString and hash value
	private int myHashCode;

	/**
	 * Constructs a block ID for the specified fileName and block number.
	 * 
	 * @param fileName
	 *            the name of the file
	 * @param blkNum
	 *            the block number
	 */
	public BlockId(String fileName, long blkNum) {
		this.fileName = fileName;
		this.blkNum = blkNum;
		// Optimization: Materialize the hash code and the output of toString
		myHashCode = 17;
		myHashCode = 31 * this.fileName.hashCode() + myHashCode;
		myHashCode = 31 * (int)(this.blkNum ^ (this.blkNum >>> 32 )) + myHashCode; 
	}

	/**
	 * Returns the name of the file where the block lives.
	 * 
	 * @return the fileName
	 */
	public String fileName() {
		return fileName;
	}

	/**
	 * Returns the location of the block within the file.
	 * 
	 * @return the block number
	 */
	public long number() {
		return blkNum;
	}

	public boolean equals(Object obj) {
		if (obj == this)
			return true;
		if (obj == null || !(obj.getClass().equals(BlockId.class)))
			return false;
		BlockId blk = (BlockId) obj;
		return fileName.equals(blk.fileName) && blkNum == blk.blkNum;
	}

	public String toString() {
		return "[file " + fileName + ", block " + blkNum + "]";
	}

	public int hashCode() {
		return myHashCode;
	}
}
