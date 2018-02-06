package org.vanilladb.core.storage.buffer;


public interface PageMetadata {
	
	void clean(Buffer ownerBuffer);
	
}
