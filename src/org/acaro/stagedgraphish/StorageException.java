package org.acaro.stagedgraphish;

public class StorageException extends RuntimeException {
	public StorageException(Exception e){
		super(e);
	}

	public StorageException(String string) {
		super(string);
	}
}
