package org.acaro.stagedgraphish;

public class StorageException extends RuntimeException {
	private static final long serialVersionUID = 6641309105609129084L;

	public StorageException(Exception e){
		super(e);
	}

	public StorageException(String string) {
		super(string);
	}
}
