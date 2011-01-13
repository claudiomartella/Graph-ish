package org.acaro.stagedgraphish;

public class DoesntExist extends RuntimeException {
	public DoesntExist(PropertyContainer container) {
		super("Couldn't find PropertyContainer "+ container.toString());
	}

	public DoesntExist(byte[] id) {
		super("Couldn't find PropertyContainer "+ id);
	}
}
