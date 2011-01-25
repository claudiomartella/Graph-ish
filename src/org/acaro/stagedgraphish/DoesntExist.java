package org.acaro.stagedgraphish;

public class DoesntExist extends RuntimeException {
	private static final long serialVersionUID = -5991826933972782793L;

	public DoesntExist(PropertyContainer container) {
		super("Couldn't find PropertyContainer "+ container.toString());
	}

	public DoesntExist(byte[] id) {
		super("Couldn't find PropertyContainer "+ id);
	}
}
