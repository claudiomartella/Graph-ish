package org.acaro.graphish;

public class ContainerDoesntExist extends RuntimeException {
	public ContainerDoesntExist(PropertyContainer container) {
		super("Couldn't find PropertyContainer "+ container.toString());
	}
}
