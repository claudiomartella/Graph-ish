package org.acaro.graphish;

import java.util.HashMap;

public class PropertyContainerImpl implements PropertyContainer {
	private HashMap<String, byte[]> props = new HashMap<String, byte[]>();
	private byte[] id;
	
	public byte[] getId() {
		return id;
	}

	public boolean hasProperty(String key) {
		return props.containsKey(key);
	}

	public byte[] setProperty(String key, byte[] value) {
		return props.put(key, value);
	}

	public byte[] getProperty(String key) {
		return props.get(key);
	}

	public byte[] removeProperty(String key) {
		return props.remove(key);
	}

	public Iterable<String> getPropertyKeys() {
		return props.keySet();
	}

	public Iterable<byte[]> getPropertyValues() {
		return props.values();
	}
}