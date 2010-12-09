package org.acaro.graphish;

public interface PropertyContainer {

	public boolean hasProperty(String key);
	
	public void setProperty(String key, byte[] value);
	
	public byte[] getProperty(String key);
	
	public byte[] removeProperty(String key);
	
	public Iterable<String> getPropertyKeys();
	
	public Iterable<byte[]> getPropertyValues();
}
