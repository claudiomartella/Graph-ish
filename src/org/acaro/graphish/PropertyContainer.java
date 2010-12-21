package org.acaro.graphish;

import java.io.IOException;

public interface PropertyContainer {

	public byte[] getId();
	
	public boolean hasProperty(String key) throws IOException;
	
	public byte[] setProperty(String key, byte[] value) throws IOException;
	
	public byte[] getProperty(String key) throws IOException;
	
	public byte[] removeProperty(String key) throws IOException;
	
	public Iterable<String> getPropertyKeys() throws IOException;
	
	public Iterable<byte[]> getPropertyValues() throws IOException;
}
