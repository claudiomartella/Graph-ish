package org.acaro.stagedgraphish;

import java.util.concurrent.Future;

public interface PropertyContainer {

	public byte[] getId();
	
	public Future<Boolean> hasProperty(String key);
	
	public Future<Void> setProperty(String key, byte[] value);
	
	public Future<byte[]> getProperty(String key);
	
	public Future<byte[]> removeProperty(String key);
	
	public Iterable<String> getPropertyKeys();
	
	public Iterable<byte[]> getPropertyValues();
}
