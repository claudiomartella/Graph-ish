package org.acaro.graphish;

import org.acaro.stagedgraphish.PropertyContainer;

/*
 * XXX: Does it make sense to have different methods for vertices and edges? In the end they are both PropertyContainers and have different ids
 * 		Why not refactor accepting a PropertyContainer as a parameter?
 */

public interface PropertyStore {
	/* Vertex support methods */
	
	public boolean hasProperty(byte[] container, String key);

	public void setProperty(byte[] container, String key, byte[] value);

	public byte[] getProperty(byte[] container, String key);

	public byte[] removeProperty(byte[] container, String key);

	public Iterable<String> getPropertyKeys(byte[] container);

	public Iterable<byte[]> getPropertyValues(byte[] container);
	
	public PropertyContainer getPropertyContainer(byte[] container);
}