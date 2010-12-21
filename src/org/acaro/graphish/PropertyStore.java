package org.acaro.graphish;

import java.io.IOException;

/*
 * XXX: Does it make sense to have different methods for vertices and edges? In the end they are both PropertyContainers and have different ids
 * 		Why not refactor accepting a PropertyContainer as a parameter?
 */

public interface PropertyStore {
	/* Vertex support methods */
	
	public boolean hasProperty(PropertyContainer container, String key) throws IOException;

	public byte[] setProperty(PropertyContainer container, String key, byte[] value) throws IOException;

	public byte[] getProperty(PropertyContainer container, String key) throws IOException;

	public byte[] removeProperty(PropertyContainer container, String key) throws IOException;

	public Iterable<String> getPropertyKeys(PropertyContainer container) throws IOException;

	public Iterable<byte[]> getPropertyValues(PropertyContainer container) throws IOException;
	
	public PropertyContainer getPropertyContainer(PropertyContainer container) throws IOException;
}