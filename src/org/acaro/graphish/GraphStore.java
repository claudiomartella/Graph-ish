package org.acaro.graphish;

public interface GraphStore {
	/* Vertex support methods */
	public boolean hasProperty(Vertex vertex, String key);

	public byte[] setProperty(Vertex vertex, String key, byte[] value);

	public byte[] getProperty(Vertex vertex, String key);

	public byte[] removeProperty(Vertex vertex, String key);

	public Iterable<String> getPropertyKeys(Vertex vertex);

	public Iterable<byte[]> getPropertyValues(Vertex vertex);
	
	public PropertyContainer getPropertyContainer(Vertex vertex);

	/* Edge support methods*/
	
	public boolean hasProperty(Edge edge, String key);
	
	public byte[] setProperty(Edge edge, String key, byte[] value);

	public byte[] getProperty(Edge edge, String key);

	public byte[] removeProperty(Edge edge, String key);

	public Iterable<String> getPropertyKeys(Edge edge);

	public Iterable<byte[]> getPropertyValues(Edge edge);
	
	public PropertyContainer getPropertyContainer(Edge edge);
}