package org.acaro.graphish;

/*
 * XXX: Does it make sense to have different methods for vertices and edges? In the end they are both PropertyContainers and have different ids
 * 		Why not refactor accepting a PropertyContainer as a parameter?
 */

public interface GraphStore {
	/* Vertex support methods */
	public Vertex createVertex(Graphish graph);
	
	public Edge createEdge(Graphish graph, Vertex from, Vertex to, String type);
	
	public boolean hasProperty(PropertyContainer container, String key);

	public byte[] setProperty(PropertyContainer container, String key, byte[] value);

	public byte[] getProperty(PropertyContainer container, String key);

	public byte[] removeProperty(PropertyContainer container, String key);

	public Iterable<String> getPropertyKeys(PropertyContainer container);

	public Iterable<byte[]> getPropertyValues(PropertyContainer container);
	
	public PropertyContainer getPropertyContainer(PropertyContainer container);
}