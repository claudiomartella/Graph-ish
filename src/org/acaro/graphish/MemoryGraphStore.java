package org.acaro.graphish;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.UUID;

/*
 * Write stuff to memory and that's it
 * 
 * TODO: what to do with non-existing vertex/edge? [FIX: shoud add this at create*() and relay on it's presence in props map]
 * 		 implement LRU semantics? Maybe in other LRUGraphStore?
 */

public class MemoryGraphStore implements GraphStore {
	private static final byte[] EDGE_CONJ = { 0x2b };
	private HashMap<byte[], PropertyContainer> vProps = new HashMap<byte[], PropertyContainer>();
	private HashMap<byte[], PropertyContainer> eProps = new HashMap<byte[], PropertyContainer>();
	
	public MemoryGraphStore(){
	}

	public boolean hasProperty(Vertex vertex, String key) {
		PropertyContainer p = checkAndSet(vertex);
		
		return p.hasProperty(key);
	}

	public byte[] setProperty(Vertex vertex, String key, byte[] value) {
		PropertyContainer p = checkAndSet(vertex);

		return p.setProperty(key, value);
	}

	public byte[] getProperty(Vertex vertex, String key) {
		PropertyContainer p = checkAndSet(vertex);
		
		return p.getProperty(key);
	}

	public byte[] removeProperty(Vertex vertex, String key) {
		PropertyContainer p = checkAndSet(vertex);

		return p.removeProperty(key);
	}

	public Iterable<String> getPropertyKeys(Vertex vertex) {
		PropertyContainer p = checkAndSet(vertex);
		
		return p.getPropertyKeys();
	}

	public Iterable<byte[]> getPropertyValues(Vertex vertex) {
		PropertyContainer p = checkAndSet(vertex);
		
		return p.getPropertyValues();
	}

	public boolean hasProperty(Edge edge, String key) {
		PropertyContainer p = checkAndSet(edge);
		
		return p.hasProperty(key);
	}

	public byte[] setProperty(Edge edge, String key, byte[] value) {
		PropertyContainer p = checkAndSet(edge);

		return p.setProperty(key, value);
	}

	public byte[] getProperty(Edge edge, String key) {
		PropertyContainer p = checkAndSet(edge);
		
		return p.getProperty(key);
	}

	public byte[] removeProperty(Edge edge, String key) {
		PropertyContainer p = checkAndSet(edge);
		
		return p.removeProperty(key);
	}

	public Iterable<String> getPropertyKeys(Edge edge) {
		PropertyContainer p = checkAndSet(edge);
		
		return p.getPropertyKeys();
	}

	public Iterable<byte[]> getPropertyValues(Edge edge) {
		PropertyContainer p = checkAndSet(edge);
		
		return p.getPropertyValues();
	}

	public PropertyContainer getPropertyContainer(Vertex vertex) {
		return checkAndSet(vertex);
	}

	public PropertyContainer getPropertyContainer(Edge edge) {
		return checkAndSet(edge);
	}
	
	private PropertyContainer checkAndSet(Vertex vertex){
		PropertyContainer p;
		if((p = vProps.get(vertex.getId()))== null){
			p = new PropertyContainerImpl();
			vProps.put(vertex.getId(), p);
		}
		
		return p;
	}
	
	private PropertyContainer checkAndSet(Edge edge){
		PropertyContainer p;
		if((p = eProps.get(edge.getId()))== null){
			p = new PropertyContainerImpl();
			eProps.put(edge.getId(), p);
		}
		
		return p;
	}

	public Vertex createVertex(Graphish graph) {
		UUID id = UUID.randomUUID();
		
		return new VertexImpl(graph, Bytes.fromUuid(id).toByteArray());
	}

	public Edge createEdge(Graphish graph, Vertex from, Vertex to, String type) { 
		byte id[] = createEdgeId(from, to);
		
		return new EdgeImpl(graph, id, from, to, type);
	}

	private byte[] createEdgeId(Vertex from, Vertex to) {
		byte[] buff = new byte[from.getId().length+to.getId().length+1];
		System.arraycopy(from.getId(), 0, buff, 0, from.getId().length);
		System.arraycopy(EDGE_CONJ, 0, buff, from.getId().length, 1);
		System.arraycopy(to.getId(), 0, buff, from.getId().length+1, to.getId().length);
		
		return buff;
	}
}