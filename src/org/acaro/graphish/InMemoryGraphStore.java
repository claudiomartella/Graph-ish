package org.acaro.graphish;

import java.util.HashMap;
import java.util.UUID;

import org.acaro.stagedgraphish.PropertyContainer;

/*
 * Write stuff to memory and that's it
 * 
 * TODO: should return copies/duplicates?
 */

public class InMemoryGraphStore implements PropertyStore {
	private static final byte[] EDGE_CONJ = { 0x2b };
	private HashMap<byte[], PropertyContainer> props = new HashMap<byte[], PropertyContainer>();
	private HashMap<byte[], Vertex> vertices = new HashMap<byte[], Vertex>();
	
	public boolean hasProperty(PropertyContainer container, String key) {
		return checkAndGet(container).hasProperty(key);
	}

	public byte[] setProperty(PropertyContainer container, String key, byte[] value) {
		return checkAndGet(container).setProperty(key, value);
	}

	public byte[] getProperty(PropertyContainer container, String key) {
		return checkAndGet(container).getProperty(key);
	}

	public byte[] removeProperty(PropertyContainer container, String key) {
		return checkAndGet(container).removeProperty(key);
	}

	public Iterable<String> getPropertyKeys(PropertyContainer container) {
		return checkAndGet(container).getPropertyKeys();
	}

	public Iterable<byte[]> getPropertyValues(PropertyContainer container) {
		return checkAndGet(container).getPropertyValues();
	}

	public PropertyContainer getPropertyContainer(PropertyContainer container) {
		return checkAndGet(container);
	}
	
	public PropertyContainer checkAndGet(PropertyContainer container) {
		PropertyContainer p = props.get(container.getId());
		if(p == null){
			throw new DoesntExist(container);
		}
		return p;
	}

	public Vertex createVertex(Graphish graph) {
		UUID id = UUID.randomUUID();
		
		Vertex v = new VertexImpl(graph, Bytes.fromUuid(id).toByteArray());
		vertices.put(v.getId(), v);
		props.put(v.getId(), new PropertyContainerImpl(v.getId()));
		
		return v;
	}

	public Edge createEdge(Graphish graph, Vertex from, Vertex to, String type) { 
		byte id[] = createEdgeId(from, to, type);
		props.put(id, new PropertyContainerImpl(id));
		
		return new EdgeImpl(graph, id, from, to, type);
	}

	private byte[] createEdgeId(Vertex from, Vertex to, String type) {
		byte[] typeb = Bytes.fromUTF8(type).toByteArray();
		byte[] buff  = new byte[from.getId().length+to.getId().length+1+typeb.length+1];
		// from
		System.arraycopy(from.getId(), 0, buff, 0, from.getId().length);
		// from+
		System.arraycopy(EDGE_CONJ, 0, buff, from.getId().length, 1);
		// from+to
		System.arraycopy(to.getId(), 0, buff, from.getId().length+1, to.getId().length);
		// from+to+
		System.arraycopy(EDGE_CONJ, 0, buff, from.getId().length+to.getId().length+1, 1);
		// from+to+type
		System.arraycopy(typeb, 0, buff, from.getId().length+to.getId().length+2, typeb.length);
		
		return buff;
	}
}