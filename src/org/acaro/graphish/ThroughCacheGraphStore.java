package org.acaro.graphish;

import java.util.HashMap;

/*
 * Read/Write Through Cache with Storage fallback
 */

public class ThroughCacheGraphStore implements GraphStore {
	private GraphStore fallback;
	private HashMap<byte[], PropertyContainer> vProps = new HashMap<byte[], PropertyContainer>();
	private HashMap<byte[], PropertyContainer> eProps = new HashMap<byte[], PropertyContainer>();
	
	public ThroughCacheGraphStore(GraphStore fallback){
		this.fallback = fallback;
	}

	public boolean hasProperty(Vertex vertex, String key) {
		PropertyContainer p = checkAndSet(vertex);
		
		return p.hasProperty(key);
	}

	public byte[] setProperty(Vertex vertex, String key, byte[] value) {
		PropertyContainer p = checkAndSet(vertex);
		p.setProperty(key, value);
		
		return fallback.setProperty(vertex, key, value);
	}

	public byte[] getProperty(Vertex vertex, String key) {
		PropertyContainer p = checkAndSet(vertex);
		
		return p.getProperty(key);
	}

	public byte[] removeProperty(Vertex vertex, String key) {
		PropertyContainer p = checkAndSet(vertex);
		p.removeProperty(key);
		
		return fallback.removeProperty(vertex, key);
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
		p.setProperty(key, value);
		
		return fallback.setProperty(edge, key, value);
	}

	public byte[] getProperty(Edge edge, String key) {
		PropertyContainer p = checkAndSet(edge);
		
		return p.getProperty(key);
	}

	public byte[] removeProperty(Edge edge, String key) {
		PropertyContainer p = checkAndSet(edge);
		p.removeProperty(key);
		
		return fallback.removeProperty(edge, key);
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
			p = fallback.getPropertyContainer(vertex);
			vProps.put(vertex.getId(), p);
		}
		
		return p;
	}
	
	private PropertyContainer checkAndSet(Edge edge){
		PropertyContainer p;
		if((p = eProps.get(edge.getId()))== null){
			p = fallback.getPropertyContainer(edge);
			eProps.put(edge.getId(), p);
		}
		
		return p;
	}
}