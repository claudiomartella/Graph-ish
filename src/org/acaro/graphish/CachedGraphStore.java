package org.acaro.graphish;

import org.acaro.stagedgraphish.PropertyContainer;

/*
 * Read/Write Through Cache with Storage fallback
 * 
 */

public class CachedGraphStore implements PropertyStore {
	private PropertyStore fallback;
	private GraphStoreCache cache;
	
	public CachedGraphStore(GraphStoreCache cache, PropertyStore fallback) {
		this.fallback = fallback;
		this.cache    = cache;
	}
	
	public Vertex createVertex(Graphish graph) {
		return fallback.createVertex(graph);
	}

	public Edge createEdge(Graphish graph, Vertex from, Vertex to, String type) {
		return fallback.createEdge(graph, from, to, type);
	}

	public boolean hasProperty(PropertyContainer container, String key) {
		PropertyContainer p = checkAndSet(container);

		return p.hasProperty(key);
	}

	public byte[] setProperty(PropertyContainer container, String key,
			byte[] value) {
		PropertyContainer p = checkAndSet(container);
		p.setProperty(key, value);
		
		return fallback.setProperty(container, key, value); 
	}

	public byte[] getProperty(PropertyContainer container, String key) {
		PropertyContainer p = checkAndSet(container);
		
		return p.getProperty(key);
	}

	public byte[] removeProperty(PropertyContainer container, String key) {
		PropertyContainer p = checkAndSet(container);
		p.removeProperty(key);
		
		return fallback.removeProperty(container, key);
	}

	public Iterable<String> getPropertyKeys(PropertyContainer container) {
		PropertyContainer p = checkAndSet(container);
		
		return p.getPropertyKeys();
	}

	public Iterable<byte[]> getPropertyValues(PropertyContainer container) {
		PropertyContainer p = checkAndSet(container);
		
		return p.getPropertyValues();
	}

	public PropertyContainer getPropertyContainer(PropertyContainer container) {
		return checkAndSet(container);
	}
	
	private PropertyContainer checkAndSet(PropertyContainer container) {
		PropertyContainer p;
		if((p = cache.getPropertyContainer(container))== null){
			p = fallback.getPropertyContainer(container);
			cache.setPropertyContainer(p);
		}

		return p;
	}
}