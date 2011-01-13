package org.acaro.stagedgraphish;

import java.util.Map;

import org.apache.commons.collections.map.LRUMap;

/*
 * Should make this smarter. Maybe solr's (http://svn.apache.org/viewvc/lucene/dev/trunk/solr/src/common/org/apache/solr/common/util/ConcurrentLRUCache.java?view=markup)
 */

public class LRUGraphStoreCache implements GraphStoreCache {
	private final int MAX_SIZE = 1000;
	LRUMap cache = new LRUMap(MAX_SIZE);
	
	public PropertyContainer getPropertyContainer(PropertyContainer container) {
		return (PropertyContainer) cache.get(container.getId());
	}

	public PropertyContainer setPropertyContainer(PropertyContainer container) {
		return (PropertyContainer) cache.put(container.getId(), container);
	}
}