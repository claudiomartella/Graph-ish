package org.acaro.stagedgraphish.operations.hbase;

import java.util.Iterator;
import java.util.List;

import org.acaro.stagedgraphish.PropertyContainer;

public abstract class PropertyIterator<T> implements Iterable<T>, Iterator<T> {
	private List<T> items;
	private Iterator<T> iterator;
	
	public PropertyIterator(PropertyContainer p){
		items = fetchIterator(p);
		iterator = items.iterator();
	}
	
	public boolean hasNext() {
		return iterator.hasNext();
	}

	public T next() {
		return iterator.next();
	}

	public void remove() {
		throw new UnsupportedOperationException("VertexPropertiesKeysIterator doesnt support remove()");
	}

	public Iterator<T> iterator() {
		return this;
	}
	
	abstract protected List<T> fetchIterator(PropertyContainer p);
}
