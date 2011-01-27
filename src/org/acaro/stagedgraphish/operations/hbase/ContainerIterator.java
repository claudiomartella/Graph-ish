package org.acaro.stagedgraphish.operations.hbase;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;

public abstract class ContainerIterator<T> implements Iterable<T>, Iterator<T> {
	private LinkedList<T> resultsCache = new LinkedList<T>();
	private boolean hasNext = true, finished = false;
	protected T last = null;
	protected int bucketSize = 1000;
	
	public boolean hasNext(){
		return hasNext;
	}

	public T next(){
		if(!hasNext) throw new NoSuchElementException();
		
		T item = resultsCache.removeFirst();
		if(resultsCache.size() == 0){
			if(!finished){
				hasNext = fillCache(bucketSize);
			} else {
				hasNext = false;
			}
		}
		
		return item;
	}

	public void remove(){
		throw new UnsupportedOperationException("RecordsIterator doesnt support remove()");
	}

	public Iterator<T> iterator(){
		return this;
	}
	
	abstract protected List<T> fetchResults(int size);

	private boolean fillCache(int size){
		boolean ret = false;
		List<T> records;
		
		records = fetchResults(size);
		
		if(records.size() != 0){
			resultsCache.addAll(records);
			last = resultsCache.getLast();
			ret  = true;
			if(records.size() < size) finished = true;	
		}

		return ret;
	}
}
