package org.acaro.stagedgraphish.operations.hbase;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;

import org.acaro.stagedgraphish.Direction;
import org.acaro.stagedgraphish.Edge;
import org.acaro.stagedgraphish.Vertex;
import org.acaro.stagedgraphish.operations.Stages;

public abstract class EdgesIterator implements Iterable<Edge>, Iterator<Edge> {
	private LinkedList<Edge> resultsCache = new LinkedList<Edge>();
	private boolean hasNext = true, finished = false, initialized = false;
	protected Edge last = null;
	protected Vertex vertex;
	protected Direction direction;
	protected String type;
	protected int bucketSize = 1000;
	
	public EdgesIterator(Vertex v, Direction direction){
		this.vertex    = v;
		this.direction = direction;
	}
	
	public EdgesIterator(Vertex v, Direction direction, String type){
		this(v, direction);
		this.type = type;
	}
	
	public boolean hasNext(){
		return hasNext;
	}

	public Edge next(){
		if(!hasNext) throw new NoSuchElementException();
		
		Edge edge = resultsCache.removeFirst();
		if(resultsCache.size() == 0){
			if(!finished){
				hasNext = fillCache(bucketSize);
			} else {
				hasNext = false;
			}
		} 
		
		return edge;
	}

	public void remove(){
		throw new UnsupportedOperationException("EdgesIterator doesnt support remove(), use Vertex.removeEdge() instead");
	}

	public Iterator<Edge> iterator(){
		return this;
	}
	
	private boolean fillCache(int size){
		boolean ret = false;
		List<Edge> edges = fetchResults(size);
		
		if(edges.size() != 0){
			resultsCache.addAll(edges);
			last = resultsCache.getLast();
			ret  = true;
			if(edges.size() < size)	finished = true;	
		}
		
		return ret;
	}
	
	abstract protected List<Edge> fetchResults(int size);
}