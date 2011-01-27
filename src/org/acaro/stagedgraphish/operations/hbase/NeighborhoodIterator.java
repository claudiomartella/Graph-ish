package org.acaro.stagedgraphish.operations.hbase;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;

import org.acaro.stagedgraphish.Direction;
import org.acaro.stagedgraphish.Edge;
import org.acaro.stagedgraphish.Vertex;

public abstract class NeighborhoodIterator implements Iterable<Edge>, Iterator<Edge> {
	private LinkedList<Edge> resultsCache = new LinkedList<Edge>();
	private boolean hasNext = true, finished = false;
	protected Edge last = null;
	protected Vertex vertex;
	protected Direction direction = null;
	protected String type = null;
	protected int bucketSize = 1000;
	
	public NeighborhoodIterator(Vertex v, Direction direction) {
		this.vertex    = v;
		this.direction = direction;
		hasNext = fillCache(1);
	}
	
	public NeighborhoodIterator(Vertex v, Direction direction, String type) {
		this.vertex    = v;
		this.direction = direction;
		this.type      = type;
		hasNext = fillCache(1);
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
		throw new UnsupportedOperationException("NeighborhoodIterator doesnt support remove(), use Vertex.removeEdge() instead");
	}

	public Iterator<Edge> iterator(){
		return this;
	}

	abstract protected List<Edge> fetchResults(int size);

	private boolean fillCache(int size){
		boolean ret = false;
		List<Edge> edges;
		
		edges = fetchResults(size);
		
		if(edges.size() != 0){
			resultsCache.addAll(edges);
			last = resultsCache.getLast();
			ret  = true;
			if(edges.size() < size)	finished = true;	
		}

		return ret;
	}
}