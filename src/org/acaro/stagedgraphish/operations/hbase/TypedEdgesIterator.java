package org.acaro.stagedgraphish.operations.hbase;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;

import org.acaro.stagedgraphish.Direction;
import org.acaro.stagedgraphish.Edge;
import org.acaro.stagedgraphish.StorageException;
import org.acaro.stagedgraphish.Vertex;
import org.acaro.stagedgraphish.operations.Stages;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;

public class TypedEdgesIterator extends EdgesIterator{
		
	public TypedEdgesIterator(Vertex v, Direction direction, String type){
		super(v, direction);
	}

	@Override
	protected List<Edge> fetchResults(int size) {
		return Stages.getStore().addOperationGetEdges(vertex, direction, type, last, size);
	}
}