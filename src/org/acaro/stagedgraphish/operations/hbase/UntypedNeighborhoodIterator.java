package org.acaro.stagedgraphish.operations.hbase;

import java.util.LinkedList;
import java.util.List;

import org.acaro.stagedgraphish.Direction;
import org.acaro.stagedgraphish.Edge;
import org.acaro.stagedgraphish.Vertex;
import org.acaro.stagedgraphish.operations.EdgeFilter;
import org.acaro.stagedgraphish.operations.Stages;

public class UntypedNeighborhoodIterator extends NeighborhoodIterator {
	
	public UntypedNeighborhoodIterator(Vertex v, Direction direction) {
		super(v, direction);
	}

	@Override
	protected List<Edge> fetchResults(int size) {
		EdgeFilter filter = new EdgeFilter(vertex);
		if(last != null) filter.setLast(last);
		filter.setSize(size);
		filter.setDirection(direction);
		
		try {
			return Stages.getStore().addOperationGetNeighbors(filter).get();
		} catch (Exception e) {
			return new LinkedList<Edge>();
		}	
	}
}