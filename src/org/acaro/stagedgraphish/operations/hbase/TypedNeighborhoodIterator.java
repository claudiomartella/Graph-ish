package org.acaro.stagedgraphish.operations.hbase;

import java.util.List;
import java.util.concurrent.ExecutionException;

import org.acaro.stagedgraphish.Direction;
import org.acaro.stagedgraphish.Edge;
import org.acaro.stagedgraphish.Vertex;
import org.acaro.stagedgraphish.operations.EdgeFilter;
import org.acaro.stagedgraphish.operations.Stages;

public class TypedNeighborhoodIterator extends NeighborhoodIterator{
	
	public TypedNeighborhoodIterator(Vertex v, Direction direction, String type){
		super(v, direction, type);
	}

	@Override
	protected List<Edge> fetchResults(int size) throws InterruptedException, ExecutionException {
		EdgeFilter filter = new EdgeFilter(vertex);
		if(last != null) filter.setLast(last);
		filter.setSize(size);
		filter.setType(type);
		filter.setDirection(direction);

		return Stages.getStore().addOperationGetNeighbors(filter).get();
	}
}