package org.acaro.stagedgraphish.operations.hbase;

import java.util.List;
import java.util.concurrent.ExecutionException;

import org.acaro.stagedgraphish.Vertex;
import org.acaro.stagedgraphish.operations.ContainerFilter;
import org.acaro.stagedgraphish.operations.Stages;

public class VerticesIterator extends ContainerIterator<Vertex> {

	@Override
	protected List<Vertex> fetchResults(int size) throws InterruptedException,
			ExecutionException {
		ContainerFilter filter = new ContainerFilter();
		if(last != null) filter.setLast(last);
		filter.setSize(size);
		
		return Stages.getStore().addOperationGetEdges(filter).get();	
	}
}
