package org.acaro.stagedgraphish.operations.hbase;

import java.util.List;
import java.util.concurrent.ExecutionException;

import org.acaro.stagedgraphish.Edge;
import org.acaro.stagedgraphish.operations.ContainerFilter;
import org.acaro.stagedgraphish.operations.Stages;

public class EdgesIterator extends ContainerIterator<Edge> {

	@Override
	protected List<Edge> fetchResults(int size) throws InterruptedException,
			ExecutionException {
		ContainerFilter filter = new ContainerFilter();
		if(last != null) filter.setLast(last);
		filter.setSize(size);

		return Stages.getStore().addOperationGetEdges(filter).get();	
	}
}
