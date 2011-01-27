package org.acaro.stagedgraphish.operations.hbase;

import java.util.LinkedList;
import java.util.List;

import org.acaro.stagedgraphish.Edge;
import org.acaro.stagedgraphish.operations.ContainerFilter;
import org.acaro.stagedgraphish.operations.Stages;

public class EdgesIterator extends ContainerIterator<Edge> {

	@Override
	protected List<Edge> fetchResults(int size) {
		ContainerFilter filter = new ContainerFilter();
		if(last != null) filter.setLast(last);
		filter.setSize(size);

		try {
			return Stages.getStore().addOperationGetEdges(filter).get();
		} catch (Exception e) {
			return new LinkedList<Edge>();
		}	
	}
}
