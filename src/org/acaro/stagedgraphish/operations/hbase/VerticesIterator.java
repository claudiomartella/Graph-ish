package org.acaro.stagedgraphish.operations.hbase;

import java.util.LinkedList;
import java.util.List;

import org.acaro.stagedgraphish.Vertex;
import org.acaro.stagedgraphish.operations.ContainerFilter;
import org.acaro.stagedgraphish.operations.Stages;

public class VerticesIterator extends ContainerIterator<Vertex> {

	@Override
	protected List<Vertex> fetchResults(int size) {
		ContainerFilter filter = new ContainerFilter();
		if(last != null) filter.setLast(last);
		filter.setSize(size);
		
		try {
			return Stages.getStore().addOperationGetVertices(filter).get();
		} catch (Exception e) {
			return new LinkedList<Vertex>();
		}	
	}
}
