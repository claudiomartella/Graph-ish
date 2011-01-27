package org.acaro.stagedgraphish.operations.hbase;

import java.util.List;

import org.acaro.stagedgraphish.PropertyContainer;
import org.acaro.stagedgraphish.Vertex;
import org.acaro.stagedgraphish.operations.Stages;

public class VertexPropertyValuesIterator extends PropertyIterator<byte[]> {

	public VertexPropertyValuesIterator(Vertex v) {
		super(v);
	}

	@Override
	protected List<byte[]> fetchIterator(PropertyContainer p) {
		return Stages.getStore().addOperationVertexGetPropertyValues((Vertex) p).get();
	}
}
