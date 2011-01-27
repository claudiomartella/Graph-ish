package org.acaro.stagedgraphish.operations.hbase;

import java.util.LinkedList;
import java.util.List;

import org.acaro.stagedgraphish.PropertyContainer;
import org.acaro.stagedgraphish.Vertex;
import org.acaro.stagedgraphish.operations.Stages;

public class VertexPropertyKeysIterator extends PropertyIterator<String> {

	public VertexPropertyKeysIterator(Vertex v) {
		super(v);
	}

	@Override
	protected List<String> fetchIterator(PropertyContainer p) {
		try {
			return Stages.getStore().addOperationVertexGetPropertyKeys((Vertex) p).get();
		} catch (Exception e) {
			return new LinkedList<String>();
		}
	}
}
