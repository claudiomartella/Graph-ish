package org.acaro.stagedgraphish.operations.hbase;

import java.util.LinkedList;
import java.util.List;

import org.acaro.stagedgraphish.Edge;
import org.acaro.stagedgraphish.PropertyContainer;
import org.acaro.stagedgraphish.operations.Stages;

public class EdgePropertyValuesIterator extends PropertyIterator<byte[]> {

	public EdgePropertyValuesIterator(Edge e) {
		super(e);
	}

	@Override
	protected List<byte[]> fetchIterator(PropertyContainer p) {
		try {
			return Stages.getStore().addOperationEdgeGetPropertyValues((Edge) p).get();
		} catch (Exception e) {
			return new LinkedList<byte[]>();
		}
	}
}
