package org.acaro.stagedgraphish.operations.hbase;

import java.util.LinkedList;
import java.util.List;

import org.acaro.stagedgraphish.Edge;
import org.acaro.stagedgraphish.PropertyContainer;
import org.acaro.stagedgraphish.operations.Stages;

public class EdgePropertyKeysIterator extends PropertyIterator<String> {

	public EdgePropertyKeysIterator(Edge v) {
		super(v);
	}

	@Override
	protected List<String> fetchIterator(PropertyContainer p) {
		try {
			return Stages.getStore().addOperationEdgeGetPropertyKeys((Edge) p).get();
		} catch (Exception e) {
			return new LinkedList<String>();
		}
	}
}
