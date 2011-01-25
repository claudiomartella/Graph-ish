package org.acaro.graphish;

import org.acaro.stagedgraphish.PropertyContainer;

public interface GraphStoreCache {
	public PropertyContainer getPropertyContainer(PropertyContainer container);

	public PropertyContainer setPropertyContainer(PropertyContainer container);
}
