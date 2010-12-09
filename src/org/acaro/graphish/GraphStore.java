package org.acaro.graphish;

public interface GraphStore {

	void deleteVertex(OldVertex v);

	void shutdown();

	Edge retrieveEdge(long id);

	OldVertex retrieveVertex(long id);

	void clear();
}
