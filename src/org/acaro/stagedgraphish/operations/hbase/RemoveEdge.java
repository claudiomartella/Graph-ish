package org.acaro.stagedgraphish.operations.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.acaro.stagedgraphish.Edge;
import org.acaro.stagedgraphish.StorageException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTableInterface;

public class RemoveEdge implements Callable<Void> {
	public Edge edge;
	
	public RemoveEdge(Edge edge){
		this.edge = edge;
	}
	
	public Void call() throws Exception {
		HTableInterface table = HBaseStore.getTable();
		List<Delete> deletes = new ArrayList<Delete>();
		
		try {
			Map<Labels, byte[]> labels = IDsHelper.createEdgeLabels(edge);
			deletes.add(new Delete(labels.get(Labels.DIRECT)));
			deletes.add(new Delete(labels.get(Labels.INVERTED)));
			deletes.add(new Delete(edge.getId()));
			table.delete(deletes);
		} catch(IOException e){
			throw new StorageException(e);
		} finally {
			HBaseStore.putTable(table);
		}
		
		return null;
	}
}