package org.acaro.stagedgraphish.operations.hbase;

import java.io.IOException;
import java.util.concurrent.Callable;

import org.acaro.stagedgraphish.StorageException;
import org.acaro.stagedgraphish.Vertex;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTableInterface;

public class RemoveVertex implements Callable<Void> {
	private Vertex v;
	
	public RemoveVertex(Vertex v){
		this.v = v;
	}
	
	public Void call() throws Exception {
		HTableInterface table = HBaseStore.getTable();

		try {
			table.delete(new Delete(v.getId()));
		} catch(IOException e){
			throw new StorageException(e);
		} finally {
			HBaseStore.putTable(table);
		}
		
		return null;
	}
}