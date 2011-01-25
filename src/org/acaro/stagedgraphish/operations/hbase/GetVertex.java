package org.acaro.stagedgraphish.operations.hbase;

import java.io.IOException;
import java.util.concurrent.Callable;

import org.acaro.stagedgraphish.DoesntExist;
import org.acaro.stagedgraphish.StorageException;
import org.acaro.stagedgraphish.Vertex;
import org.acaro.stagedgraphish.VertexImpl;
import org.apache.hadoop.hbase.client.HTableInterface;

public class GetVertex implements Callable<Vertex> {
	private byte[] id;
	
	public GetVertex(byte[] id){
		this.id = id;
	}
	
	public Vertex call() throws Exception {
		HTableInterface table = HBaseStore.getTable();
		
		try {
			if(!HBaseStore.recordExists(table, id)){
				throw new DoesntExist(id);
			}
		} catch(IOException e){
			throw new StorageException(e);
		} finally {
			HBaseStore.putTable(table);
		}
		
		return new VertexImpl(id);
	}
}
