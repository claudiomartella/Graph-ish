package org.acaro.stagedgraphish.operations.hbase;

import java.io.IOException;
import java.util.concurrent.Callable;

import org.acaro.stagedgraphish.StorageException;
import org.acaro.stagedgraphish.Vertex;
import org.acaro.stagedgraphish.VertexImpl;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;

public class CreateVertex implements Callable<Vertex> {

	public Vertex call() throws Exception {
		HTableInterface table = HBaseStore.getTable();
		byte[] id;
		Put p;
		
		try {
			do {
				id = IDsHelper.createVertexId();
				p = new Put(id);
				p.add(HBaseStore.GRAPHMETA_FAM, HBaseStore.ID_QUAL, id);
			} while(!table.checkAndPut(id, HBaseStore.GRAPHMETA_FAM, HBaseStore.ID_QUAL, null, p));
		} catch(IOException e){
			throw new StorageException(e);
		} finally {
			HBaseStore.putTable(table);
		}
		
		return new VertexImpl(id);
	}
}
