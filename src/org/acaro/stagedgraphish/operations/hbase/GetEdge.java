package org.acaro.stagedgraphish.operations.hbase;

import java.io.IOException;
import java.util.concurrent.Callable;

import org.acaro.stagedgraphish.DoesntExist;
import org.acaro.stagedgraphish.Edge;
import org.acaro.stagedgraphish.EdgeImpl;
import org.acaro.stagedgraphish.StorageException;
import org.acaro.stagedgraphish.VertexImpl;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class GetEdge implements Callable<Edge> {
	private byte[] id;
	
	public GetEdge(byte[] id){
		this.id = id;
	}
	
	public Edge call() throws Exception {
		HTableInterface table = HBaseStore.getTable();
		Edge edge;
		
		try {
			Get g = new Get(id);
			g.addFamily(HBaseStore.EPROPERTIES_FAM);
			Result res = table.get(g);
			if(res.isEmpty()) throw new DoesntExist(id);
			
			byte[] id   = res.getValue(HBaseStore.GRAPHMETA_FAM, HBaseStore.ID_QUAL);
			byte[] from = res.getValue(HBaseStore.GRAPHMETA_FAM, HBaseStore.FROM_QUAL);
			byte[] to   = res.getValue(HBaseStore.GRAPHMETA_FAM, HBaseStore.TO_QUAL);
			byte[] type = res.getValue(HBaseStore.GRAPHMETA_FAM, HBaseStore.TYPE_QUAL);
			edge   = new EdgeImpl(id, new VertexImpl(from), 
					new VertexImpl(to), Bytes.toString(type));
		} catch(IOException e){
			throw new StorageException(e);
		} finally {
			HBaseStore.putTable(table);
		}
		
		return edge;
	}
}
