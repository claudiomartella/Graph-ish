package org.acaro.stagedgraphish.operations.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.acaro.stagedgraphish.Edge;
import org.acaro.stagedgraphish.EdgeExists;
import org.acaro.stagedgraphish.EdgeImpl;
import org.acaro.stagedgraphish.StorageException;
import org.acaro.stagedgraphish.Vertex;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class CreateEdge implements Callable<Edge> {
	private Vertex from;
	private Vertex to;
	private String type;
	
	public CreateEdge(Vertex from, Vertex to, String type){
		this.from = from;
		this.to   = to;
		this.type = type;
	}
	
	public Edge call() throws Exception {
		byte[] id;
		Put p;
		HTableInterface table = HBaseStore.getInstance().getTable();
		List<Put> puts = new ArrayList<Put>();
		Map<Labels, byte[]> labels = IDsHelper.createEdgeLabels(from, to, type);
		
		try {
			if(HBaseStore.recordExists(table, labels.get(Labels.DIRECT))){ // does it make sense to check for the other label?
				throw new EdgeExists(from, to, type);
			}
			byte[] t = Bytes.toBytes(type);
			do {
				id = IDsHelper.createEdgeId();
				p = new Put(id);
				p.add(HBaseStore.GRAPHMETA_FAM, HBaseStore.ID_QUAL, id);
				p.add(HBaseStore.GRAPHMETA_FAM, HBaseStore.FROM_QUAL, from.getId());
				p.add(HBaseStore.GRAPHMETA_FAM, HBaseStore.TO_QUAL, to.getId());
				p.add(HBaseStore.GRAPHMETA_FAM, HBaseStore.TYPE_QUAL, t);
			} while(!table.checkAndPut(id, HBaseStore.GRAPHMETA_FAM, HBaseStore.ID_QUAL, null, p));
			
			p = new Put(labels.get(Labels.DIRECT));
			p.add(HBaseStore.GRAPHMETA_FAM, HBaseStore.ID_QUAL, id);
			p.add(HBaseStore.GRAPHMETA_FAM, HBaseStore.FROM_QUAL, from.getId());
			p.add(HBaseStore.GRAPHMETA_FAM, HBaseStore.TO_QUAL, to.getId());
			p.add(HBaseStore.GRAPHMETA_FAM, HBaseStore.TYPE_QUAL, t);
			puts.add(p);
			
			p = new Put(labels.get(Labels.INVERTED));
			p.add(HBaseStore.GRAPHMETA_FAM, HBaseStore.ID_QUAL, id);
			p.add(HBaseStore.GRAPHMETA_FAM, HBaseStore.FROM_QUAL, from.getId());
			p.add(HBaseStore.GRAPHMETA_FAM, HBaseStore.TO_QUAL, to.getId());
			p.add(HBaseStore.GRAPHMETA_FAM, HBaseStore.TYPE_QUAL, t);
			puts.add(p);
			table.put(puts);
		} catch(IOException e){
			throw new StorageException(e);
		} finally {
			HBaseStore.getInstance().putTable(table);
		}
		
		return new EdgeImpl(id, from, to, type);
	}
}