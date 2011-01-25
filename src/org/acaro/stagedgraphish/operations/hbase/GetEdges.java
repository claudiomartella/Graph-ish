package org.acaro.stagedgraphish.operations.hbase;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;

import org.acaro.stagedgraphish.Edge;
import org.acaro.stagedgraphish.EdgeImpl;
import org.acaro.stagedgraphish.StorageException;
import org.acaro.stagedgraphish.VertexImpl;
import org.acaro.stagedgraphish.operations.ContainerFilter;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class GetEdges implements Callable<List<Edge>> {
	private ContainerFilter filter;

	public GetEdges(ContainerFilter filter){
		this.filter = filter;
	}
	
	public List<Edge> call() throws Exception {
		LinkedList<Edge> edges = new LinkedList<Edge>();
		Result[] results;
		ResultScanner scanner = null;
		HTableInterface table = HBaseStore.getTable();
	
		try {
			Scan scan = getScan(filter);
			scanner = table.getScanner(scan);
			results = scanner.next(filter.getSize());
			for(Result result: results){
				byte[] id   = result.getValue(HBaseStore.GRAPHMETA_FAM, HBaseStore.ID_QUAL);
				byte[] from = result.getValue(HBaseStore.GRAPHMETA_FAM, HBaseStore.FROM_QUAL);
				byte[] to   = result.getValue(HBaseStore.GRAPHMETA_FAM, HBaseStore.TO_QUAL);
				byte[] type = result.getValue(HBaseStore.GRAPHMETA_FAM, HBaseStore.TYPE_QUAL);
				Edge edge   = new EdgeImpl(id, new VertexImpl(from), 
						new VertexImpl(to), Bytes.toString(type));
				edges.addLast(edge);
			}
		} catch(IOException e){
			throw new StorageException(e);
		} finally {
			scanner.close();
			HBaseStore.putTable(table);
		}
		
		return edges;
	}
	
	private Scan getScan(ContainerFilter filter){
		Scan scan = new Scan();
		if(filter.getLast() != null){
			byte[] startRow = filter.getLast().getId().clone();
			IDsHelper.incrementKey(startRow, startRow.length-1);
			scan.setStartRow(startRow);
		}
		
		return scan;
	}
}