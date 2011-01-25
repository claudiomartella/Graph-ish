package org.acaro.stagedgraphish.operations.hbase;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;

import org.acaro.stagedgraphish.StorageException;
import org.acaro.stagedgraphish.Vertex;
import org.acaro.stagedgraphish.VertexImpl;
import org.acaro.stagedgraphish.operations.ContainerFilter;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

public class GetVertices implements Callable<List<Vertex>> {
	private ContainerFilter filter;

	public GetVertices(ContainerFilter filter){
		this.filter = filter;
	}
	
	public List<Vertex> call() throws Exception {
		LinkedList<Vertex> vertices = new LinkedList<Vertex>();
		Result[] results;
		ResultScanner scanner = null;
		HTableInterface table = HBaseStore.getTable();
	
		try {
			Scan scan = getScan(filter);
			scanner = table.getScanner(scan);
			results = scanner.next(filter.getSize());
			for(Result result: results){
				byte[] id = result.getValue(HBaseStore.GRAPHMETA_FAM, HBaseStore.ID_QUAL);
				Vertex v  = new VertexImpl(id);
				vertices.addLast(v);
			}
		} catch(IOException e){
			throw new StorageException(e);
		} finally {
			scanner.close();
			HBaseStore.putTable(table);
		}
		
		return vertices;
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