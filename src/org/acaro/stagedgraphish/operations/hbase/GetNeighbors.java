package org.acaro.stagedgraphish.operations.hbase;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.acaro.stagedgraphish.Edge;
import org.acaro.stagedgraphish.EdgeImpl;
import org.acaro.stagedgraphish.StorageException;
import org.acaro.stagedgraphish.VertexImpl;
import org.acaro.stagedgraphish.operations.EdgeFilter;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class GetNeighbors implements Callable<List<Edge>> {
	private EdgeFilter filter;
	
	public GetNeighbors(EdgeFilter filter){
		this.filter = filter;
	}
	
	public List<Edge> call() throws Exception {
		LinkedList<Edge> edges = new LinkedList<Edge>();
		Result[] results;
		ResultScanner scanner = null;
		HTableInterface table = HBaseStore.getInstance().getTable();
	
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
			HBaseStore.getInstance().putTable(table);
		}
		
		return edges;
	}

	private Scan getScan(EdgeFilter filter){
		byte[] prefix = null;
		Scan scan = new Scan();
		Edge edge;
		if((edge = filter.getLast())!= null){
			Map<Labels, byte[]> labels = IDsHelper.createEdgeLabels(edge);
			byte[] startRow = labels.get(Labels.DIRECT);
			IDsHelper.incrementKey(startRow, startRow.length-1);
			scan.setStartRow(startRow);
		} else {
			scan.setStartRow(filter.getVertex().getId());
		}
		byte[] stopRow = filter.getVertex().getId().clone();
		IDsHelper.incrementKey(stopRow, stopRow.length-1);
		scan.setStopRow(stopRow);// stop at next vertex
		
		if(filter.getType() == null){
			prefix = IDsHelper.createEdgePrefix(filter.getVertex(), filter.getDirection());
		} else {
			prefix = IDsHelper.createTypedEdgePrefix(filter.getVertex(), filter.getDirection(), filter.getType());
		}
		
		Filter hfilter = new PrefixFilter(prefix);
		scan.setFilter(hfilter);
		
		return scan;
	}
}