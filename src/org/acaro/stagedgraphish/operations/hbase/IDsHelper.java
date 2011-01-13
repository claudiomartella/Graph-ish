package org.acaro.stagedgraphish.operations.hbase;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import org.acaro.stagedgraphish.Direction;
import org.acaro.stagedgraphish.Edge;
import org.acaro.stagedgraphish.Vertex;
import org.apache.hadoop.hbase.util.Bytes;

public class IDsHelper {
	final private static byte[] EDGE_CONJ = { 0x2b };
	final private static byte[] DIR_IN    = { 0x49 };
	final private static byte[] DIR_OUT   = { 0x4f };
	
	static byte[] createVertexId(){
		UUID id = UUID.randomUUID();
		return org.acaro.graphish.Bytes.fromUuid(id).toByteArray();
	}
	
	static byte[] createEdgeId(){
		UUID id = UUID.randomUUID();
		return org.acaro.graphish.Bytes.fromUuid(id).toByteArray();
	}
	
	static void incrementKey(byte[] id, int index){
		if(id[index] == Byte.MAX_VALUE){
			id[index] = 0;
			if(index > 0){
				incrementKey(id, index - 1);
			}
		} else {
			id[index]++;
		}
	}
	
	static byte[] createRecordId(List<byte[]> tokens){
		int totalLength = 0;
		byte[] id;
		
		for(byte[] token: tokens){
			totalLength += token.length;
		}
		
		id = new byte[totalLength];
		int offset = 0;
		for(byte[]token: tokens){
			System.arraycopy(token, 0, id, offset, token.length);
			offset += token.length;
		}
		
		return id;
	}
	
	static List<byte[]> createEdgeLabels(Vertex from, Vertex to, String type){
		List<byte[]> labels = new ArrayList<byte[]>(2);
		List<byte[]> args   = new ArrayList<byte[]>(7);
		
		args.add(from.getId());
		args.add(EDGE_CONJ);
		args.add(to.getId());
		args.add(EDGE_CONJ);
		args.add(DIR_OUT);
		args.add(EDGE_CONJ);
		args.add(Bytes.toBytes(type));
		
		labels.add(createRecordId(args));
		
		args.clear();
		args.add(to.getId());
		args.add(EDGE_CONJ);
		args.add(from.getId());
		args.add(EDGE_CONJ);
		args.add(DIR_IN);
		args.add(EDGE_CONJ);
		args.add(Bytes.toBytes(type));
		
		labels.add(createRecordId(args));
		
		return labels;
	}
	
	static List<byte[]> createEdgeLabels(Edge edge){
		return createEdgeLabels(edge.getFrom(), edge.getTo(), edge.getType());
	}
	
	static byte[] createEdgePrefix(Vertex v, Direction direction){
		List<byte[]> args = new LinkedList<byte[]>();
		
		args.add(v.getId());
		args.add(EDGE_CONJ);
		args.add((direction.equals(Direction.IN)) ? DIR_IN: DIR_OUT);
		
		return createRecordId(args);
	}
	
	static byte[] createTypedEdgePrefix(Vertex v, Direction direction, String type){
		List<byte[]> args = new LinkedList<byte[]>();
		
		args.add(v.getId());
		args.add(EDGE_CONJ);
		args.add((direction.equals(Direction.IN)) ? DIR_IN: DIR_OUT);
		args.add(EDGE_CONJ);
		args.add(Bytes.toBytes(type));
		
		return createRecordId(args);
	}
}