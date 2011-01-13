package org.acaro.stagedgraphish.operations.hbase;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.acaro.stagedgraphish.Edge;
import org.acaro.stagedgraphish.StorageException;
import org.acaro.stagedgraphish.Vertex;
import org.acaro.stagedgraphish.operations.StorageStage;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseStore implements StorageStage {
	final protected static byte[] GRAPHISH_TABLE  = Bytes.toBytes("Graphish");
	// column families
	final protected static byte[] VPROPERTIES_FAM = Bytes.toBytes("VertexProperties");
	final protected static byte[] EPROPERTIES_FAM = Bytes.toBytes("EdgeProperties");
	final protected static byte[] GRAPHMETA_FAM   = Bytes.toBytes("GraphMetadata");
	final protected static byte[] EDGES_FAM = Bytes.toBytes("Edges");
	// qualifiers
	final protected static byte[] FROM_QUAL = Bytes.toBytes("from");
	final protected static byte[] TYPE_QUAL = Bytes.toBytes("type");
	final protected static byte[] ID_QUAL   = Bytes.toBytes("id");
	final protected static byte[] TO_QUAL   = Bytes.toBytes("to");
	
	private static HTablePool pool = new HTablePool();
	private ExecutorService es = Executors.newFixedThreadPool(5);
	
	static {
		try {
			HBaseAdmin admin = new HBaseAdmin(HBaseConfiguration.create());
			if(!admin.tableExists(GRAPHISH_TABLE)){
				HTableDescriptor table = new HTableDescriptor(GRAPHISH_TABLE);
				HColumnDescriptor family1 = new HColumnDescriptor(EPROPERTIES_FAM);
				HColumnDescriptor family2 = new HColumnDescriptor(VPROPERTIES_FAM);
				HColumnDescriptor family3 = new HColumnDescriptor(GRAPHMETA_FAM);
				HColumnDescriptor family4 = new HColumnDescriptor(EDGES_FAM);
				table.addFamily(family1);
				table.addFamily(family2);
				table.addFamily(family3);
				table.addFamily(family4);
				admin.createTable(table);
			}
			if(!admin.isTableEnabled(GRAPHISH_TABLE)){
				throw new StorageException("Graphish Table not enabled, can't go further");
			}
		} catch(IOException e){
			throw new StorageException(e);
		}	
	}
	
	public static HTableInterface getTable(){
		return pool.getTable(GRAPHISH_TABLE);
	}
	
	public static void putTable(HTableInterface table){
		pool.putTable(table);
	}
	
	public static boolean recordExists(HTableInterface table, byte[] id) throws IOException {
		Get g = new Get(id);

		return table.exists(g);
	}
	
	public Future<Boolean> addOperationVertexHasProperty(Vertex v, String key) {
		Callable<Boolean> operation = new ContainerHasProperty(VPROPERTIES_FAM, v.getId(), key);
		
		return es.submit(operation);
	}

	public Future<Void> addOperationVertexSetProperty(Vertex v, String key,
			byte[] value) {
		Callable<Void> operation = new ContainerSetProperty(VPROPERTIES_FAM, v.getId(), key, value);

		return es.submit(operation);
	}

	public Future<byte[]> addOperationVertexGetProperty(Vertex v, String key) {
		Callable<byte[]> operation = new ContainerGetProperty(VPROPERTIES_FAM, v.getId(), key);

		return es.submit(operation);
	}

	public Future<byte[]> addOperationVertexRemoveProperty(Vertex v, String key) {
		Callable<byte[]> operation = new ContainerRemoveProperty(VPROPERTIES_FAM, v.getId(), key);

		return es.submit(operation);
	}

	public Future<Iterable<String>> addOperationVertexGetPropertyKeys(Vertex v) {
		Callable<Iterable<String>> operation = new ContainerGetPropertyKeys(VPROPERTIES_FAM, v.getId());

		return es.submit(operation);
	}

	public Future<Iterable<byte[]>> addOperationVertexGetPropertyValues(Vertex v) {
		Callable<Iterable<byte[]>> operation = new ContainerGetPropertyValues(VPROPERTIES_FAM, v.getId());

		return es.submit(operation);
	}

	public Future<Boolean> addOperationEdgeHasProperty(Edge e, String key) {
		Callable<Boolean> operation = new ContainerHasProperty(EPROPERTIES_FAM, e.getId(), key);
		
		return es.submit(operation);
	}

	public Future<Void> addOperationEdgeSetProperty(Edge e, String key,
			byte[] value) {
		Callable<Void> operation = new ContainerSetProperty(EPROPERTIES_FAM, e.getId(), key, value);

		return es.submit(operation);
	}

	public Future<byte[]> addOperationEdgeGetProperty(Edge e, String key) {
		Callable<byte[]> operation = new ContainerGetProperty(EPROPERTIES_FAM,e.getId(), key);

		return es.submit(operation);
	}

	public Future<byte[]> addOperationEdgeRemoveProperty(Edge e, String key) {
		Callable<byte[]> operation = new ContainerRemoveProperty(EPROPERTIES_FAM, e.getId(), key);

		return es.submit(operation);
	}

	public Future<Iterable<String>> addOperationEdgeGetPropertyKeys(Edge e) {
		Callable<Iterable<String>> operation = new ContainerGetPropertyKeys(EPROPERTIES_FAM, e.getId());

		return es.submit(operation);
	}

	public Future<Iterable<byte[]>> addOperationEdgeGetPropertyValues(Edge e) {
		Callable<Iterable<byte[]>> operation = new ContainerGetPropertyValues(EPROPERTIES_FAM, e.getId());

		return es.submit(operation);
	}

	public Future<Edge> addOperationVertexPutOutgoingEdge(Vertex v,
			Vertex other, String type) {
		Callable<Edge> operation = new CreateEdge(v, other, type);
		
		return es.submit(operation);
	}

	public Future<Iterable<Edge>> addOperationVertexGetOutgoingEdges(Vertex v,
			String type) {
		Callable<Iterable<Edge>> operation = new VertexGetOutgoingEdges(v, type);
		
		return es.submit(operation);
	}

	public Future<Edge> addOperationVertexPutIncomingEdge(Vertex v,
			Vertex other, String type) {
		Callable<Edge> operation = new CreateEdge(other, v, type);
		
		return es.submit(operation);
	}

	public Future<Iterable<Edge>> addOperationVertexGetIncomingEdges(Vertex v,
			String type) {
		Callable<Iterable<Edge>> operation = new VertexGetIncomingEdges(v, type);
		
		return es.submit(operation);
	}

	public Iterable<String> getIterableVertexPropertyKeys(Vertex v) {
		// TODO Auto-generated method stub
		return null;
	}

	public Iterable<byte[]> getIterableVertexPropertyValues(Vertex v) {
		// TODO Auto-generated method stub
		return null;
	}

	public Iterable<Edge> getIterableVertexIncomingEdges(Vertex v, String type) {
		// TODO Auto-generated method stub
		return null;
	}

	public Iterable<Edge> getIterableVertexOutgoingEdges(Vertex v, String type) {
		// TODO Auto-generated method stub
		return null;
	}
}