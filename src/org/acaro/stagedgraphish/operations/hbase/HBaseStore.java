package org.acaro.stagedgraphish.operations.hbase;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.acaro.stagedgraphish.Direction;
import org.acaro.stagedgraphish.Edge;
import org.acaro.stagedgraphish.StorageException;
import org.acaro.stagedgraphish.Vertex;
import org.acaro.stagedgraphish.operations.ContainerFilter;
import org.acaro.stagedgraphish.operations.EdgeFilter;
import org.acaro.stagedgraphish.operations.StorageStage;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;
import org.apache.hadoop.hbase.regionserver.StoreFile.BloomType;
import org.apache.hadoop.hbase.util.Bytes;

public final class HBaseStore implements StorageStage {
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
	
	private HTablePool pool = new HTablePool();
	private ExecutorService es = Executors.newFixedThreadPool(5);
	
	private static class HBaseStoreHolder {
		public static final HBaseStore INSTANCE = new HBaseStore();
	}
	
	private HBaseStore(){
		try {
			HBaseAdmin admin = new HBaseAdmin(HBaseConfiguration.create());
			if(!admin.tableExists(GRAPHISH_TABLE)){
				HTableDescriptor table = new HTableDescriptor(GRAPHISH_TABLE);
				HColumnDescriptor family1 = new HColumnDescriptor(EPROPERTIES_FAM);
				family1.setBlockCacheEnabled(true);
				family1.setBloomFilterType(BloomType.ROWCOL);
				family1.setCompressionType(Algorithm.GZ);
				HColumnDescriptor family2 = new HColumnDescriptor(VPROPERTIES_FAM);
				family2.setBlockCacheEnabled(true);
				family2.setBloomFilterType(BloomType.ROWCOL);
				family2.setCompressionType(Algorithm.GZ);
				HColumnDescriptor family3 = new HColumnDescriptor(GRAPHMETA_FAM);
				family3.setBlockCacheEnabled(true);
				family3.setBloomFilterType(BloomType.ROWCOL);
				family3.setCompressionType(Algorithm.GZ);
				HColumnDescriptor family4 = new HColumnDescriptor(EDGES_FAM);
				family4.setBlockCacheEnabled(true);
				family4.setBloomFilterType(BloomType.ROW);
				family4.setCompressionType(Algorithm.GZ);
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
	
	public static HBaseStore getInstance() {
		return HBaseStoreHolder.INSTANCE;
	}
	
	HTableInterface getTable(){
		return pool.getTable(GRAPHISH_TABLE);
	}
	
	void putTable(HTableInterface table){
		pool.putTable(table);
	}
	
	static boolean recordExists(HTableInterface table, byte[] id) throws IOException {
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

	public Future<List<String>> addOperationVertexGetPropertyKeys(Vertex v) {
		Callable<List<String>> operation = new ContainerGetPropertyKeys(VPROPERTIES_FAM, v.getId());

		return es.submit(operation);
	}

	public Future<List<byte[]>> addOperationVertexGetPropertyValues(Vertex v) {
		Callable<List<byte[]>> operation = new ContainerGetPropertyValues(VPROPERTIES_FAM, v.getId());

		return es.submit(operation);
	}

	public Future<Boolean> addOperationEdgeHasProperty(Edge e, String key) {
		Callable<Boolean> operation = new ContainerHasProperty(EPROPERTIES_FAM, e.getId(), key);
		
		return es.submit(operation);
	}

	public Future<Void> addOperationEdgeSetProperty(Edge e, String key,
			final byte[] value) {
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

	public Future<List<String>> addOperationEdgeGetPropertyKeys(Edge e) {
		Callable<List<String>> operation = new ContainerGetPropertyKeys(EPROPERTIES_FAM, e.getId());

		return es.submit(operation);
	}

	public Future<List<byte[]>> addOperationEdgeGetPropertyValues(Edge e) {
		Callable<List<byte[]>> operation = new ContainerGetPropertyValues(EPROPERTIES_FAM, e.getId());

		return es.submit(operation);
	}

	public Future<Edge> addOperationVertexPutOutgoingEdge(Vertex v,
			Vertex other, String type) {
		Callable<Edge> operation = new CreateEdge(v, other, type);
		
		return es.submit(operation);
	}

	public Future<Edge> addOperationVertexPutIncomingEdge(Vertex v,
			Vertex other, String type) {
		Callable<Edge> operation = new CreateEdge(other, v, type);
		
		return es.submit(operation);
	}
	
	public Future<List<Edge>> addOperationGetNeighbors(EdgeFilter filter) {
		Callable<List<Edge>> operation = new GetNeighbors(filter);
		
		return es.submit(operation);
	}
	
	public Future<List<Edge>> addOperationGetEdges(ContainerFilter filter) {
		Callable<List<Edge>> operation = new GetEdges(filter);
		
		return es.submit(operation);
	}
	
	public Future<List<Vertex>> addOperationGetVertices(ContainerFilter filter) {
		Callable<List<Vertex>> operation = new GetVertices(filter);
		
		return es.submit(operation);
	}

	public Iterable<String> getIterableVertexPropertyKeys(Vertex v) {
		return new VertexPropertyKeysIterator(v);
	}

	public Iterable<byte[]> getIterableVertexPropertyValues(Vertex v) {
		return new VertexPropertyValuesIterator(v);
	}
	
	public Iterable<String> getIterableEdgePropertyKeys(Edge e) {
		return new EdgePropertyKeysIterator(e);
	}

	public Iterable<byte[]> getIterableEdgePropertyValues(Edge e) {
		return new EdgePropertyValuesIterator(e);
	}

	public Iterable<Edge> getIterableVertexIncomingEdges(Vertex v, String type) {
		return new TypedNeighborhoodIterator(v, Direction.IN, type);
	}

	public Iterable<Edge> getIterableVertexOutgoingEdges(Vertex v, String type) {
		return new TypedNeighborhoodIterator(v, Direction.OUT, type);
	}

	public Iterable<Edge> getIterableVertexIncomingEdges(Vertex v) {
		return new UntypedNeighborhoodIterator(v, Direction.IN);
	}

	public Iterable<Edge> getIterableVertexOutgoingEdges(Vertex v) {
		return new UntypedNeighborhoodIterator(v, Direction.OUT);
	}
	
	public Iterable<Edge> getIterableEdges() {
		return new EdgesIterator();
	}

	public Iterable<Vertex> getIterableVertices() {
		return new VerticesIterator();
	}
	
	public Future<Vertex> addOperationCreateVertex() {
		Callable<Vertex> operation = new CreateVertex();
		
		return es.submit(operation);
	}

	public Future<Vertex> addOperationGetVertex(byte[] id) {
		Callable<Vertex> operation = new GetVertex(id);
		
		return es.submit(operation);
	}

	public Future<Void> addOperationRemoveVertex(Vertex v) {
		Callable<Void> operation = new RemoveVertex(v);
		
		return es.submit(operation);
	}
	
	public Future<Void> addOperationRemoveEdge(Edge e) {
		Callable<Void> operation = new RemoveEdge(e);
		
		return es.submit(operation);
	}

	public Future<Edge> addOperationGetEdge(byte[] id) {
		Callable<Edge> operation = new GetEdge(id);
		
		return es.submit(operation);
	}
}