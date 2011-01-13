package org.acaro.graphish;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.UUID;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.ScannerTimeoutException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseStore implements PropertyStore, GraphStore {
	// table
	final private byte[] GRAPHISH_TABLE  = Bytes.toBytes("Graphish");
	// column families
	final private byte[] VPROPERTIES_FAM = Bytes.toBytes("VertexProperties");
	final private byte[] EPROPERTIES_FAM = Bytes.toBytes("EdgeProperties");
	final private byte[] GRAPHMETA_FAM   = Bytes.toBytes("GraphMetadata");
	final private byte[] EDGES_FAM = Bytes.toBytes("Edges");
	// qualifiers
	final private byte[] FROM_QUAL = Bytes.toBytes("from");
	final private byte[] TYPE_QUAL = Bytes.toBytes("type");
	final private byte[] ID_QUAL   = Bytes.toBytes("id");
	final private byte[] TO_QUAL   = Bytes.toBytes("to");
	// composite keys tokens
	final private byte[] EDGE_CONJ = { 0x2b };
	final private byte[] DIR_IN    = { 0x49 };
	final private byte[] DIR_OUT   = { 0x4f };
	private enum Direction { IN, OUT };
	private Graphish graph;
	private static HTablePool pool = new HTablePool();
	final private int bucketSize = 100;
	
	public HBaseStore(Graphish graph) {
		this.graph = graph;
		createTable(GRAPHISH_TABLE);
	}
	
	private void createTable(byte[] tName) {
		try {
			HBaseAdmin admin = new HBaseAdmin(HBaseConfiguration.create());
			if(!admin.tableExists(tName)){
				HTableDescriptor table = new HTableDescriptor(tName);
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
			if(!admin.isTableEnabled(tName)){
				throw new StorageException(Bytes.toString(tName)+" Table not enabled, can't go further");
			}
		} catch(IOException e){
			throw new StorageException(e);
		}
	}
	
	/*
	 * GraphStore Interface implementations
	 */
	
	public Vertex createVertex() {
		byte[] id;
		Put p;
		HTableInterface table = pool.getTable(GRAPHISH_TABLE);
		
		try {
			do {
				id = createVertexId();
				p = new Put(id);
				p.add(GRAPHMETA_FAM, ID_QUAL, id);
			} while(!table.checkAndPut(id, GRAPHMETA_FAM, ID_QUAL, null, p));
		} catch(IOException e){
			throw new StorageException(e);
		} finally {
			pool.putTable(table);
		}
		
		return new VertexImpl(graph, id);
	}
	

	public Vertex getVertex(byte[] id) {
		try {
			if(!recordExists(id)){
				throw new DoesntExist(id);
			}
		} catch(IOException e){
			throw new StorageException(e);
		}
		
		return new VertexImpl(graph, id);
	}

	public void removeVertex(Vertex vertex) {
		int i = 0;
		List<Delete> deletes = new LinkedList<Delete>();
		Iterator<Edge> iterator;
		HTableInterface table = pool.getTable(GRAPHISH_TABLE);
		
		try {
			iterator = vertex.getIncomingEdges().iterator();
			for(; iterator.hasNext(); i++){
				Edge edge = iterator.next();
				List<byte[]> labels = createEdgeLabels(edge);
				deletes.add(new Delete(labels.get(0)));
				deletes.add(new Delete(labels.get(1)));
				deletes.add(new Delete(edge.getId()));
				if(i == bucketSize){
					table.delete(deletes);
					deletes.clear();
					i = 0;
				}
			}
			iterator = vertex.getOutgoingEdges().iterator();
			for(; iterator.hasNext(); i++){
				Edge edge = iterator.next();
				List<byte[]> labels = createEdgeLabels(edge);
				deletes.add(new Delete(labels.get(0)));
				deletes.add(new Delete(labels.get(1)));
				deletes.add(new Delete(edge.getId()));
				if(i == bucketSize){
					table.delete(deletes);
					deletes.clear();
					i = 0;
				}
			}
			if(deletes.size() > 0){
				table.delete(deletes);
			}
			table.delete(new Delete(vertex.getId()));
		} catch(IOException e){
			throw new StorageException(e);
		} finally {
			pool.putTable(table);
		}
	}

	public Edge createEdge(Vertex from, Vertex to, String type){
		byte[] id;
		Put p;
		HTableInterface table = pool.getTable(GRAPHISH_TABLE);
		List<Put> puts = new ArrayList<Put>();
		List<byte[]> labels = createEdgeLabels(from, to, type);
		
		try {
			if(recordExists(labels.get(0))){ // does it make sense to check for the other label?
				throw new EdgeExists(from, to, type);
			}
			do {
				id = createEdgeId();
				p = new Put(id);
				p.add(GRAPHMETA_FAM, ID_QUAL, id);
				p.add(GRAPHMETA_FAM, FROM_QUAL, from.getId());
				p.add(GRAPHMETA_FAM, TO_QUAL, to.getId());
				p.add(GRAPHMETA_FAM, TYPE_QUAL, Bytes.toBytes(type));
			} while(!table.checkAndPut(id, GRAPHMETA_FAM, ID_QUAL, null, p));
			
			p = new Put(labels.get(0));
			p.add(GRAPHMETA_FAM, ID_QUAL, id);
			p.add(GRAPHMETA_FAM, FROM_QUAL, from.getId());
			p.add(GRAPHMETA_FAM, TO_QUAL, to.getId());
			p.add(GRAPHMETA_FAM, TYPE_QUAL, Bytes.toBytes(type));
			puts.add(p);
			
			p = new Put(labels.get(1));
			p.add(GRAPHMETA_FAM, ID_QUAL, id);
			p.add(GRAPHMETA_FAM, FROM_QUAL, from.getId());
			p.add(GRAPHMETA_FAM, TO_QUAL, to.getId());
			p.add(GRAPHMETA_FAM, TYPE_QUAL, Bytes.toBytes(type));
			puts.add(p);
			table.put(puts);
		} catch(IOException e){
			throw new StorageException(e);
		} finally {
			pool.putTable(table);
		}
		
		return new EdgeImpl(graph, id, from, to, type);
	}
	
	public void removeEdge(Edge edge) {
		HTableInterface table = pool.getTable(GRAPHISH_TABLE);
		
		try {
			List<byte[]> labels = createEdgeLabels(edge);
			List<Delete> deletes = new ArrayList<Delete>();
			deletes.add(new Delete(labels.get(0)));
			deletes.add(new Delete(labels.get(1)));
			deletes.add(new Delete(edge.getId()));
			table.delete(deletes);
		} catch(IOException e){
			throw new StorageException(e);
		} finally {
			pool.putTable(table);
		}
	}

	public Edge putOutgoingEdge(Vertex from, Vertex to, String type){
		return createEdge(from, to, type);
	}

	public Iterable<Edge> getOutgoingEdges(Vertex vertex) {
		return new NeighborsIterator(vertex, Direction.OUT);
	}

	public Iterable<Edge> getOutgoingEdges(Vertex vertex, String type) {
		return new NeighborsIterator(vertex, Direction.OUT, type);
	}

	public Edge putIncomingEdge(Vertex from, Vertex to, String type){
		return createEdge(from, to, type);
	}

	public Iterable<Edge> getIncomingEdges(Vertex vertex) {
		return new NeighborsIterator(vertex, Direction.IN);
	}

	public Iterable<Edge> getIncomingEdges(Vertex vertex, String type) {
		return new NeighborsIterator(vertex, Direction.IN, type);
	}
	
	/*
	 * PropertyStore Interface implementations
	 */
	
	public boolean hasProperty(byte[] container, String key){
		boolean ret;
		HTableInterface table = pool.getTable(GRAPHISH_TABLE);
		
		try {
			if(!recordExists(table, container)){
				throw new DoesntExist(container);
			}
			Get g = new Get(container);
			g.addColumn(PROPERTIES_FAM, Bytes.toBytes(key));
			ret = table.exists(g);
		} catch(IOException e){
			throw new StorageException(e);
		} finally {
			pool.putTable(table);
		}
		
		return ret;
	}

	public void setProperty(byte[] container, String key, byte[] value){
		byte[] property = Bytes.toBytes(key);
		HTableInterface table = pool.getTable(GRAPHISH_TABLE);
		
		try {
			if(!recordExists(table, container)){
				throw new DoesntExist(container);
			}
			Put p = new Put(container);
			p.add(PROPERTIES_FAM, property, value);
			table.put(p);
		} catch(IOException e){
			throw new StorageException(e);
		} finally {
			pool.putTable(table);
		}
	}

	public byte[] getProperty(byte[] container, String key){
		byte[] ret = null;
		byte[] property = Bytes.toBytes(key);
		HTableInterface table = pool.getTable(GRAPHISH_TABLE);
		
		try {
			if(!recordExists(table, container)){
				throw new DoesntExist(container);
			}
			Get g = new Get(container);
			g.addColumn(PROPERTIES_FAM, property);
			Result res = table.get(g);
			if(!res.isEmpty()){
				ret = res.getValue(PROPERTIES_FAM, property);
			}
		} catch(IOException e){
			throw new StorageException(e);
		} finally {
			pool.putTable(table);
		}
		
		return ret;
	}

	public byte[] removeProperty(byte[] container, String key){
		byte[] ret = null;
		byte[] property = Bytes.toBytes(key);
		HTableInterface table = pool.getTable(GRAPHISH_TABLE);
		
		try {
			if(!recordExists(table, container)){
				throw new DoesntExist(container);
			}
			Get g = new Get(container);
			g.addColumn(PROPERTIES_FAM, property);
			Result res = table.get(g);
			if(!res.isEmpty()){
				ret = res.getValue(PROPERTIES_FAM, property);
			}
			Delete d = new Delete(container);
			d.deleteColumn(PROPERTIES_FAM, property);
			table.delete(d);
		} catch(IOException e){
			throw new StorageException(e);
		} finally {
			pool.putTable(table);
		}
		
		return ret;
	}

	public Iterable<String> getPropertyKeys(byte[] container){
		return getPropertyContainer(container).getPropertyKeys();
	}

	public Iterable<byte[]> getPropertyValues(byte[] container) {
		return getPropertyContainer(container).getPropertyValues();
	}

	public PropertyContainer getPropertyContainer(byte[] container) {
		HTableInterface table = pool.getTable(GRAPHISH_TABLE);
		PropertyContainer data = new PropertyContainerImpl(container);
		
		try {
			if(!recordExists(table, container)){
				throw new DoesntExist(container);
			}
			Get g = new Get(container);
			Result res = table.get(g);
			if(!res.isEmpty()){
				for(Entry<byte[],byte[]> property: res.getFamilyMap(PROPERTIES_FAM).entrySet()){
					data.setProperty(Bytes.toString(property.getKey()), property.getValue());
				}
			}
		} catch(IOException e){
			throw new StorageException(e);
		} finally {
			pool.putTable(table);
		}
		
		return data;
	}
	
	private byte[] createVertexId(){
		UUID id = UUID.randomUUID();
		return org.acaro.graphish.Bytes.fromUuid(id).toByteArray();
	}
	
	private byte[] createEdgeId(){
		UUID id = UUID.randomUUID();
		return org.acaro.graphish.Bytes.fromUuid(id).toByteArray();
	}
	
	private void incrementKey(byte[] id, int index){
		if(id[index] == Byte.MAX_VALUE){
			id[index] = 0;
			if(index > 0){
				incrementKey(id, index - 1);
			}
		} else {
			id[index]++;
		}
	}
	
	private byte[] createRecordId(List<byte[]> tokens){
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
	
	private List<byte[]> createEdgeLabels(Vertex from, Vertex to, String type){
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
	
	private List<byte[]> createEdgeLabels(Edge edge){
		return createEdgeLabels(edge.getFrom(), edge.getTo(), edge.getType());
	}
	
	private byte[] createEdgePrefix(Vertex v, Direction direction){
		List<byte[]> args = new LinkedList<byte[]>();
		
		args.add(v.getId());
		args.add(EDGE_CONJ);
		args.add((direction.equals(Direction.IN)) ? DIR_IN: DIR_OUT);
		
		return createRecordId(args);
	}
	
	private byte[] createTypedEdgePrefix(Vertex v, Direction direction, String type){
		List<byte[]> args = new LinkedList<byte[]>();
		
		args.add(v.getId());
		args.add(EDGE_CONJ);
		args.add((direction.equals(Direction.IN)) ? DIR_IN: DIR_OUT);
		args.add(EDGE_CONJ);
		args.add(Bytes.toBytes(type));
		
		return createRecordId(args);
	}
	
	private boolean recordExists(HTableInterface table, byte[] id) throws IOException {
		Get g = new Get(id);

		return table.exists(g);
	}
	
	private boolean recordExists(byte[] id) throws IOException {
		boolean ret;
		HTableInterface table = pool.getTable(GRAPHISH_TABLE);
		
		try {
			ret = recordExists(table, id);
		} finally {
			pool.putTable(table);
		}
		
		return ret;
	}
	
	private byte[] getTable(Object o){
		if(o instanceof Vertex){
			return VERTICES;
		} else if(o instanceof Edge){
			return EDGES;
		} else {
			return null;
		}
	}
	
	private class NeighborsIterator implements Iterator<Edge>, Iterable<Edge> {
		private boolean hasNext = true, finished = false;
		private byte[] startRow, stopRow;
		private Edge last = null;
		private ResultScanner scanner;
		private Filter filter;
		private HTableInterface table;
		private LinkedList<Edge> resultsCache = new LinkedList<Edge>();
		
		public NeighborsIterator(Vertex v, Direction direction) {
			byte[] edgePrefix = createEdgePrefix(v, direction);
			filter   = new PrefixFilter(edgePrefix);
			startRow = edgePrefix;
			stopRow  = v.getId();
			incrementKey(stopRow, stopRow.length-1);
			init();
		}
		
		public NeighborsIterator(Vertex v, Direction direction, String type) {
			byte[] edgePrefix = createTypedEdgePrefix(v, direction, type);
			filter   = new PrefixFilter(edgePrefix);
			startRow = edgePrefix;
			stopRow  = v.getId();
			incrementKey(stopRow, stopRow.length-1);
			init();
		}
		
		protected void finalize() throws Throwable {
			scanner.close();
			pool.putTable(table);
		}
		
		public Iterator<Edge> iterator() {
			return this;
		}

		public boolean hasNext() {
			return hasNext;
		}

		public Edge next() {
			if(!hasNext){
				throw new NoSuchElementException();
			}
			Edge edge = resultsCache.removeFirst();
			if(resultsCache.size() == 0){
				if(!finished){
					hasNext = fetchResults(bucketSize);
				} else {
					hasNext = false;
				}
			} 
			last = edge;
			
			return edge;
		}
		
		public void remove() {
			if(last == null){
				throw new IllegalStateException(); 
			} else {
				removeEdge(last);
				last = null;
			}
		}
		
		private boolean fetchResults(int num) {
			boolean ret = false;

			try {
				try {
					ret = scanResults(num);
				} catch(ScannerTimeoutException e){
					this.scanner = table.getScanner(initScan());
					ret = scanResults(num);
				}
			} catch(IOException e){
				throw new StorageException(e);
			}
			
			return ret;
		}
		
		private boolean scanResults(int num) throws ScannerTimeoutException, IOException {
			Result[] results;
			
			results = scanner.next(num);
			if(results.length < num){
				scanner.close();
				pool.putTable(table);
				finished = true;
			}
			if(results.length == 0){
				return false;
			}
			for(Result result: results){
				byte[] id   = result.getValue(SPROPERTIES_FAM, ID_QUAL);
				byte[] from = result.getValue(SPROPERTIES_FAM, FROM_QUAL);
				byte[] to   = result.getValue(SPROPERTIES_FAM, TO_QUAL);
				byte[] type = result.getValue(SPROPERTIES_FAM, TYPE_QUAL);
				Edge edge   = new EdgeImpl(graph, id, new VertexImpl(graph, from), 
						new VertexImpl(graph, to), Bytes.toString(type));
				resultsCache.addLast(edge);
			}
			// in case the scanner expires and we have to re-create it
			startRow = resultsCache.getLast().getId();
			incrementKey(startRow, startRow.length-1);
			
			return true;
		}
		
		private void init() {
			HTableInterface table = pool.getTable(NEIGHBORS);
			try {
				scanner = table.getScanner(initScan());
			} catch (IOException e) {
				throw new StorageException(e);
			}
			hasNext = fetchResults(1);
		}
		
		private Scan initScan() {
			Scan scan = new Scan();
			scan.setStartRow(startRow);
			scan.setStopRow(stopRow);
			scan.setFilter(filter);
			
			return scan;
		}
	}
}