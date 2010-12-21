package org.acaro.graphish;

import java.io.IOException;
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

/*
 * XXX: Should we really check for record existence? or just return null/false? performance-wise it's better...
 * XXX: Is returning old value in setProperty and removeProperty really necessary? Considering the context of low-latency
 * 		it could be user's responsability to check old value first, when needed!
 */

public class HBaseStore implements PropertyStore, GraphStore {
	final private byte[] EDGE_CONJ = { 0x2b };
	final private byte[] END_TOKEN = { 0x7f };
	final private byte[] DIR_IN  = { 0x49 };
	final private byte[] DIR_OUT = { 0x4f };
	private enum Direction { IN, OUT };
	final private byte[] GRAPHISH_TABLE  = Bytes.toBytes("Graphish");
	final private byte[] PROPERTIES_FAM  = Bytes.toBytes("Properties");
	final private byte[] SPROPERTIES_FAM = Bytes.toBytes("SProperties");
	final private byte[] FROM_QUAL = Bytes.toBytes("from");
	final private byte[] TYPE_QUAL = Bytes.toBytes("type");
	final private byte[] ID_QUAL   = Bytes.toBytes("id");
	final private byte[] TO_QUAL   = Bytes.toBytes("to");
	private Graphish graph;
	private HTablePool pool = new HTablePool();
	
	public HBaseStore(Graphish graph) throws IOException {
		this.graph = graph;
		HBaseAdmin admin = new HBaseAdmin(HBaseConfiguration.create());
		if(!admin.tableExists(GRAPHISH_TABLE)){
			HTableDescriptor table = new HTableDescriptor(GRAPHISH_TABLE);
			HColumnDescriptor family1 = new HColumnDescriptor(PROPERTIES_FAM);
			HColumnDescriptor family2 = new HColumnDescriptor(SPROPERTIES_FAM);
			table.addFamily(family1);
			table.addFamily(family2);
			admin.createTable(table);
		}
		if(!admin.isTableEnabled(GRAPHISH_TABLE)){
			throw new IOException(Bytes.toString(GRAPHISH_TABLE)+" Table not enabled, can't go further");
		}
	}
	
	/*
	 * GraphStore Interface implementations
	 */
	
	public Vertex createVertex(Graphish graph) throws IOException {
		byte[] id;
		Put p;
		HTableInterface table = pool.getTable(GRAPHISH_TABLE);
		
		try {
			// we handle the rare case of uuid collision
			do {
				id = createVertexId();
				p = new Put(id);
				p.add(SPROPERTIES_FAM, ID_QUAL, id);
			} while(!table.checkAndPut(id, SPROPERTIES_FAM, ID_QUAL, null, p));

		} finally {
			pool.putTable(table);
		}
		
		return new VertexImpl(graph, id);
	}

	public Edge createEdge(Graphish graph, Vertex from, Vertex to, String type) throws IOException {
		byte id[];
		Put p;
		HTableInterface table = pool.getTable(GRAPHISH_TABLE);
		
		try {
			id = createEdgeId(from, to, Direction.OUT, type);
			p = new Put(id);
			p.add(SPROPERTIES_FAM, ID_QUAL, id);
			p.add(SPROPERTIES_FAM, FROM_QUAL, from.getId());
			p.add(SPROPERTIES_FAM, TO_QUAL, to.getId());
			p.add(SPROPERTIES_FAM, TYPE_QUAL, Bytes.toBytes(type));
			if(!table.checkAndPut(id, SPROPERTIES_FAM, ID_QUAL, null, p)){
				throw new EdgeExists(from, to, type);
			}
			id = createEdgeId(to, from, Direction.IN, type);
			p = new Put(id);
			p.add(SPROPERTIES_FAM, ID_QUAL, id);
			p.add(SPROPERTIES_FAM, FROM_QUAL, from.getId());
			p.add(SPROPERTIES_FAM, TO_QUAL, to.getId());
			p.add(SPROPERTIES_FAM, TYPE_QUAL, Bytes.toBytes(type));
			table.put(p); // should not exist!
		} finally {
			pool.putTable(table);
		}
		
		return new EdgeImpl(graph, id, from, to, type);
	}

	public Edge putOutgoingEdge(Vertex from, Vertex to, String type) throws IOException {
		return createEdge(graph, from, to, type);
	}

	public Iterable<Edge> getOutgoingEdges(Vertex vertex) {
		return new EdgeIterator(vertex, Direction.OUT);
	}

	public Iterable<Edge> getOutgoingEdges(Vertex vertex, String type) {
		return new EdgeIterator(vertex, Direction.OUT, type);
	}

	public Edge putIncomingEdge(Vertex from, Vertex to, String type) throws IOException {
		return createEdge(graph, from, to, type);
	}

	public Iterable<Edge> getIncomingEdges(Vertex vertex) {
		return new EdgeIterator(vertex, Direction.IN);
	}

	public Iterable<Edge> getIncomingEdges(Vertex vertex, String type) {
		return new EdgeIterator(vertex, Direction.IN, type);
	}
	
	/*
	 * PropertyStore Interface implementations
	 */
	
	public boolean hasProperty(PropertyContainer container, String key) throws IOException {
		boolean ret;
		HTableInterface table = pool.getTable(GRAPHISH_TABLE);
		
		try {
			if(!recordExists(table, container.getId())){
				throw new ContainerDoesntExist(container);
			}
			Get g = new Get(container.getId());
			g.addColumn(PROPERTIES_FAM, Bytes.toBytes(key));
			ret = table.exists(g);
		} finally {
			pool.putTable(table);
		}
		
		return ret;
	}

	public byte[] setProperty(PropertyContainer container, String key,
			byte[] value) throws IOException {
		byte[] ret = null;
		byte[] property = Bytes.toBytes(key);
		HTableInterface table = pool.getTable(GRAPHISH_TABLE);
		
		try {
			if(!recordExists(table, container.getId())){
				throw new ContainerDoesntExist(container);
			}
			// could use getProperty here, but we're saving a call to recordExists and a new HTable from the pool!
			Get g = new Get(container.getId());
			g.addColumn(PROPERTIES_FAM, property);
			Result res = table.get(g);
			if(!res.isEmpty()){
				ret = res.getValue(PROPERTIES_FAM, property);
			}
			Put p = new Put(container.getId());
			p.add(PROPERTIES_FAM, property, value);
			table.put(p);
		} finally {
			pool.putTable(table);
		}

		return ret;
	}

	public byte[] getProperty(PropertyContainer container, String key) throws IOException {
		byte[] ret = null;
		byte[] property = Bytes.toBytes(key);
		HTableInterface table = pool.getTable(GRAPHISH_TABLE);
		
		try {
			if(!recordExists(table, container.getId())){
				throw new ContainerDoesntExist(container);
			}
			Get g = new Get(container.getId());
			g.addColumn(PROPERTIES_FAM, property);
			Result res = table.get(g);
			if(!res.isEmpty()){
				ret = res.getValue(PROPERTIES_FAM, property);
			}
		} finally {
			pool.putTable(table);
		}
		
		return ret;
	}

	public byte[] removeProperty(PropertyContainer container, String key) throws IOException {
		byte[] ret = null;
		byte[] property = Bytes.toBytes(key);
		HTableInterface table = pool.getTable(GRAPHISH_TABLE);
		
		try {
			if(!recordExists(table, container.getId())){
				throw new ContainerDoesntExist(container);
			}
			Get g = new Get(container.getId());
			g.addColumn(PROPERTIES_FAM, property);
			Result res = table.get(g);
			if(!res.isEmpty()){
				ret = res.getValue(PROPERTIES_FAM, property);
			}
			Delete d = new Delete(container.getId());
			d.deleteColumn(PROPERTIES_FAM, property);
			table.delete(d);
		} finally {
			pool.putTable(table);
		}
		
		return ret;
	}

	public Iterable<String> getPropertyKeys(PropertyContainer container) throws IOException {
		return getPropertyContainer(container).getPropertyKeys();
	}

	public Iterable<byte[]> getPropertyValues(PropertyContainer container) throws IOException {
		return getPropertyContainer(container).getPropertyValues();
	}

	public PropertyContainer getPropertyContainer(PropertyContainer container) throws IOException {
		HTableInterface table = pool.getTable(GRAPHISH_TABLE);
		PropertyContainer data = new PropertyContainerImpl(container.getId());
		
		try {
			if(!recordExists(table, container.getId())){
				throw new ContainerDoesntExist(container);
			}
			Get g = new Get(container.getId());
			Result res = table.get(g);
			if(!res.isEmpty()){
				for(Entry<byte[],byte[]> property: res.getFamilyMap(PROPERTIES_FAM).entrySet()){
					data.setProperty(Bytes.toString(property.getKey()), property.getValue());
				}
			}
		} finally {
			pool.putTable(table);
		}
		
		return data;
	}
	
	private byte[] createVertexId() {
		UUID id = UUID.randomUUID();
		return org.acaro.graphish.Bytes.fromUuid(id).toByteArray();
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
	
	private byte[] createEdgeId(Vertex from, Vertex to, Direction direction, String type){
		List<byte[]> args = new LinkedList<byte[]>();
		
		args.add(from.getId());
		args.add(EDGE_CONJ);
		args.add((direction.equals(Direction.IN)) ? DIR_IN: DIR_OUT);
		args.add(EDGE_CONJ);
		args.add(Bytes.toBytes(type));
		args.add(EDGE_CONJ);
		args.add(to.getId());
		
		return createRecordId(args);
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
	
	private byte[] createEndEdgePrefix(Vertex v){
		List<byte[]> args = new LinkedList<byte[]>();
		
		args.add(v.getId());
		args.add(EDGE_CONJ);
		args.add(END_TOKEN);
		
		return createRecordId(args);
	}
	
	private boolean recordExists(HTableInterface table, byte[] id) throws IOException{
		Get g = new Get(id);

		return table.exists(g);
	}
	
	private class EdgeIterator implements Iterator<Edge>, Iterable<Edge> {
		private String type = null;
		private boolean hasNext = true, finished = false;
		private ResultScanner scanner;
		private Scan scan;
		private HTableInterface table;
		private List<Edge> resultsCache = new LinkedList<Edge>();
		private int fetchSize = 100;
		
		private EdgeIterator() {
			hasNext = fetchResults(1);
		}
		
		public EdgeIterator(Vertex v, Direction direction) {
			Filter filter = new PrefixFilter(createEdgePrefix(v, direction));
			this.scan = initScan(v, filter);
		}
		
		public EdgeIterator(Vertex v, Direction direction, String type) {
			Filter filter = new PrefixFilter(createTypedEdgePrefix(v, direction, type));
			this.scan = initScan(v, filter);
		}
		
		public Iterator<Edge> iterator() {
			return this;
		}

		public boolean hasNext() {
			return hasNext;
		}

		public Edge next() {

		}

		public void remove() {
		}
		
		private boolean fetchResults(int num) {
			Result[] results;
			
			try {
				results = scanner.next(num);
			} catch (ScannerTimeoutException e) {
				table.getScanner(this.scan);
			} catch (IOException e) {
				results = new Result[0];
			}
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
				Edge edge = new EdgeImpl(graph, id, new VertexImpl(graph, from), new VertexImpl(graph, to), Bytes.toString(type));
				resultsCache.add(edge);
			}
			
			return true;
		}
		
		private Scan initScan(Vertex v, Filter filter) {
			Scan scan = new Scan();
			scan.setStartRow(v.getId());
			scan.setStopRow(createEndEdgePrefix(v));
			scan.setFilter(filter);
			return scan;
		}
	}
}