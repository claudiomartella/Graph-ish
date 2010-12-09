package org.acaro.graphish;

import java.util.UUID;

public class OldVertex {

	private boolean fetched = false;
	private UUID id;
	private VertexProperties props;
	
	protected static OldVertex factory() {
		UUID uuid = UUID.randomUUID();
		
		return new OldVertex(uuid);
	}
	
	protected OldVertex(UUID id) {
		
		this.id = id;
	}
	
	/* Could add this constructor but let's simplify for the moment and push the caller to call putProperties on the new Vertex
	protected Vertex(UUID id, VertexProperties p) {
		
		this(id);
		this.putProperties(p);
	}*/
	
	/* public interface */
	
	public UUID getId() {
		
		return this.id;
	}
	
	public Edge addIncomingEdge(OldVertex from, String type) {
		Edge edge = Edge.factory(from, this, type);

		return from.addOutgoingEdge(this, edge);
	}
	
	/*	Could add this method but let's simplify for the moment and push the caller to call putProperties on the new Edge
	 * 
    public Edge addIncomingEdge(Vertex from, EdgeProperties props) {
		
		return this.addIncomingEdge(from).putProperties(props);
	}*/
	
	private Edge addIncomingEdge(OldVertex from, Edge edge) {
		
		return null;
	}
	
	public Edge addOutgoingEdge(OldVertex to, String type) {
		Edge edge = Edge.factory(this, to, type);
		
		return to.addIncomingEdge(this, edge);
	}
	
	/*	Could add this method but let's simplify for the moment and push the caller to call putProperties on the new Edge
	 * 
	public Edge addOutgoingEdge(Vertex to, EdgeProperties props) {
		
		return this.addOutgoingEdge(to).putProperties(props);
	}*/
	
	private Edge addOutgoingEdge(OldVertex to, Edge edge){
		
		return null;
	}
	
	public Iterable<Edge> getOutgoingEdges() {
		return null;
	}
	
	public Iterable<Edge> getIncomingEdges() {
		return null;
	}
	
	public byte[] getProperty(String name) {
		
		return props.getProperty(name);
	}
	
	public void putProperty(String name, byte[] value){
		
		props.putProperty(name, value);
	}
	
	public void putProperties(VertexProperties props) {
		
		props.putProperties(props);
	}
	
	public VertexProperties getProperties() {
		
		return props;
	}
}