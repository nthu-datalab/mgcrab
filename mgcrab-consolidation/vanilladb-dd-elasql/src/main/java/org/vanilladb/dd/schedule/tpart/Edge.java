package org.vanilladb.dd.schedule.tpart;

import org.vanilladb.dd.sql.RecordKey;

public class Edge {
	// XXX source/destination should be combined with old system
	private Node source;
	private Node destination;
	private Node target;
	private RecordKey resource;

	public Edge(Node target, RecordKey res) {
		this.target = target;
		this.resource = res;
	}

	public Edge(Node source, Node dest, RecordKey res) {
		this.source = source;
		this.destination = dest;
		this.resource = res;
	}

	public Node getTarget() {
		return target;
	}

	public RecordKey getResourceKey() {
		return resource;
	}

	public Node getSource() {
		return source;
	}

	public Node getDestination() {
		return destination;
	}

}
