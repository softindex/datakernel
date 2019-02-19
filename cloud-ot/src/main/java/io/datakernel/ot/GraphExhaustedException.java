package io.datakernel.ot;

import io.datakernel.exception.StacklessException;

public class GraphExhaustedException extends StacklessException {
	public static final GraphExhaustedException INSTANCE = new GraphExhaustedException();

	private GraphExhaustedException() {
		super(OTAlgorithms.class, "Graph exhausted");
	}
}
