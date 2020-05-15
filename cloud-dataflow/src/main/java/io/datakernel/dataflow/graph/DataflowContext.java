package io.datakernel.dataflow.graph;

import org.jetbrains.annotations.Nullable;

import java.util.concurrent.ThreadLocalRandom;

public final class DataflowContext {
	private final DataflowGraph graph;

	@Nullable
	private final Integer nonce;

	private DataflowContext(DataflowGraph graph, @Nullable Integer nonce) {
		this.nonce = nonce;
		this.graph = graph;
	}

	public static DataflowContext of(DataflowGraph graph) {
		return new DataflowContext(graph, null);
	}

	public DataflowGraph getGraph() {
		return graph;
	}

	public int getNonce() {
		return nonce == null ?
				ThreadLocalRandom.current().nextInt() :
				nonce;
	}

	public DataflowContext withFixedNonce(int nonce) {
		return new DataflowContext(graph, nonce);
	}

	public DataflowContext withoutFixedNonce() {
		return new DataflowContext(graph, null);
	}
}
