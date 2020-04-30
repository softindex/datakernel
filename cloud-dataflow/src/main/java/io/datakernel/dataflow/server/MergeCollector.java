package io.datakernel.dataflow.server;

import io.datakernel.dataflow.dataset.Dataset;
import io.datakernel.dataflow.dataset.SortedDataset;
import io.datakernel.dataflow.graph.DataflowContext;
import io.datakernel.dataflow.graph.DataflowGraph;
import io.datakernel.dataflow.graph.Partition;
import io.datakernel.dataflow.graph.StreamId;
import io.datakernel.dataflow.node.NodeUpload;
import io.datakernel.datastream.StreamSupplier;
import io.datakernel.datastream.processor.StreamMerger;

import java.util.Comparator;
import java.util.List;
import java.util.function.Function;

public class MergeCollector<K, T> {
	private final Dataset<T> input;
	private final DataflowClient client;
	private final Function<T, K> keyFunction;
	private final Comparator<K> keyComparator;
	private final boolean distinct;

	public MergeCollector(SortedDataset<K, T> input, DataflowClient client, boolean distinct) {
		this(input, client, input.keyFunction(), input.keyComparator(), distinct);
	}

	public MergeCollector(Dataset<T> input, DataflowClient client,
	                      Function<T, K> keyFunction, Comparator<K> keyComparator, boolean distinct) {
		this.input = input;
		this.client = client;
		this.keyFunction = keyFunction;
		this.keyComparator = keyComparator;
		this.distinct = distinct;
	}

	public StreamSupplier<T> compile(DataflowGraph graph) {
		List<StreamId> inputStreamIds = input.channels(DataflowContext.of(graph));

		StreamMerger<K, T> merger = StreamMerger.create(keyFunction, keyComparator, distinct);
		for (StreamId streamId : inputStreamIds) {
			NodeUpload<T> nodeUpload = new NodeUpload<>(input.valueType(), streamId);
			Partition partition = graph.getPartition(streamId);
			graph.addNode(partition, nodeUpload);
			StreamSupplier<T> supplier = StreamSupplier.ofPromise(client.download(partition.getAddress(), streamId, input.valueType()));
			supplier.streamTo(merger.newInput());
		}

		return merger.getOutput();
	}
}
