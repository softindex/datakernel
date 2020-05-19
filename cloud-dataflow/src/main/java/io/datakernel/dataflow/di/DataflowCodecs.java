package io.datakernel.dataflow.di;

import io.datakernel.codec.StructuredCodec;
import io.datakernel.codec.StructuredOutput;
import io.datakernel.common.parse.ParseException;
import io.datakernel.dataflow.di.CodecsModule.SubtypeNameFactory;
import io.datakernel.dataflow.di.CodecsModule.Subtypes;
import io.datakernel.dataflow.graph.StreamId;
import io.datakernel.dataflow.node.*;
import io.datakernel.dataflow.server.command.DatagraphCommand;
import io.datakernel.dataflow.server.command.DatagraphCommandDownload;
import io.datakernel.dataflow.server.command.DatagraphCommandExecute;
import io.datakernel.dataflow.server.command.DatagraphResponse;
import io.datakernel.datastream.processor.StreamJoin.Joiner;
import io.datakernel.datastream.processor.StreamReducers.MergeSortReducer;
import io.datakernel.datastream.processor.StreamReducers.Reducer;
import io.datakernel.datastream.processor.StreamReducers.ReducerToResult;
import io.datakernel.datastream.processor.StreamReducers.ReducerToResult.AccumulatorToAccumulator;
import io.datakernel.datastream.processor.StreamReducers.ReducerToResult.AccumulatorToOutput;
import io.datakernel.datastream.processor.StreamReducers.ReducerToResult.InputToAccumulator;
import io.datakernel.datastream.processor.StreamReducers.ReducerToResult.InputToOutput;
import io.datakernel.di.Key;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.di.util.Types;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

import static io.datakernel.codec.StructuredCodec.ofObject;
import static io.datakernel.codec.StructuredCodecs.object;
import static io.datakernel.codec.StructuredCodecs.ofList;
import static io.datakernel.datastream.processor.StreamReducers.MergeDistinctReducer;

@SuppressWarnings({"rawtypes", "unchecked"})
public final class DataflowCodecs extends AbstractModule {

	private DataflowCodecs() {
	}

	public static DataflowCodecs create() {
		return new DataflowCodecs();
	}

	private static final Comparator<?> NATURAL_ORDER = Comparator.naturalOrder();
	private static final Class<?> NATURAL_ORDER_CLASS = NATURAL_ORDER.getClass();

	@Override
	protected void configure() {
		install(CodecsModule.create());

		bind(new Key<StructuredCodec<DatagraphCommand>>() {}).qualified(Subtypes.class);

		bind(Key.ofType(Types.parameterized(StructuredCodec.class, NATURAL_ORDER_CLASS)))
				.toInstance(StructuredCodec.ofObject(() -> NATURAL_ORDER));
	}

	@Provides
	StructuredCodec<StreamId> streamId() {
		return StructuredCodec.of(
				in -> new StreamId(in.readLong()),
				(out, item) -> out.writeLong(item.getId()));
	}

	@Provides
	StructuredCodec<InetSocketAddress> address() {
		return StructuredCodec.of(
				in -> {
					String str = in.readString();
					String[] split = str.split(":");
					if (split.length != 2) {
						throw new ParseException("Address should be splitted with a single ':'");
					}
					try {
						return new InetSocketAddress(InetAddress.getByName(split[0]), Integer.parseInt(split[1]));
					} catch (UnknownHostException e) {
						throw new ParseException(DataflowCodecs.class, "Failed to create InetSocketAdress", e);
					}
				},
				(out, addr) -> out.writeString(addr.getAddress().getHostAddress() + ':' + addr.getPort())
		);
	}

	@Provides
	StructuredCodec<DatagraphCommandDownload> datagraphCommandDownload(StructuredCodec<StreamId> streamId) {
		return object(DatagraphCommandDownload::new,
				"streamId", DatagraphCommandDownload::getStreamId, streamId);
	}

	@Provides
	StructuredCodec<DatagraphCommandExecute> datagraphCommandExecute(@Subtypes StructuredCodec<Node> node) {
		return object(DatagraphCommandExecute::new,
				"nodes", DatagraphCommandExecute::getNodes, ofList(node));
	}

	@Provides
	StructuredCodec<DatagraphResponse> datagraphResponse(StructuredCodec<String> string) {
		//noinspection ConstantConditions - intellji false positive
		return object(DatagraphResponse::new,
				"error", DatagraphResponse::getError, string.nullable());
	}

	@Provides
	StructuredCodec<NodeReduce.Input> nodeReduceInput(@Subtypes StructuredCodec<Reducer> reducer, @Subtypes StructuredCodec<Function> function) {
		return object(NodeReduce.Input::new,
				"reducer", NodeReduce.Input::getReducer, reducer,
				"keyFunction", NodeReduce.Input::getKeyFunction, function);
	}

	@Provides
	StructuredCodec<InputToAccumulator> inputToAccumulator(@Subtypes StructuredCodec<ReducerToResult> reducerToResult) {
		return object(InputToAccumulator::new,
				"reducerToResult", InputToAccumulator::getReducerToResult, reducerToResult);
	}

	@Provides
	StructuredCodec<InputToOutput> inputToOutput(@Subtypes StructuredCodec<ReducerToResult> reducerToResult) {
		return object(InputToOutput::new,
				"reducerToResult", InputToOutput::getReducerToResult, reducerToResult);
	}

	@Provides
	StructuredCodec<AccumulatorToAccumulator> accumulatorToAccumulator(@Subtypes StructuredCodec<ReducerToResult> reducerToResult) {
		return object(AccumulatorToAccumulator::new,
				"reducerToResult", AccumulatorToAccumulator::getReducerToResult, reducerToResult);
	}

	@Provides
	StructuredCodec<AccumulatorToOutput> accumulatorToOutput(@Subtypes StructuredCodec<ReducerToResult> reducerToResult) {
		return object(AccumulatorToOutput::new,
				"reducerToResult", AccumulatorToOutput::getReducerToResult, reducerToResult);
	}

	@Provides
	StructuredCodec<MergeDistinctReducer> mergeDistinctReducer() {
		return ofObject(MergeDistinctReducer::new);
	}

	@Provides
	StructuredCodec<MergeSortReducer> mergeSortReducer() {
		return ofObject(MergeSortReducer::new);
	}

	@Provides
	StructuredCodec<NodeDownload> nodeDownload(StructuredCodec<Class<?>> cls, StructuredCodec<InetSocketAddress> address, StructuredCodec<StreamId> streamId) {
		return object(NodeDownload::new,
				"type", NodeDownload::getType, cls,
				"address", NodeDownload::getAddress, address,
				"streamId", NodeDownload::getStreamId, streamId,
				"output", NodeDownload::getOutput, streamId);
	}

	@Provides
	StructuredCodec<NodeUpload> nodeUpload(StructuredCodec<Class<?>> cls, StructuredCodec<StreamId> streamId) {
		return object(NodeUpload::new,
				"type", NodeUpload::getType, cls,
				"streamId", NodeUpload::getStreamId, streamId);
	}

	@Provides
	StructuredCodec<NodeMap> nodeMap(@Subtypes StructuredCodec<Function> function, StructuredCodec<StreamId> streamId) {
		return object(NodeMap::new,
				"function", NodeMap::getFunction, function,
				"input", NodeMap::getInput, streamId,
				"output", NodeMap::getOutput, streamId);
	}

	@Provides
	StructuredCodec<NodeFilter> nodeFilter(@Subtypes StructuredCodec<Predicate> predicate, StructuredCodec<StreamId> streamId) {
		return object(NodeFilter::new,
				"predicate", NodeFilter::getPredicate, predicate,
				"input", NodeFilter::getInput, streamId,
				"output", NodeFilter::getOutput, streamId);
	}

	@Provides
	StructuredCodec<NodeShard> nodeShard(@Subtypes StructuredCodec<Function> function, StructuredCodec<StreamId> streamId, StructuredCodec<List<StreamId>> streamIds, StructuredCodec<Integer> integer) {
		return object(NodeShard::new,
				"keyFunction", NodeShard::getKeyFunction, function,
				"input", NodeShard::getInput, streamId,
				"outputs", NodeShard::getOutputs, streamIds,
				"nonce", NodeShard::getNonce, integer);
	}

	@Provides
	StructuredCodec<NodeMerge> nodeMerge(@Subtypes StructuredCodec<Function> function, @Subtypes StructuredCodec<Comparator> comparator, StructuredCodec<Boolean> bool, StructuredCodec<StreamId> streamId, StructuredCodec<List<StreamId>> streamIds) {
		return object(NodeMerge::new,
				"keyFunction", NodeMerge::getKeyFunction, function,
				"keyComparator", NodeMerge::getKeyComparator, comparator,
				"deduplicate", NodeMerge::isDeduplicate, bool,
				"inputs", NodeMerge::getInputs, streamIds,
				"output", NodeMerge::getOutput, streamId);
	}

	@Provides
	StructuredCodec<NodeReduce> nodeReduce(@Subtypes StructuredCodec<Comparator> comparator, StructuredCodec<StreamId> streamId, StructuredCodec<Map<StreamId, NodeReduce.Input>> inputs) {
		return object((a, b, c) -> new NodeReduce(a, b, c),
				"keyComparator", NodeReduce::getKeyComparator, comparator,
				"inputs", n -> (Map<StreamId, NodeReduce.Input>) n.getInputMap(), inputs,
				"output", NodeReduce::getOutput, streamId);
	}

	@Provides
	StructuredCodec<NodeReduceSimple> nodeReduceSimple(@Subtypes StructuredCodec<Function> function, @Subtypes StructuredCodec<Comparator> comparator, @Subtypes StructuredCodec<Reducer> reducer, StructuredCodec<StreamId> streamId, StructuredCodec<List<StreamId>> streamIds) {
		return object(NodeReduceSimple::new,
				"keyFunction", NodeReduceSimple::getKeyFunction, function,
				"keyComparator", NodeReduceSimple::getKeyComparator, comparator,
				"reducer", NodeReduceSimple::getReducer, reducer,
				"inputs", NodeReduceSimple::getInputs, streamIds,
				"output", NodeReduceSimple::getOutput, streamId);
	}

	@Provides
	StructuredCodec<NodeUnion> nodeUnion(StructuredCodec<StreamId> streamId, StructuredCodec<List<StreamId>> streamIds) {
		return object(NodeUnion::new,
				"inputs", NodeUnion::getInputs, streamIds,
				"output", NodeUnion::getOutput, streamId);
	}

	@Provides
	StructuredCodec<NodeSupplierOfIterable> nodeSupplierOfIterable(StructuredCodec<String> string, StructuredCodec<StreamId> streamId) {
		return object(NodeSupplierOfIterable::new,
				"iterableId", node -> (String) node.getIterableId(), string,
				"output", NodeSupplierOfIterable::getOutput, streamId);
	}

	@Provides
	StructuredCodec<NodeConsumerToList> nodeConsumerToList(StructuredCodec<String> string, StructuredCodec<StreamId> streamId) {
		return object(NodeConsumerToList::new,
				"input", NodeConsumerToList::getInput, streamId,
				"listId", node -> (String) node.getListId(), string);
	}

	@Provides
	StructuredCodec<NodeSort> nodeSort(StructuredCodec<Class<?>> cls, @Subtypes StructuredCodec<Comparator> comparator, @Subtypes StructuredCodec<Function> function, StructuredCodec<StreamId> streamId, StructuredCodec<Boolean> bool, StructuredCodec<Integer> integer) {
		return ofObject(
				in -> new NodeSort(
						in.readKey("type", cls),
						in.readKey("keyFunction", function),
						in.readKey("keyComparator", comparator),
						in.readKey("deduplicate", bool),
						in.readKey("itemsInMemorySize", integer),
						in.readKey("input", streamId),
						in.readKey("output", streamId)),
				(StructuredOutput out, NodeSort node) -> {
					out.writeKey("type", cls, (Class<Object>) node.getType());
					out.writeKey("keyFunction", function, node.getKeyFunction());
					out.writeKey("keyComparator", comparator, node.getKeyComparator());
					out.writeKey("deduplicate", bool, node.isDeduplicate());
					out.writeKey("itemsInMemorySize", integer, node.getItemsInMemorySize());
					out.writeKey("input", streamId, node.getInput());
					out.writeKey("output", streamId, node.getOutput());
				});
	}

	@Provides
	StructuredCodec<NodeJoin> nodeJoin(@Subtypes StructuredCodec<Joiner> joiner, @Subtypes StructuredCodec<Comparator> comparator, @Subtypes StructuredCodec<Function> function, StructuredCodec<StreamId> streamId) {
		return ofObject(
				in -> new NodeJoin(
						in.readKey("left", streamId),
						in.readKey("right", streamId),
						in.readKey("output", streamId),
						in.readKey("keyComparator", comparator),
						in.readKey("leftKeyFunction", function),
						in.readKey("rightKeyFunction", function),
						in.readKey("joiner", joiner)),
				(StructuredOutput out, NodeJoin node) -> {
					out.writeKey("left", streamId, node.getLeft());
					out.writeKey("right", streamId, node.getRight());
					out.writeKey("output", streamId, node.getOutput());
					out.writeKey("keyComparator", comparator, node.getKeyComparator());
					out.writeKey("leftKeyFunction", function, node.getLeftKeyFunction());
					out.writeKey("rightKeyFunction", function, node.getRightKeyFunction());
					out.writeKey("joiner", joiner, node.getJoiner());
				});
	}

	@Provides
	SubtypeNameFactory subtypeNames() {
		return subtype -> {
			if (subtype == NATURAL_ORDER_CLASS) return "Comparator.naturalOrder";
			if (subtype == DatagraphCommandDownload.class) return "Download";
			if (subtype == DatagraphCommandExecute.class) return "Execute";
			if (subtype == DatagraphResponse.class) return "Response";
			return null;
		};
	}
}
