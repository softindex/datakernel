/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.datakernel.dataflow.server;

import io.datakernel.codec.CodecSubtype;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.codec.StructuredCodecs;
import io.datakernel.codec.StructuredOutput;
import io.datakernel.codegen.DefiningClassLoader;
import io.datakernel.dataflow.graph.StreamId;
import io.datakernel.dataflow.node.*;
import io.datakernel.dataflow.server.command.*;
import io.datakernel.exception.ParseException;
import io.datakernel.logger.LoggerFactory;
import io.datakernel.serializer.BinarySerializer;
import io.datakernel.serializer.SerializerBuilder;
import io.datakernel.stream.processor.StreamJoin.Joiner;
import io.datakernel.stream.processor.StreamReducers.MergeDistinctReducer;
import io.datakernel.stream.processor.StreamReducers.MergeSortReducer;
import io.datakernel.stream.processor.StreamReducers.Reducer;
import io.datakernel.stream.processor.StreamReducers.ReducerToResult;
import io.datakernel.stream.processor.StreamReducers.ReducerToResult.AccumulatorToAccumulator;
import io.datakernel.stream.processor.StreamReducers.ReducerToResult.AccumulatorToOutput;
import io.datakernel.stream.processor.StreamReducers.ReducerToResult.InputToAccumulator;
import io.datakernel.stream.processor.StreamReducers.ReducerToResult.InputToOutput;
import io.datakernel.util.Initializable;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

import static io.datakernel.codec.StructuredCodecs.*;
import static java.lang.ClassLoader.getSystemClassLoader;

/**
 * Responsible for setting up serialization operations for datagraph.
 * Provides specific complex codecs for datagraph objects.
 * Maintains the cache of BufferSerializer's.
 */
@SuppressWarnings({"rawtypes", "unchecked", "WeakerAccess"})
public final class DatagraphSerialization implements Initializable<DatagraphSerialization> {
	static final Logger logger = LoggerFactory.getLogger(DatagraphSerialization.class.getName());

	final class CodecProvider<T> {
		private StructuredCodec<T> ref;
		private final Supplier<StructuredCodec<T>> supplier;

		CodecProvider(Supplier<StructuredCodec<T>> supplier) {this.supplier = supplier;}

		StructuredCodec<T> get() {
			if (ref != null) return ref;
			ref = supplier.get();
			return ref;
		}
	}

	private <T> CodecProvider providerOf(Supplier<StructuredCodec<T>> supplier) {
		return new CodecProvider(supplier);
	}

	@SuppressWarnings("CastCanBeRemovedNarrowingVariableType")
	private <T> CodecSubtype<T> createCodec(Class<T> type) {
		CodecSubtype<?> subtypeCodec = CodecSubtype.<T>create();
		for (Map.Entry<Class<?>, StructuredCodec<?>> entry : userDefinedTypes.entrySet()) {
			Class<?> userDefinedType = entry.getKey();
			StructuredCodec<?> codec = entry.getValue();
			if (type.isAssignableFrom(userDefinedType)) {
				subtypeCodec.with(userDefinedType, (StructuredCodec) codec);
			}
		}
		return (CodecSubtype<T>) subtypeCodec;
	}

	/**
	 * Store StreamIds as longs
	 */
	static final StructuredCodec<StreamId> STREAM_ID_CODEC = StructuredCodec.of(
			in -> new StreamId(in.readLong()),
			(out, item) -> out.writeLong(item.getId())
	);

	/**
	 * Store addresses as strings
	 */
	static final StructuredCodec<InetSocketAddress> ADDRESS_CODEC = StructuredCodec.of(
			in -> {
				String str = in.readString();
				String[] split = str.split(":");
				if (split.length != 2) {
					throw new ParseException("Address should be splitted with a single ':'");
				}
				try {
					return new InetSocketAddress(InetAddress.getByName(split[0]), Integer.parseInt(split[1]));
				} catch (UnknownHostException e) {
					throw new ParseException(DatagraphSerialization.class, "Failed to create InetSocketAdress", e);
				}
			},
			(out, addr) -> out.writeString(addr.getAddress().getHostAddress() + ':' + addr.getPort())
	);

	final CodecProvider<Predicate> predicate = providerOf(() -> createCodec(Predicate.class));
	final CodecProvider<Function> function = providerOf(() -> createCodec(Function.class));
	final CodecProvider<Comparator> comparator = providerOf(() -> createCodec(Comparator.class));

	final CodecProvider<Joiner> joiner = providerOf(() -> createCodec(Joiner.class));

	final CodecProvider<ReducerToResult> REDUCER_TO_RESULT_PROVIDER = providerOf(() -> createCodec(ReducerToResult.class));

	final CodecProvider<Reducer> reducer = providerOf(() -> createCodec(Reducer.class)
			.with(InputToAccumulator.class,
					StructuredCodec.ofObject(
							in -> new InputToAccumulator(
									in.readKey("reducerToResult", REDUCER_TO_RESULT_PROVIDER.get())),
							(StructuredOutput out, InputToAccumulator item) ->
									out.writeKey("reducerToResult", REDUCER_TO_RESULT_PROVIDER.get(), item.getReducerToResult())))

			.with(InputToOutput.class,
					StructuredCodec.ofObject(
							in -> new InputToOutput(
									in.readKey("reducerToResult", REDUCER_TO_RESULT_PROVIDER.get())),
							(StructuredOutput out, InputToOutput item) ->
									out.writeKey("reducerToResult", REDUCER_TO_RESULT_PROVIDER.get(), item.getReducerToResult())))

			.with(AccumulatorToAccumulator.class,
					StructuredCodec.ofObject(
							in -> new AccumulatorToAccumulator(
									in.readKey("reducerToResult", REDUCER_TO_RESULT_PROVIDER.get())),
							(StructuredOutput out, AccumulatorToAccumulator item) ->
									out.writeKey("reducerToResult", REDUCER_TO_RESULT_PROVIDER.get(), item.getReducerToResult())))

			.with(AccumulatorToOutput.class,
					StructuredCodec.ofObject(
							in -> new AccumulatorToOutput(
									in.readKey("reducerToResult", REDUCER_TO_RESULT_PROVIDER.get())),
							(StructuredOutput out, AccumulatorToOutput item) ->
									out.writeKey("reducerToResult", REDUCER_TO_RESULT_PROVIDER.get(), item.getReducerToResult())))

			.with(MergeDistinctReducer.class,
					StructuredCodec.ofObject(
							in -> new MergeDistinctReducer(),
							(StructuredOutput out, MergeDistinctReducer item) -> {}))

			.with(MergeSortReducer.class,
					StructuredCodec.ofObject(
							in -> new MergeSortReducer(),
							(StructuredOutput out, MergeSortReducer item) -> {}))
	);

	private final CodecProvider<Map<StreamId, NodeReduce.Input>> nodeReduceInput = providerOf(() ->
			StructuredCodecs.ofMap(
					STREAM_ID_CODEC,
					StructuredCodec.ofObject(
							in -> new NodeReduce.Input<>(
									in.readKey("reducer", reducer.get()),
									in.readKey("keyFunction", function.get())),
							(out, item) -> {
								out.writeKey("reducer", reducer.get(), item.getReducer());
								out.writeKey("keyFunction", function.get(), item.getKeyFunction());
							}
					)));

	@SuppressWarnings("unchecked")
	final CodecProvider<Node> node = providerOf(() -> createCodec(Node.class)
			.with(NodeDownload.class,
					StructuredCodec.ofObject(
							in -> new NodeDownload(
									in.readKey("type", CLASS_CODEC),
									in.readKey("address", ADDRESS_CODEC),
									in.readKey("streamId", STREAM_ID_CODEC),
									in.readKey("output", STREAM_ID_CODEC)
							),
							(StructuredOutput out, NodeDownload item) -> {
								out.writeKey("type", ofClass(), (Class<Object>) item.getType());
								out.writeKey("address", ADDRESS_CODEC, item.getAddress());
								out.writeKey("streamId", STREAM_ID_CODEC, item.getStreamId());
								out.writeKey("output", STREAM_ID_CODEC, item.getOutput());
							}))

			.with(NodeUpload.class,
					StructuredCodec.ofObject(
							in -> new NodeUpload(
									in.readKey("type", CLASS_CODEC),
									in.readKey("streamId", STREAM_ID_CODEC)
							),
							(StructuredOutput out, NodeUpload item) -> {
								out.writeKey("type", CLASS_CODEC, (Class<Object>) item.getType());
								out.writeKey("streamId", STREAM_ID_CODEC, item.getStreamId());
							}))

			.with(NodeMap.class,
					StructuredCodec.ofObject(
							in -> new NodeMap(
									in.readKey("function", function.get()),
									in.readKey("input", STREAM_ID_CODEC),
									in.readKey("output", STREAM_ID_CODEC)
							),
							(StructuredOutput out, NodeMap item) -> {
								out.writeKey("function", function.get(), item.getFunction());
								out.writeKey("input", STREAM_ID_CODEC, item.getInput());
								out.writeKey("output", STREAM_ID_CODEC, item.getOutput());
							}))

			.with(NodeFilter.class,
					StructuredCodec.ofObject(
							in -> new NodeFilter(
									in.readKey("predicate", predicate.get()),
									in.readKey("input", STREAM_ID_CODEC),
									in.readKey("output", STREAM_ID_CODEC)
							),
							(StructuredOutput out, NodeFilter item) -> {
								out.writeKey("predicate", predicate.get(), item.getPredicate());
								out.writeKey("input", STREAM_ID_CODEC, item.getInput());
								out.writeKey("output", STREAM_ID_CODEC, item.getOutput());
							}))

			.with(NodeSort.class,
					StructuredCodec.ofObject(
							in -> new NodeSort(
									in.readKey("keyFunction", function.get()),
									in.readKey("keyComparator", comparator.get()),
									in.readKey("deduplicate", BOOLEAN_CODEC),
									in.readKey("itemsInMemorySize", INT_CODEC),
									in.readKey("input", STREAM_ID_CODEC),
									in.readKey("output", STREAM_ID_CODEC)),
							(StructuredOutput out, NodeSort node) -> {
								out.writeKey("keyFunction", function.get(), node.getKeyFunction());
								out.writeKey("keyComparator", comparator.get(), node.getKeyComparator());
								out.writeKey("deduplicate", BOOLEAN_CODEC, node.isDeduplicate());
								out.writeKey("itemsInMemorySize", INT_CODEC, node.getItemsInMemorySize());
								out.writeKey("input", STREAM_ID_CODEC, node.getInput());
								out.writeKey("output", STREAM_ID_CODEC, node.getOutput());
							}))

			.with(NodeShard.class,
					StructuredCodec.ofObject(
							in -> new NodeShard(
									in.readKey("keyFunction", function.get()),
									in.readKey("input", STREAM_ID_CODEC),
									in.readKey("outputs", ofList(STREAM_ID_CODEC))),
							(StructuredOutput out, NodeShard node) -> {
								out.writeKey("keyFunction", function.get(), node.getKeyFunction());
								out.writeKey("input", STREAM_ID_CODEC, node.getInput());
								out.writeKey("outputs", ofList(STREAM_ID_CODEC), (List<StreamId>) node.getOutputs());
							}))

			.with(NodeMerge.class,
					StructuredCodec.ofObject(
							in -> new NodeMerge(
									in.readKey("keyFunction", function.get()),
									in.readKey("keyComparator", comparator.get()),
									in.readKey("deduplicate", BOOLEAN_CODEC),
									in.readKey("inputs", ofList(STREAM_ID_CODEC)),
									in.readKey("output", STREAM_ID_CODEC)),
							(StructuredOutput out, NodeMerge node) -> {
								out.writeKey("keyFunction", function.get(), node.getKeyFunction());
								out.writeKey("keyComparator", comparator.get(), node.getKeyComparator());
								out.writeKey("deduplicate", BOOLEAN_CODEC, node.isDeduplicate());
								out.writeKey("inputs", ofList(STREAM_ID_CODEC), (List<StreamId>) node.getInputs());
								out.writeKey("output", STREAM_ID_CODEC, node.getOutput());
							}))

			.with(NodeReduce.class,
					StructuredCodec.ofObject(
							in -> new NodeReduce(
									in.readKey("keyComparator", comparator.get()),
									in.readKey("inputs", nodeReduceInput.get()),
									in.readKey("output", STREAM_ID_CODEC)),
							(StructuredOutput out, NodeReduce v) -> {
								out.writeKey("keyComparator", comparator.get(), v.getKeyComparator());
								out.writeKey("inputs", nodeReduceInput.get(), (Map<StreamId, NodeReduce.Input>) v.getInputs());
								out.writeKey("output", STREAM_ID_CODEC, v.getOutput());
							}))

			.with(NodeReduceSimple.class,
					StructuredCodec.ofObject(
							in -> new NodeReduceSimple(
									in.readKey("keyFunction", function.get()),
									in.readKey("keyComparator", comparator.get()),
									in.readKey("reducer", reducer.get()),
									in.readKey("inputs", ofList(STREAM_ID_CODEC)),
									in.readKey("output", STREAM_ID_CODEC)),
							(StructuredOutput out, NodeReduceSimple node) -> {
								out.writeKey("keyFunction", function.get(), node.getKeyFunction());
								out.writeKey("keyComparator", comparator.get(), node.getKeyComparator());
								out.writeKey("reducer", reducer.get(), node.getReducer());
								out.writeKey("inputs", ofList(STREAM_ID_CODEC), (List<StreamId>) node.getInputs());
								out.writeKey("output", STREAM_ID_CODEC, node.getOutput());
							}))

			.with(NodeJoin.class,
					StructuredCodec.ofObject(
							in -> new NodeJoin(
									in.readKey("left", STREAM_ID_CODEC),
									in.readKey("right", STREAM_ID_CODEC),
									in.readKey("output", STREAM_ID_CODEC),
									in.readKey("keyComparator", comparator.get()),
									in.readKey("leftKeyFunction", function.get()),
									in.readKey("rightKeyFunction", function.get()),
									in.readKey("joiner", joiner.get())),
							(StructuredOutput out, NodeJoin node) -> {
								out.writeKey("left", STREAM_ID_CODEC, node.getLeft());
								out.writeKey("right", STREAM_ID_CODEC, node.getRight());
								out.writeKey("output", STREAM_ID_CODEC, node.getOutput());
								out.writeKey("keyComparator", comparator.get(), node.getKeyComparator());
								out.writeKey("leftKeyFunction", function.get(), node.getLeftKeyFunction());
								out.writeKey("rightKeyFunction", function.get(), node.getRightKeyFunction());
								out.writeKey("joiner", joiner.get(), node.getJoiner());
							}))

			.with(NodeUnion.class,
					StructuredCodec.ofObject(
							in -> new NodeUnion(
									in.readKey("inputs", ofList(STREAM_ID_CODEC)),
									in.readKey("output", STREAM_ID_CODEC)),
							(StructuredOutput out, NodeUnion node) -> {
								out.writeKey("inputs", ofList(STREAM_ID_CODEC), (List<StreamId>) node.getInputs());
								out.writeKey("output", STREAM_ID_CODEC, node.getOutput());
							}))

			.with(NodeSupplierOfIterable.class,
					StructuredCodec.ofObject(
							in -> new NodeSupplierOfIterable(
									in.readKey("iterableId", STRING_CODEC),
									in.readKey("output", STREAM_ID_CODEC)),
							(StructuredOutput out, NodeSupplierOfIterable node) -> {
								out.writeKey("iterableId", STRING_CODEC, (String) node.getIterableId());
								out.writeKey("output", STREAM_ID_CODEC, node.getOutput());
							}))

			.with(NodeConsumerToList.class,
					StructuredCodec.ofObject(
							in -> new NodeConsumerToList(
									in.readKey("input", STREAM_ID_CODEC),
									in.readKey("listId", STRING_CODEC)),
							(StructuredOutput out, NodeConsumerToList node) -> {
								out.writeKey("input", STREAM_ID_CODEC, node.getInput());
								out.writeKey("listId", STRING_CODEC, (String) node.getListId());
							}))
	);

	final CodecProvider<DatagraphCommand> command = providerOf(() -> createCodec(DatagraphCommand.class)
			.with(DatagraphCommandDownload.class, "Download",
					object(DatagraphCommandDownload::new,
							"streamId", DatagraphCommandDownload::getStreamId, STREAM_ID_CODEC))

			.with(DatagraphCommandExecute.class, "Execute",
					object(DatagraphCommandExecute::new,
							"nodes", DatagraphCommandExecute::getNodes, ofList(node.get()))));

	final CodecProvider<DatagraphResponse> response = providerOf(() -> createCodec(DatagraphResponse.class)
			.with(DatagraphResponseAck.class, "Ack",
					object(DatagraphResponseAck::new))

			.with(DatagraphResponseDisconnect.class, "Disconnect",
					object(DatagraphResponseDisconnect::new))

			.with(DatagraphResponseExecute.class, "Execute",
					object(DatagraphResponseExecute::new,
							"nodeIds", DatagraphResponseExecute::getNodeIds, ofList(INT_CODEC))));

	private final Map<Class<?>, StructuredCodec<?>> userDefinedTypes = new HashMap<>();
	private final Map<Class<?>, BinarySerializer<?>> serializers = new HashMap<>();

	private DatagraphSerialization() {
	}

	public static DatagraphSerialization create() {
		return new DatagraphSerialization();
	}

	public <T> DatagraphSerialization withCodec(Class<T> type, StructuredCodec<T> codec) {
		this.userDefinedTypes.put(type, codec);
		return this;
	}

	public <T> DatagraphSerialization withBufferSerializer(Class<T> type, BinarySerializer<T> serializer) {
		this.serializers.put(type, serializer);
		return this;
	}

	public StructuredCodec<DatagraphCommand> getCommandCodec() {
		return command.get();
	}

	public StructuredCodec<DatagraphResponse> getResponseCodec() {
		return response.get();
	}

	public StructuredCodec<Node> getNodeCodec() {
		return node.get();
	}

	@SuppressWarnings("unchecked")
	public synchronized <T> BinarySerializer<T> getSerializer(Class<T> type) {
		BinarySerializer<T> serializer = (BinarySerializer<T>) serializers.get(type);
		if (serializer == null) {
			logger.log(Level.INFO, "Creating serializer for {}", type);
			serializer = SerializerBuilder.create(DefiningClassLoader.create(getSystemClassLoader()))
					.build(type);
			serializers.put(type, serializer);
		}
		return serializer;
	}
}
