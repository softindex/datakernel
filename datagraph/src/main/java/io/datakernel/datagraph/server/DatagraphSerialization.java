/*
 * Copyright (C) 2015 SoftIndex LLC.
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

package io.datakernel.datagraph.server;

import com.google.gson.TypeAdapter;
import io.datakernel.codegen.DefiningClassLoader;
import io.datakernel.datagraph.graph.StreamId;
import io.datakernel.datagraph.node.*;
import io.datakernel.datagraph.node.NodeReduce.Input;
import io.datakernel.datagraph.server.command.*;
import io.datakernel.exception.ParseException;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.serializer.SerializerBuilder;
import io.datakernel.stream.processor.Sharder;
import io.datakernel.stream.processor.Sharders.HashSharder;
import io.datakernel.stream.processor.StreamJoin;
import io.datakernel.stream.processor.StreamMap;
import io.datakernel.stream.processor.StreamReducers;
import io.datakernel.stream.processor.StreamReducers.MergeDistinctReducer;
import io.datakernel.stream.processor.StreamReducers.MergeSortReducer;
import io.datakernel.stream.processor.StreamReducers.Reducer;
import io.datakernel.stream.processor.StreamReducers.ReducerToResult.AccumulatorToAccumulator;
import io.datakernel.stream.processor.StreamReducers.ReducerToResult.AccumulatorToOutput;
import io.datakernel.stream.processor.StreamReducers.ReducerToResult.InputToAccumulator;
import io.datakernel.stream.processor.StreamReducers.ReducerToResult.InputToOutput;
import io.datakernel.util.gson.TypeAdapterObject;
import io.datakernel.util.gson.TypeAdapterObjectSubtype;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

import static io.datakernel.util.Preconditions.checkArgument;
import static io.datakernel.util.gson.GsonAdapters.*;

/**
 * Responsible for setting up serialization operations for datagraph.
 * Provides specific complex adapters for datagraph objects.
 * Maintains the cache of BufferSerializer's.
 */
public final class DatagraphSerialization {
	private final Logger logger = LoggerFactory.getLogger(getClass());

	/**
	 * Store StreamIds as longs
	 */
	public static final TypeAdapter<StreamId> STREAM_ID_JSON = transform(LONG_JSON, StreamId::new, StreamId::getId).nullSafe();

	/**
	 * Store addresses as strings
	 */
	public static final TypeAdapter<InetSocketAddress> ADDRESS_JSON = transform(STRING_JSON,
			str -> {
				String[] split = str.split(":");
				checkArgument(split.length == 2);
				try {
					return new InetSocketAddress(InetAddress.getByName(split[0]), Integer.parseInt(split[1]));
				} catch (UnknownHostException e) {
					throw new ParseException(e);
				}
			},
			(InetSocketAddress addr) ->
					addr.getAddress().getHostAddress() + ':' + addr.getPort())
			.nullSafe();

	@SuppressWarnings("unchecked")
	public static final TypeAdapter<Predicate<Object>> PREDICATE_JSON = stateless();
	@SuppressWarnings("unchecked")
	public static final TypeAdapter<Function<Object, Object>> FUNCTION_JSON = stateless(Function.identity());
	@SuppressWarnings("unchecked")
	public static final TypeAdapter<Comparator<Object>> COMPARATOR_JSON = stateless();

	public static final TypeAdapterObjectSubtype<Sharder> SHARDER_JSON = TypeAdapterObjectSubtype.<Sharder>create()
			.withSubtype(HashSharder.class, "HashSharder", TypeAdapterObject.create(HashSharder::new)
					.with("partitions", INTEGER_JSON, HashSharder::getPartitions, HashSharder::setPartitions))
			.allOtherAreStateless();

	public static final TypeAdapter<StreamMap.Mapper<Object, Object>> MAPPER_JSON = stateless();
	public static final TypeAdapter<StreamJoin.Joiner<Object, Object, Object, Object>> JOINER_JSON = stateless();

	public static final TypeAdapter<StreamReducers.ReducerToResult> REDUCER_TO_RESULT_JSON = stateless();

	public static final TypeAdapterObjectSubtype<Reducer> REDUCER_JSON = TypeAdapterObjectSubtype.<Reducer>create()
			.withSubtype(InputToAccumulator.class, "InputToAccumulator", TypeAdapterObject.create(InputToAccumulator::new)
					.with("reducerToResult", REDUCER_TO_RESULT_JSON, InputToAccumulator::getReducerToResult, InputToAccumulator::setReducerToResult))
			.withSubtype(InputToOutput.class, "InputToOutput", TypeAdapterObject.create(InputToOutput::new)
					.with("reducerToResult", REDUCER_TO_RESULT_JSON, InputToOutput::getReducerToResult, InputToOutput::setReducerToResult))
			.withSubtype(AccumulatorToAccumulator.class, "AccumulatorToAccumulator", TypeAdapterObject.create(AccumulatorToAccumulator::new)
					.with("reducerToResult", REDUCER_TO_RESULT_JSON, AccumulatorToAccumulator::getReducerToResult, AccumulatorToAccumulator::setReducerToResult))
			.withSubtype(AccumulatorToOutput.class, "AccumulatorToOutput", TypeAdapterObject.create(AccumulatorToOutput::new)
					.with("reducerToResult", REDUCER_TO_RESULT_JSON, AccumulatorToOutput::getReducerToResult, AccumulatorToOutput::setReducerToResult))
			.withStatelessSubtype(MergeDistinctReducer::new, "MergeDeduplicateReducer")
			.withStatelessSubtype(MergeSortReducer::new, "MergeSortReducer")
			.allOtherAreStateless();

	@SuppressWarnings("unchecked")
	public static final TypeAdapterObjectSubtype<Node> NODE_JSON = TypeAdapterObjectSubtype.<Node>create()
			.withSubtype(NodeDownload.class, "Download", TypeAdapterObject.create(NodeDownload::new)
					.with("type", CLASS_JSON, NodeDownload::getType, NodeDownload::setType)
					.with("address", ADDRESS_JSON, NodeDownload::getAddress, NodeDownload::setAddress)
					.with("streamId", STREAM_ID_JSON, NodeDownload::getStreamId, NodeDownload::setStreamId)
					.with("output", STREAM_ID_JSON, NodeDownload::getOutput, NodeDownload::setOutput))

			.withSubtype(NodeUpload.class, "Upload", TypeAdapterObject.create(NodeUpload::new)
					.with("type", CLASS_JSON, NodeUpload::getType, NodeUpload::setType)
					.with("streamId", STREAM_ID_JSON, NodeUpload::getStreamId, NodeUpload::setStreamId))

			.withSubtype(NodeMap.class, "Map", TypeAdapterObject.create(NodeMap::new)
					.with("mapper", MAPPER_JSON, NodeMap::getMapper, NodeMap::setMapper)
					.with("input", STREAM_ID_JSON, NodeMap::getInput, NodeMap::setInput)
					.with("output", STREAM_ID_JSON, NodeMap::getOutput, NodeMap::setOutput))

			.withSubtype(NodeFilter.class, "Filter", TypeAdapterObject.create(NodeFilter::new)
					.with("predicate", PREDICATE_JSON, NodeFilter::getPredicate, NodeFilter::setPredicate)
					.with("input", STREAM_ID_JSON, NodeFilter::getInput, NodeFilter::setInput)
					.with("output", STREAM_ID_JSON, NodeFilter::getOutput, NodeFilter::setOutput))

			.withSubtype(NodeSort.class, "Sort", TypeAdapterObject.create(NodeSort::new)
					.with("keyFunction", FUNCTION_JSON, NodeSort::getKeyFunction, NodeSort::setKeyFunction)
					.with("keyComparator", COMPARATOR_JSON, NodeSort::getKeyComparator, NodeSort::setKeyComparator)
					.with("deduplicate", BOOLEAN_JSON, NodeSort::isDeduplicate, NodeSort::setDeduplicate)
					.with("itemsInMemorySize", INTEGER_JSON, NodeSort::getItemsInMemorySize, NodeSort::setItemsInMemorySize)
					.with("input", STREAM_ID_JSON, NodeSort::getInput, NodeSort::setInput)
					.with("output", STREAM_ID_JSON, NodeSort::getOutput, NodeSort::setOutput))

			.withSubtype(NodeShard.class, "Shard", TypeAdapterObject.create(NodeShard::new)
					.with("keyFunction", FUNCTION_JSON, NodeShard::getKeyFunction, NodeShard::setKeyFunction)
					.with("input", STREAM_ID_JSON, NodeShard::getInput, NodeShard::setInput)
					.with("outputs", ofList(STREAM_ID_JSON), NodeShard::getOutputs, NodeShard::setOutputs))

			.withSubtype(NodeMerge.class, "Merge", TypeAdapterObject.create(NodeMerge::new)
					.with("keyFunction", FUNCTION_JSON, NodeMerge::getKeyFunction, NodeMerge::setKeyFunction)
					.with("keyComparator", COMPARATOR_JSON, NodeMerge::getKeyComparator, NodeMerge::setKeyComparator)
					.with("deduplicate", BOOLEAN_JSON, NodeMerge::isDeduplicate, NodeMerge::setDeduplicate)
					.with("inputs", ofList(STREAM_ID_JSON), NodeMerge::getInputs, NodeMerge::setInputs)
					.with("output", STREAM_ID_JSON, NodeMerge::getOutput, NodeMerge::setOutput))

			.withSubtype(NodeReduce.class, "Reduce", TypeAdapterObject.create(NodeReduce::new)
					.with("keyComparator", COMPARATOR_JSON, NodeReduce::getKeyComparator, NodeReduce::setKeyComparator)
					.with("output", STREAM_ID_JSON, NodeReduce::getOutput, NodeReduce::setOutput)
					.with("inputs", ofMap(
							s -> Long.toString(s.getId()),
							s -> new StreamId(Long.parseLong(s)),
							TypeAdapterObject.create(Input::new)
									.with("reducer", REDUCER_JSON,
											Input::getReducer,
											(obj, reducer) -> obj.setReducer((Reducer<Object, ?, Object, Object>) reducer))
									.with("keyFunction", FUNCTION_JSON,
											obj -> (Function) obj.getKeyFunction(),
											Input::setKeyFunction)
					), NodeReduce::getInputs, NodeReduce::setInputs))

			.withSubtype(NodeReduceSimple.class, "ReduceSimple", TypeAdapterObject.create(NodeReduceSimple::new)
					.with("keyFunction", FUNCTION_JSON, NodeReduceSimple::getKeyFunction, NodeReduceSimple::setKeyFunction)
					.with("keyComparator", COMPARATOR_JSON, NodeReduceSimple::getKeyComparator, NodeReduceSimple::setKeyComparator)
					.with("reducer", REDUCER_JSON, NodeReduceSimple::getReducer,
							(obj, reducer) -> obj.setReducer((Reducer<Object, Object, Object, Object>) reducer))
					.with("inputs", ofList(STREAM_ID_JSON), NodeReduceSimple::getInputs, NodeReduceSimple::setInputs)
					.with("output", STREAM_ID_JSON, NodeReduceSimple::getOutput, NodeReduceSimple::setOutput))

			.withSubtype(NodeJoin.class, "Join", TypeAdapterObject.create(NodeJoin::new)
					.with("left", STREAM_ID_JSON, NodeJoin::getLeft, NodeJoin::setLeft)
					.with("right", STREAM_ID_JSON, NodeJoin::getRight, NodeJoin::setRight)
					.with("output", STREAM_ID_JSON, NodeJoin::getOutput, NodeJoin::setOutput)
					.with("keyComparator", COMPARATOR_JSON, NodeJoin::getKeyComparator, NodeJoin::setKeyComparator)
					.with("leftKeyFunction", FUNCTION_JSON, NodeJoin::getLeftKeyFunction, NodeJoin::setLeftKeyFunction)
					.with("rightKeyFunction", FUNCTION_JSON, NodeJoin::getRightKeyFunction, NodeJoin::setRightKeyFunction)
					.with("joiner", JOINER_JSON, NodeJoin::getJoiner, NodeJoin::setJoiner))

			.withSubtype(NodeUnion.class, "Union", TypeAdapterObject.create(NodeUnion::new)
					.with("inputs", ofList(STREAM_ID_JSON), NodeUnion::getInputs, NodeUnion::setInputs)
					.with("output", STREAM_ID_JSON, NodeUnion::getOutput, NodeUnion::setOutput))

			.withSubtype(NodeSupplierOfIterable.class, "SupplierOfIterable", TypeAdapterObject.create(NodeSupplierOfIterable::new)
					.with("iterableId", STRING_JSON, t1 -> (String) t1.getIterableId(), NodeSupplierOfIterable::setIterableId)
					.with("output", STREAM_ID_JSON, NodeSupplierOfIterable::getOutput, NodeSupplierOfIterable::setOutput))

			.withSubtype(NodeConsumerToList.class, "ConsumerToList", TypeAdapterObject.create(NodeConsumerToList::new)
					.with("iterableId", STRING_JSON, t -> (String) t.getListId(), NodeConsumerToList::setListId)
					.with("input", STREAM_ID_JSON, NodeConsumerToList::getInput, NodeConsumerToList::setInput));

	public static final TypeAdapterObjectSubtype<DatagraphCommand> COMMAND_JSON = TypeAdapterObjectSubtype.<DatagraphCommand>create()
			.withSubtype(DatagraphCommandDownload.class, "Download", TypeAdapterObject.create(DatagraphCommandDownload::new)
					.with("streamId", STREAM_ID_JSON, DatagraphCommandDownload::getStreamId, DatagraphCommandDownload::setStreamId))
			.withSubtype(DatagraphCommandExecute.class, "Execute", TypeAdapterObject.create(DatagraphCommandExecute::new)
					.with("nodes", ofList(NODE_JSON), DatagraphCommandExecute::getNodes, DatagraphCommandExecute::setNodes));

	public static final TypeAdapterObjectSubtype<DatagraphResponse> RESONSE_JSON = TypeAdapterObjectSubtype.<DatagraphResponse>create()
			.withStatelessSubtype(DatagraphResponseAck::new, "Ack")
			.withStatelessSubtype(DatagraphResponseDisconnect::new, "Disconnect")
			.withSubtype(DatagraphResponseExecute.class, "Execute", TypeAdapterObject.create(DatagraphResponseExecute::new)
					.with("nodeIds", ofList(INTEGER_JSON), DatagraphResponseExecute::getNodeIds, DatagraphResponseExecute::setNodeIds));

	public final TypeAdapter<DatagraphCommand> commandAdapter;
	public final TypeAdapter<DatagraphResponse> responseAdapter;
	public final TypeAdapter<Node> nodeAdapter;

	private final Map<Class<?>, BufferSerializer<?>> serializers = new HashMap<>();

	private DatagraphSerialization(TypeAdapter<DatagraphCommand> commandAdapter, TypeAdapter<DatagraphResponse> responseAdapter, TypeAdapter<Node> nodeAdapter) {
		this.commandAdapter = commandAdapter;
		this.responseAdapter = responseAdapter;
		this.nodeAdapter = nodeAdapter;
	}

	public static DatagraphSerialization create() {
		return new DatagraphSerialization(COMMAND_JSON, RESONSE_JSON, NODE_JSON);
	}

	public DatagraphSerialization withNodeAdapter(TypeAdapter<Node> nodeAdapter) {
		return new DatagraphSerialization(commandAdapter, responseAdapter, nodeAdapter);
	}

	public DatagraphSerialization withMessagingAdapters(TypeAdapter<DatagraphCommand> commandAdapter, TypeAdapter<DatagraphResponse> responseAdapter) {
		return new DatagraphSerialization(commandAdapter, responseAdapter, nodeAdapter);
	}

	@SuppressWarnings("unchecked")
	public synchronized <T> BufferSerializer<T> getSerializer(Class<T> type) {
		BufferSerializer<T> serializer = (BufferSerializer<T>) serializers.get(type);
		if (serializer == null) {
			logger.info("Creating serializer for {}", type);
			serializer = SerializerBuilder.create(DefiningClassLoader.create(ClassLoader.getSystemClassLoader())).build(type);
			serializers.put(type, serializer);
		}
		return serializer;
	}
}
