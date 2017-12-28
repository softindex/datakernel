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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import io.datakernel.codegen.DefiningClassLoader;
import io.datakernel.datagraph.graph.StreamId;
import io.datakernel.datagraph.node.*;
import io.datakernel.datagraph.server.command.DatagraphCommand;
import io.datakernel.datagraph.server.command.DatagraphCommandDownload;
import io.datakernel.datagraph.server.command.DatagraphCommandExecute;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.serializer.GsonSubclassesAdapter;
import io.datakernel.serializer.SerializerBuilder;
import io.datakernel.stream.processor.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Responsible for setting up serialization operations for datagraph.
 * Provides Gson object with specific adapters.
 * Maintains the cache of BufferSerializer's.
 */
public final class DatagraphSerialization {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	public final Gson gson;

	private final Map<Class<?>, BufferSerializer<?>> serializers = new HashMap<>();

	public DatagraphSerialization() {
		this(Collections.emptyList());
	}

	/**
	 * Constructs a new datagraph serialization object, initializes Gson object with used classes.
	 */
	public DatagraphSerialization(List<NodeSubclass> nodeSubclasses) {
//		Class<?> naturalOrderingClass;
//		Class<?> identityFunctionClass;
//		try {
//			naturalOrderingClass = Class.forName("com.google.common.collect.NaturalOrdering");
//			identityFunctionClass = Class.forName("com.google.common.base.Functions$IdentityFunction");
//		} catch (ClassNotFoundException e) {
//			throw new RuntimeException(e);
//		}

		GsonSubclassesAdapter<Object> gsonSubclassesAdapter = GsonSubclassesAdapter.create()
				.withSubclassField("nodeType")
				.withSubclass("Download", NodeDownload.class)
				.withSubclass("Upload", NodeUpload.class)
				.withSubclass("Map", NodeMap.class)
				.withSubclass("Filter", NodeFilter.class)
				.withSubclass("Sort", NodeSort.class)
				.withSubclass("Shard", NodeShard.class)
				.withSubclass("Merge", NodeMerge.class)
				.withSubclass("Reduce", NodeReduce.class)
				.withSubclass("ReduceSimple", NodeReduceSimple.class)
				.withSubclass("Join", NodeJoin.class)
				.withSubclass("Union", NodeUnion.class)
				.withSubclass("ProducerOfIterable", NodeProducerOfIterable.class)
				.withSubclass("ConsumerToList", NodeConsumerToList.class);

		for (NodeSubclass nodeSubclass : nodeSubclasses) {
			gsonSubclassesAdapter =
					gsonSubclassesAdapter.withSubclass(nodeSubclass.getSubclassName(), nodeSubclass.getSubclass());
		}

		this.gson = new GsonBuilder()
				.registerTypeAdapter(Class.class, new GsonClassTypeAdapter())
				.registerTypeAdapter(StreamId.class, new GsonStreamIdAdapter())
				.registerTypeAdapter(InetSocketAddress.class, new GsonInetSocketAddressAdapter())
				.registerTypeAdapter(DatagraphCommand.class, GsonSubclassesAdapter.create()
						.withSubclassField("commandType")
						.withSubclass("Download", DatagraphCommandDownload.class)
						.withSubclass("Execute", DatagraphCommandExecute.class))
				.registerTypeAdapter(Node.class, gsonSubclassesAdapter)
				.registerTypeAdapter(Predicate.class, GsonSubclassesAdapter.create()
						.withSubclassField("predicateType"))
				.registerTypeAdapter(Function.class, GsonSubclassesAdapter.create()
						.withSubclassField("functionType")
//						.withClassTag(identityFunctionClass.getName(), identityFunctionClass, (InstanceCreator<Object>) type -> Functions.identity()))
						.withClassTag(Function.identity().getClass().getName(), Function.identity().getClass(), type -> Function.identity()))
				.registerTypeAdapter(Comparator.class, GsonSubclassesAdapter.create()
						.withSubclassField("comparatorType")
//						.withClassTag(naturalOrderingClass.getName(), naturalOrderingClass, type -> Ordering.natural()))
						.withClassTag(Comparator.class.getName(), Comparator.class, type -> Comparator.naturalOrder()))
				.registerTypeAdapter(StreamMap.Mapper.class, GsonSubclassesAdapter.create()
						.withSubclassField("mapperType"))
				.registerTypeAdapter(StreamReducers.Reducer.class, GsonSubclassesAdapter.create()
						.withSubclassField("reducerType")
						.withClassTag("MergeDeduplicateReducer", StreamReducers.MergeDeduplicateReducer.class)
						.withClassTag("MergeSortReducer", StreamReducers.MergeSortReducer.class)
						.withSubclass("InputToAccumulator", StreamReducers.ReducerToResult.InputToAccumulator.class)
						.withSubclass("InputToOutput", StreamReducers.ReducerToResult.InputToOutput.class)
						.withSubclass("AccumulatorToAccumulator", StreamReducers.ReducerToResult.AccumulatorToAccumulator.class)
						.withSubclass("AccumulatorToOutput", StreamReducers.ReducerToResult.AccumulatorToOutput.class))
				.registerTypeAdapter(StreamReducers.ReducerToResult.class, GsonSubclassesAdapter.create()
						.withSubclassField("reducerToResultType"))
				.registerTypeAdapter(Sharder.class, GsonSubclassesAdapter.create()
						.withSubclassField("sharderType")
						.withSubclass("HashSharder", Sharders.HashSharder.class))
				.registerTypeAdapter(StreamJoin.Joiner.class, GsonSubclassesAdapter.create()
						.withSubclassField("joinerType"))
				.setPrettyPrinting()
				.enableComplexMapKeySerialization()
				.create();
	}

	@SuppressWarnings("unchecked")
	public synchronized <T> BufferSerializer<T> getSerializer(Class<T> type) {
		BufferSerializer<T> serializer = (BufferSerializer<T>) serializers.get(type);
		if (serializer == null) {
			logger.info("Creating serializer for {}", type);
			serializer = SerializerBuilder.create(
					DefiningClassLoader.create(ClassLoader.getSystemClassLoader())).build(type);
			serializers.put(type, serializer);
		}
		return serializer;
	}

	<T> boolean checkGson(T item, Class<T> type) {
		String str = null;
		try {
			str = gson.toJson(item, type);
			gson.fromJson(str, type);
			return true;
		} catch (JsonSyntaxException e) {
			logger.error("Gson error:\n{}\n\n{}", e, str);
			throw e;
		}
	}
}
