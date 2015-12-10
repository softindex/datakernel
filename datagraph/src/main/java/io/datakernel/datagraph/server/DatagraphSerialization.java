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

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.Ordering;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.InstanceCreator;
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

import java.lang.reflect.Type;
import java.net.InetSocketAddress;
import java.util.*;

/**
 * Responsible for setting up serialization operations for datagraph.
 * Provides Gson object with specific adapters.
 * Maintains the cache of BufferSerializer's.
 */
public final class DatagraphSerialization {
	private static final Logger logger = LoggerFactory.getLogger(DatagraphSerialization.class);

	public final Gson gson;

	private final Map<Class<?>, BufferSerializer<?>> serializers = new HashMap<>();

	public DatagraphSerialization() {
		this(Collections.<NodeSubclass>emptyList());
	}

	/**
	 * Constructs a new datagraph serialization object, initializes Gson object with used classes.
	 */
	public DatagraphSerialization(List<NodeSubclass> nodeSubclasses) {
		Class<?> naturalOrderingClass;
		Class<?> identityFunctionClass;
		try {
			naturalOrderingClass = Class.forName("com.google.common.collect.NaturalOrdering");
			identityFunctionClass = Class.forName("com.google.common.base.Functions$IdentityFunction");
		} catch (ClassNotFoundException e) {
			throw Throwables.propagate(e);
		}

		GsonSubclassesAdapter.Builder<Object> gsonSubclassesAdapterBuilder = GsonSubclassesAdapter.builder()
				.subclassField("nodeType")
				.subclass("Download", NodeDownload.class)
				.subclass("Upload", NodeUpload.class)
				.subclass("Map", NodeMap.class)
				.subclass("Filter", NodeFilter.class)
				.subclass("Sort", NodeSort.class)
				.subclass("Shard", NodeShard.class)
				.subclass("Merge", NodeMerge.class)
				.subclass("Reduce", NodeReduce.class)
				.subclass("ReduceSimple", NodeReduceSimple.class)
				.subclass("Join", NodeJoin.class)
				.subclass("ProducerOfIterable", NodeProducerOfIterable.class)
				.subclass("ConsumerToList", NodeConsumerToList.class);

		for (NodeSubclass nodeSubclass : nodeSubclasses) {
			gsonSubclassesAdapterBuilder.subclass(nodeSubclass.getSubclassName(), nodeSubclass.getSubclass());
		}

		this.gson = new GsonBuilder()
				.registerTypeAdapter(Class.class, new GsonClassTypeAdapter())
				.registerTypeAdapter(StreamId.class, new GsonStreamIdAdapter())
				.registerTypeAdapter(InetSocketAddress.class, new GsonInetSocketAddressAdapter())
				.registerTypeAdapter(DatagraphCommand.class, GsonSubclassesAdapter.builder()
						.subclassField("commandType")
						.subclass("Download", DatagraphCommandDownload.class)
						.subclass("Execute", DatagraphCommandExecute.class)
						.build())
				.registerTypeAdapter(Node.class, gsonSubclassesAdapterBuilder.build())
				.registerTypeAdapter(Predicate.class, GsonSubclassesAdapter.builder()
						.subclassField("predicateType")
						.build())
				.registerTypeAdapter(Function.class, GsonSubclassesAdapter.builder()
						.subclassField("functionType")
						.classTag(identityFunctionClass.getName(), identityFunctionClass, new InstanceCreator<Object>() {
							@Override
							public Object createInstance(Type type) {
								return Functions.identity();
							}
						})
						.build())
				.registerTypeAdapter(Comparator.class, GsonSubclassesAdapter.builder()
						.subclassField("comparatorType")
						.classTag(naturalOrderingClass.getName(), naturalOrderingClass, new InstanceCreator<Object>() {
							@Override
							public Object createInstance(Type type) {
								return Ordering.natural();
							}
						})
						.build())
				.registerTypeAdapter(StreamMap.Mapper.class, GsonSubclassesAdapter.builder()
						.subclassField("mapperType")
						.build())
				.registerTypeAdapter(StreamReducers.Reducer.class, GsonSubclassesAdapter.builder()
						.subclassField("reducerType")
						.classTag("MergeDeduplicateReducer", StreamReducers.MergeDeduplicateReducer.class)
						.classTag("MergeSortReducer", StreamReducers.MergeSortReducer.class)
						.subclass("InputToAccumulator", StreamReducers.ReducerToResult.InputToAccumulator.class)
						.subclass("InputToOutput", StreamReducers.ReducerToResult.InputToOutput.class)
						.subclass("AccumulatorToAccumulator", StreamReducers.ReducerToResult.AccumulatorToAccumulator.class)
						.subclass("AccumulatorToOutput", StreamReducers.ReducerToResult.AccumulatorToOutput.class)
						.build())
				.registerTypeAdapter(StreamReducers.ReducerToResult.class, GsonSubclassesAdapter.builder()
						.subclassField("reducerToResultType")
						.build())
				.registerTypeAdapter(Sharder.class, GsonSubclassesAdapter.builder()
						.subclassField("sharderType")
						.subclass("HashSharder", Sharders.HashSharder.class)
						.build())
				.registerTypeAdapter(StreamJoin.Joiner.class, GsonSubclassesAdapter.builder()
						.subclassField("joinerType")
						.build())
				.setPrettyPrinting()
				.enableComplexMapKeySerialization()
				.create();
	}

	public synchronized <T> BufferSerializer<T> getSerializer(Class<T> type) {
		BufferSerializer<T> serializer = (BufferSerializer<T>) serializers.get(type);
		if (serializer == null) {
			try {
				logger.info("Creating serializer for {}", type);
				serializer = SerializerBuilder.newDefaultSerializer(type, ClassLoader.getSystemClassLoader());
				serializers.put(type, serializer);
			} catch (Exception e) {
				logger.error("Error creating serializer for {}", type, e);
			}
		}
		return serializer;
	}

	<T> boolean checkGson(T item, Class<T> type) {
		String str = null;
		try {
			str = gson.toJson(item, type);
			gson.fromJson(str, type);
			return true;
		} catch (Exception e) {
			logger.error("Gson error:\n{}\n\n{}", e, str);
			throw e;
		}
	}
}
