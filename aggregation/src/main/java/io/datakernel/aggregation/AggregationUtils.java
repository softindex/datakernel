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

package io.datakernel.aggregation;

import com.google.common.base.Function;
import com.google.common.collect.Sets;
import io.datakernel.aggregation.fieldtype.FieldType;
import io.datakernel.aggregation.measure.Measure;
import io.datakernel.aggregation.util.BiPredicate;
import io.datakernel.aggregation.util.Predicates;
import io.datakernel.codegen.ClassBuilder;
import io.datakernel.codegen.DefiningClassLoader;
import io.datakernel.codegen.Expression;
import io.datakernel.codegen.PredicateDefAnd;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.serializer.SerializerBuilder;
import io.datakernel.serializer.asm.SerializerGenClass;
import io.datakernel.stream.processor.StreamMap;
import io.datakernel.stream.processor.StreamReducers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.propagate;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newLinkedHashSet;
import static io.datakernel.codegen.Expressions.*;

/**
 * Defines a structure of an aggregation.
 * It is defined by keys, fields and their types.
 * Contains methods for defining dynamic classes, that are used for different operations.
 * Provides serializer for records that have the defined structure.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class AggregationUtils {
	private final static Logger logger = LoggerFactory.getLogger(AggregationUtils.class);

	private AggregationUtils() {
	}

	public static Map<String, FieldType> projectKeys(Map<String, FieldType> keyTypes, List<String> keys) {
		return projectMap(keyTypes, keys);
	}

	public static Map<String, FieldType> projectFields(Map<String, FieldType> fieldTypes, List<String> fields) {
		return projectMap(fieldTypes, fields);
	}

	private static <K, V> Map<K, V> projectMap(Map<K, V> map, Collection<K> keys) {
		keys = new HashSet<>(keys);
		checkArgument(map.keySet().containsAll(keys), "Unknown fields: " + Sets.difference(newLinkedHashSet(keys), map.keySet()));
		LinkedHashMap<K, V> result = new LinkedHashMap<>();
		for (Map.Entry<K, V> entry : map.entrySet()) {
			if (keys.contains(entry.getKey())) {
				result.put(entry.getKey(), entry.getValue());
			}
		}
		return result;
	}

	public static Class<?> createKeyClass(Aggregation aggregation, List<String> keys, DefiningClassLoader classLoader) {
		return createKeyClass(projectKeys(aggregation.getKeyTypes(), keys), classLoader);
	}

	public static Class<?> createKeyClass(Map<String, FieldType> keys, DefiningClassLoader classLoader) {
		logger.trace("Creating key class for keys {}", keys.keySet());
		ClassBuilder builder = ClassBuilder.create(classLoader, Comparable.class);
		for (String key : keys.keySet()) {
			builder = builder.withField(key, keys.get(key).getInternalDataType());
		}
		List<String> keyList = new ArrayList<>(keys.keySet());
		builder = builder
				.withMethod("compareTo", compareTo(keyList))
				.withMethod("equals", asEquals(keyList))
				.withMethod("hashCode", hashCodeOfThis(keyList))
				.withMethod("toString", asString(keyList));

		return builder.build();
	}

	public static Comparator createKeyComparator(Class<?> recordClass, List<String> keys, DefiningClassLoader classLoader) {
		Expression comparator = compare(recordClass, keys);
		ClassBuilder<Comparator> builder = ClassBuilder.create(classLoader, Comparator.class)
				.withMethod("compare", comparator);
		return builder.buildClassAndCreateNewInstance();
	}

	public static StreamMap.MapperProjection createMapper(Class<?> recordClass, Class<?> resultClass,
	                                                      List<String> keys, List<String> fields,
	                                                      DefiningClassLoader classLoader) {
		Expression result = let(constructor(resultClass));
		List<Expression> expressions = new ArrayList<>();
		expressions.add(result);
		for (String fieldName : concat(keys, fields)) {
			expressions.add(set(
					field(result, fieldName),
					field(cast(arg(0), recordClass), fieldName)));
		}
		expressions.add(result);
		Expression applyDef = sequence(expressions);
		ClassBuilder<StreamMap.MapperProjection> builder =
				ClassBuilder.create(classLoader, StreamMap.MapperProjection.class)
						.withMethod("apply", applyDef);
		return builder.buildClassAndCreateNewInstance();
	}

	public static Function createKeyFunction(Class<?> recordClass, Class<?> keyClass,
	                                         List<String> keys,
	                                         DefiningClassLoader classLoader) {
		logger.trace("Creating key function for keys {}", keys);
		Expression key = let(constructor(keyClass));
		List<Expression> expressions = new ArrayList<>();
		expressions.add(key);
		for (String keyString : keys) {
			expressions.add(set(
					field(key, keyString),
					field(cast(arg(0), recordClass), keyString)));
		}
		expressions.add(key);
		Expression applyDef = sequence(expressions);
		ClassBuilder factory = ClassBuilder.create(classLoader, Function.class)
				.withMethod("apply", applyDef);
		return (Function) factory.buildClassAndCreateNewInstance();
	}

	public static Class<?> createRecordClass(Aggregation structure,
	                                         List<String> keys, List<String> fields,
	                                         DefiningClassLoader classLoader) {
		return createRecordClass(projectKeys(structure.getKeyTypes(), keys), projectFields(structure.getFieldTypes(), fields), classLoader);
	}

	public static Class<?> createRecordClass(Map<String, FieldType> keys, Map<String, FieldType> fields,
	                                         DefiningClassLoader classLoader) {
		logger.trace("Creating record class for keys {}, fields {}", keys, fields);
		ClassBuilder<Object> builder = ClassBuilder.create(classLoader, Object.class);
		for (String key : keys.keySet()) {
			builder = builder.withField(key, keys.get(key).getInternalDataType());
		}
		for (String field : fields.keySet()) {
			builder = builder.withField(field, fields.get(field).getInternalDataType());
		}
		return builder
				.withMethod("toString", asString(newArrayList(concat(keys.keySet(), fields.keySet()))))
				.build();
	}

	public static <T> BufferSerializer<T> createBufferSerializer(Aggregation aggregation, Class<T> recordClass,
	                                                             List<String> keys, List<String> fields,
	                                                             DefiningClassLoader classLoader) {
		return createBufferSerializer(recordClass, projectKeys(aggregation.getKeyTypes(), keys), projectFields(aggregation.getFieldTypes(), fields), classLoader);
	}

	private static <T> BufferSerializer<T> createBufferSerializer(Class<T> recordClass,
	                                                              Map<String, FieldType> keys, Map<String, FieldType> fields,
	                                                              DefiningClassLoader classLoader) {
		SerializerGenClass serializerGenClass = new SerializerGenClass(recordClass);
		for (String key : keys.keySet()) {
			FieldType keyType = keys.get(key);
			try {
				Field recordClassKey = recordClass.getField(key);
				serializerGenClass.addField(recordClassKey, keyType.getSerializer(), -1, -1);
			} catch (NoSuchFieldException e) {
				throw propagate(e);
			}
		}
		for (String field : fields.keySet()) {
			try {
				Field recordClassField = recordClass.getField(field);
				serializerGenClass.addField(recordClassField, fields.get(field).getSerializer(), -1, -1);
			} catch (NoSuchFieldException e) {
				throw propagate(e);
			}
		}
		return SerializerBuilder.create(classLoader).build(serializerGenClass);
	}

	public static StreamReducers.Reducer aggregationReducer(Aggregation aggregation, Class<?> inputClass, Class<?> outputClass,
	                                                        List<String> keys, List<String> fields,
	                                                        DefiningClassLoader classLoader) {

		Expression accumulator = let(constructor(outputClass));
		List<Expression> onFirstItem = new ArrayList<>();
		List<Expression> onNextItem = new ArrayList<>();

		for (String key : keys) {
			onFirstItem.add(set(
					field(accumulator, key),
					field(cast(arg(2), inputClass), key)));
		}

		for (String field : fields) {
			Measure aggregateFunction = aggregation.getFieldAggregateFunction(field);
			onFirstItem.add(aggregateFunction.initAccumulatorWithAccumulator(
					field(accumulator, field),
					field(cast(arg(2), inputClass), field)
			));
			onNextItem.add(aggregateFunction.reduce(
					field(cast(arg(3), outputClass), field),
					field(cast(arg(2), inputClass), field)
			));
		}

		onFirstItem.add(accumulator);
		ClassBuilder<StreamReducers.Reducer> builder = ClassBuilder.create(classLoader, StreamReducers.Reducer.class)
				.withMethod("onFirstItem", sequence(onFirstItem));

		onNextItem.add(arg(3));
		return builder
				.withMethod("onNextItem", sequence(onNextItem))
				.withMethod("onComplete", call(arg(0), "onData", arg(2)))
				.buildClassAndCreateNewInstance();
	}

	public static Aggregate createPreaggregator(Aggregation aggregation, Class<?> inputClass, Class<?> outputClass,
	                                            List<String> keys, List<String> fields,
	                                            Map<String, String> outputToInputFields,
	                                            DefiningClassLoader classLoader) {

		Expression accumulator = let(constructor(outputClass));
		List<Expression> createAccumulatorDefExpressions = new ArrayList<>();
		createAccumulatorDefExpressions.add(accumulator);
		List<Expression> accumulateDefExpressions = new ArrayList<>();

		for (String key : keys) {
			createAccumulatorDefExpressions.add(set(
					field(accumulator, key),
					field(cast(arg(0), inputClass), key)));
		}

		for (String outputField : fields) {
			String inputField = outputToInputFields != null && outputToInputFields.containsKey(outputField) ?
					outputToInputFields.get(outputField) : outputField;
			createAggregateExpressions(aggregation, accumulator, inputField, inputClass,
					outputField, outputClass,
					createAccumulatorDefExpressions, accumulateDefExpressions);
		}

		createAccumulatorDefExpressions.add(accumulator);

		return ClassBuilder.create(classLoader, Aggregate.class)
				.withMethod("createAccumulator", sequence(createAccumulatorDefExpressions))
				.withMethod("accumulate", sequence(accumulateDefExpressions))
				.buildClassAndCreateNewInstance();
	}

	private static void createAggregateExpressions(Aggregation structure, Expression accumulator, String inputField, Class<?> inputClass,
	                                               String outputField, Class<?> outputClass,
	                                               List<Expression> createAccumulatorDefExpressions,
	                                               List<Expression> accumulateDefExpressions) {
		Measure aggregateFunction = structure.getFieldAggregateFunction(outputField);

		createAccumulatorDefExpressions.add(aggregateFunction.initAccumulatorWithValue(
				field(accumulator, outputField),
				inputField == null ? null : field(cast(arg(0), inputClass), inputField)
		));
		accumulateDefExpressions.add(aggregateFunction.accumulate(
				field(cast(arg(0), outputClass), outputField),
				inputField == null ? null : field(cast(arg(1), inputClass), inputField)
		));
	}

	public static BiPredicate createPartitionPredicate(Class recordClass, List<String> partitioningKey,
	                                                   DefiningClassLoader classLoader) {
		if (partitioningKey.isEmpty())
			return Predicates.alwaysTrue();

		PredicateDefAnd predicate = and();
		for (String keyComponent : partitioningKey) {
			predicate.add(cmpEq(
					field(cast(arg(0), recordClass), keyComponent),
					field(cast(arg(1), recordClass), keyComponent)));
		}

		return ClassBuilder.create(classLoader, BiPredicate.class)
				.withMethod("test", predicate)
				.buildClassAndCreateNewInstance();
	}
}
