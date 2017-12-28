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

import com.google.gson.TypeAdapter;
import io.datakernel.aggregation.annotation.Key;
import io.datakernel.aggregation.annotation.Measures;
import io.datakernel.aggregation.fieldtype.FieldType;
import io.datakernel.aggregation.measure.Measure;
import io.datakernel.aggregation.ot.AggregationStructure;
import io.datakernel.aggregation.util.PartitionPredicate;
import io.datakernel.codegen.*;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.serializer.SerializerBuilder;
import io.datakernel.serializer.asm.SerializerGenClass;
import io.datakernel.stream.processor.StreamMap;
import io.datakernel.stream.processor.StreamReducers;
import io.datakernel.util.WithValue;
import io.datakernel.utils.GsonAdapters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.*;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.Stream;

import static io.datakernel.codegen.Expressions.*;
import static io.datakernel.util.Preconditions.checkArgument;

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

	public static Map<String, FieldType> projectKeys(Map<String, FieldType> keyTypes, Collection<String> keys) {
		return projectMap(keyTypes, keys);
	}

	public static Map<String, FieldType> projectFields(Map<String, FieldType> fieldTypes, Collection<String> fields) {
		return projectMap(fieldTypes, fields);
	}

	public static Map<String, Measure> projectMeasures(Map<String, Measure> measures, Collection<String> fields) {
		return projectMap(measures, fields);
	}

	public static Map<String, FieldType> measuresAsFields(Map<String, Measure> measures) {
		return transformValuesToLinkedMap(measures.entrySet().stream(), Measure::getFieldType);
	}

	private static <K, V> Map<K, V> projectMap(Map<K, V> map, Collection<K> keys) {
		keys = new HashSet<>(keys);
		checkArgument(map.keySet().containsAll(keys), "Unknown fields: " + difference(new LinkedHashSet<>(keys), map.keySet()));
		LinkedHashMap<K, V> result = new LinkedHashMap<>();
		for (Entry<K, V> entry : map.entrySet()) {
			if (keys.contains(entry.getKey())) {
				result.put(entry.getKey(), entry.getValue());
			}
		}
		return result;
	}

	private static <T> Set<T> difference(Set<T> a, Set<T> b) {
		Set<T> set = new HashSet<>(a);
		set.removeAll(b);
		return set;
	}

	public static Class<?> createKeyClass(AggregationStructure aggregation, List<String> keys, DefiningClassLoader classLoader) {
		return createKeyClass(projectKeys(aggregation.getKeyTypes(), keys), classLoader);
	}

	public static Class<?> createKeyClass(Map<String, FieldType> keys, DefiningClassLoader classLoader) {
		List<String> keyList = new ArrayList<>(keys.keySet());
		return ClassBuilder.create(classLoader, Comparable.class)
				.withFields(transformValuesToLinkedMap(keys.entrySet().stream(), FieldType::getInternalDataType))
				.withMethod("compareTo", compareTo(keyList))
				.withMethod("equals", asEquals(keyList))
				.withMethod("hashCode", hashCodeOfThis(keyList))
				.withMethod("toString", asString(keyList)).build();
	}

	public static Comparator createKeyComparator(Class<?> recordClass, List<String> keys, DefiningClassLoader classLoader) {
		return ClassBuilder.create(classLoader, Comparator.class)
				.withMethod("compare", compare(recordClass, keys))
				.buildClassAndCreateNewInstance();
	}

	public static StreamMap.MapperProjection createMapper(final Class<?> recordClass, final Class<?> resultClass,
	                                                      final List<String> keys, final List<String> fields,
	                                                      DefiningClassLoader classLoader) {
		return ClassBuilder.create(classLoader, StreamMap.MapperProjection.class)
				.withMethod("apply", ((WithValue<Expression>) () -> {
					Expression result1 = let(constructor(resultClass));
					ExpressionSequence sequence = ExpressionSequence.create();
					for (String fieldName : (Iterable<String>) Stream.concat(keys.stream(), fields.stream())::iterator) {
						sequence.add(set(
								field(result1, fieldName),
								field(cast(arg(0), recordClass), fieldName)));
					}
					return sequence.add(result1);
				}).get())
				.buildClassAndCreateNewInstance();
	}

	public static Function createKeyFunction(final Class<?> recordClass, final Class<?> keyClass,
	                                         final List<String> keys,
	                                         DefiningClassLoader classLoader) {
		return ClassBuilder.create(classLoader, Function.class)
				.withMethod("apply", ((WithValue<Expression>) () -> {
					Expression key = let(constructor(keyClass));
					ExpressionSequence sequence = ExpressionSequence.create();
					for (String keyString : keys) {
						sequence.add(set(
								field(key, keyString),
								field(cast(arg(0), recordClass), keyString)));
					}
					return sequence.add(key);
				}).get())
				.buildClassAndCreateNewInstance();
	}

	public static Class<?> createRecordClass(AggregationStructure aggregation,
	                                         Collection<String> keys, Collection<String> fields,
	                                         DefiningClassLoader classLoader) {
		return createRecordClass(
				projectKeys(aggregation.getKeyTypes(), keys),
				projectFields(aggregation.getMeasureTypes(), fields),
				classLoader);
	}

	public static Class<?> createRecordClass(Map<String, FieldType> keys, Map<String, FieldType> fields,
	                                         DefiningClassLoader classLoader) {
		final ArrayList list = new ArrayList<>();
		list.addAll(keys.keySet());
		list.addAll(fields.keySet());

		return ClassBuilder.create(classLoader, Object.class)
				.withFields(transformValuesToLinkedMap(keys.entrySet().stream(), FieldType::getInternalDataType))
				.withFields(transformValuesToLinkedMap(fields.entrySet().stream(), FieldType::getInternalDataType))
				.withMethod("toString", asString(list))
				.build();
	}

	public static <T> BufferSerializer<T> createBufferSerializer(AggregationStructure aggregation, Class<T> recordClass,
	                                                             List<String> keys, List<String> fields,
	                                                             DefiningClassLoader classLoader) {
		return createBufferSerializer(recordClass,
				streamToLinkedMap(keys.stream(), aggregation.getKeyTypes()::get),
				streamToLinkedMap(fields.stream(), aggregation.getMeasureTypes()::get),
				classLoader);
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
				throw new RuntimeException(e);
			}
		}
		for (String field : fields.keySet()) {
			try {
				Field recordClassField = recordClass.getField(field);
				serializerGenClass.addField(recordClassField, fields.get(field).getSerializer(), -1, -1);
			} catch (NoSuchFieldException e) {
				throw new RuntimeException(e);
			}
		}
		return SerializerBuilder.create(classLoader).build(serializerGenClass);
	}

	public static StreamReducers.Reducer aggregationReducer(AggregationStructure aggregation, Class<?> inputClass, Class<?> outputClass,
	                                                        List<String> keys, List<String> fields,
	                                                        DefiningClassLoader classLoader) {

		Expression accumulator = let(constructor(outputClass));
		ExpressionSequence onFirstItem = ExpressionSequence.create();
		ExpressionSequence onNextItem = ExpressionSequence.create();

		for (String key : keys) {
			onFirstItem.add(set(
					field(accumulator, key),
					field(cast(arg(2), inputClass), key)));
		}

		for (String field : fields) {
			Measure aggregateFunction = aggregation.getMeasure(field);
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
		onNextItem.add(arg(3));

		return ClassBuilder.create(classLoader, StreamReducers.Reducer.class)
				.withMethod("onFirstItem", onFirstItem)
				.withMethod("onNextItem", onNextItem)
				.withMethod("onComplete", call(arg(0), "onData", arg(2)))
				.buildClassAndCreateNewInstance();
	}

	public static Aggregate createPreaggregator(AggregationStructure aggregation, Class<?> inputClass, Class<?> outputClass,
	                                            Map<String, String> keyFields, Map<String, String> measureFields,
	                                            DefiningClassLoader classLoader) {

		Expression accumulator = let(constructor(outputClass));
		ExpressionSequence createAccumulator = ExpressionSequence.create();
		ExpressionSequence accumulate = ExpressionSequence.create();

		for (String key : keyFields.keySet()) {
			String inputField = keyFields.get(key);
			createAccumulator.add(set(
					field(accumulator, key),
					field(cast(arg(0), inputClass), inputField)));
		}

		for (String measure : measureFields.keySet()) {
			String inputFields = measureFields.get(measure);
			Measure aggregateFunction = aggregation.getMeasure(measure);

			createAccumulator.add(aggregateFunction.initAccumulatorWithValue(
					field(accumulator, measure),
					inputFields == null ? null : field(cast(arg(0), inputClass), inputFields)));
			accumulate.add(aggregateFunction.accumulate(
					field(cast(arg(0), outputClass), measure),
					inputFields == null ? null : field(cast(arg(1), inputClass), inputFields)));
		}

		createAccumulator.add(accumulator);

		return ClassBuilder.create(classLoader, Aggregate.class)
				.withMethod("createAccumulator", createAccumulator)
				.withMethod("accumulate", accumulate)
				.buildClassAndCreateNewInstance();
	}

	private static final PartitionPredicate SINGLE_PARTITION = (t, u) -> true;

	public static <T> PartitionPredicate<T> singlePartition() {
		return SINGLE_PARTITION;
	}

	public static PartitionPredicate createPartitionPredicate(Class recordClass, List<String> partitioningKey,
	                                                          DefiningClassLoader classLoader) {
		if (partitioningKey.isEmpty())
			return singlePartition();

		PredicateDefAnd predicate = PredicateDefAnd.create();
		for (String keyComponent : partitioningKey) {
			predicate.add(cmpEq(
					field(cast(arg(0), recordClass), keyComponent),
					field(cast(arg(1), recordClass), keyComponent)));
		}

		return ClassBuilder.create(classLoader, PartitionPredicate.class)
				.withMethod("isSamePartition", predicate)
				.buildClassAndCreateNewInstance();
	}

	public static <T> Map<String, String> scanKeyFields(Class<T> inputClass) {
		Map<String, String> keyFields = new LinkedHashMap<>();
		for (Field field : inputClass.getFields()) {
			for (Annotation annotation : field.getAnnotations()) {
				if (annotation.annotationType() == Key.class) {
					String value = ((Key) annotation).value();
					keyFields.put("".equals(value) ? field.getName() : value, field.getName());
				}
			}
		}
		for (Method method : inputClass.getMethods()) {
			for (Annotation annotation : method.getAnnotations()) {
				if (annotation.annotationType() == Key.class) {
					String value = ((Key) annotation).value();
					keyFields.put("".equals(value) ? method.getName() : value, method.getName());
				}
			}
		}
		checkArgument(!keyFields.isEmpty(), "Missing @Key annotations in %s", inputClass);
		return keyFields;
	}

	public static <T> Map<String, String> scanMeasureFields(Class<T> inputClass) {
		Map<String, String> measureFields = new LinkedHashMap<>();
		for (Annotation annotation : inputClass.getAnnotations()) {
			if (annotation.annotationType() == Measures.class) {
				for (String measure : ((Measures) annotation).value()) {
					measureFields.put(measure, null);
				}
			}
		}
		for (Field field : inputClass.getFields()) {
			for (Annotation annotation : field.getAnnotations()) {
				if (annotation.annotationType() == Measures.class) {
					for (String measure : ((Measures) annotation).value()) {
						measureFields.put(measure, field.getName());
					}
				}
			}
		}
		for (Field field : inputClass.getFields()) {
			for (Annotation annotation : field.getAnnotations()) {
				if (annotation.annotationType() == io.datakernel.aggregation.annotation.Measure.class) {
					String value = ((io.datakernel.aggregation.annotation.Measure) annotation).value();
					measureFields.put("".equals(value) ? field.getName() : value, field.getName());
				}
			}
		}
		for (Method method : inputClass.getMethods()) {
			for (Annotation annotation : method.getAnnotations()) {
				if (annotation.annotationType() == io.datakernel.aggregation.annotation.Measure.class) {
					String value = ((io.datakernel.aggregation.annotation.Measure) annotation).value();
					measureFields.put("".equals(value) ? method.getName() : value, method.getName());
				}
			}
		}
		checkArgument(!measureFields.isEmpty(), "Missing @Measure(s) annotations in %s", inputClass);
		return measureFields;
	}

	public static TypeAdapter<PrimaryKey> getPrimaryKeyJson(AggregationStructure aggregation) {
		TypeAdapter<?>[] keyTypeAdapters = new TypeAdapter<?>[aggregation.getKeys().size()];
		for (int i = 0; i < aggregation.getKeys().size(); i++) {
			String key = aggregation.getKeys().get(i);
			FieldType keyType = aggregation.getKeyTypes().get(key);
			keyTypeAdapters[i] = keyType.getInternalJson();
		}
		TypeAdapter<Object[]> typeAdapter = GsonAdapters.ofHeterogeneousArray(keyTypeAdapters);
		return GsonAdapters.transform(typeAdapter, PrimaryKey::ofArray, PrimaryKey::getArray);
	}

	public static <K, V> Map<K, V> streamToLinkedMap(Stream<K> collection, Function<K, V> function) {
		final LinkedHashMap<K, V> map = new LinkedHashMap<>();
		collection.forEach(k -> map.put(k, function.apply(k)));
		return map;
	}

	public static <K, V, T> Map<K, V> transformValuesToLinkedMap(Stream<Entry<K, T>> stream, Function<T, V> function) {
		final LinkedHashMap<K, V> map = new LinkedHashMap<>();
		stream.forEach(entry -> map.put(entry.getKey(), function.apply(entry.getValue())));
		return map;
	}

	public static <K, V> Map<K, V> valuesToLinkedMap(Stream<Entry<K, V>> stream) {
		final LinkedHashMap<K, V> map = new LinkedHashMap<>();
		stream.forEach(entry -> map.put(entry.getKey(), entry.getValue()));
		return map;
	}
}
