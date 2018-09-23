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
import io.datakernel.util.gson.GsonAdapters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Stream;

import static io.datakernel.codegen.Expressions.*;
import static io.datakernel.util.CollectionUtils.concat;
import static io.datakernel.util.CollectionUtils.keysToMap;
import static io.datakernel.util.Preconditions.checkArgument;
import static io.datakernel.util.ReflectionUtils.extractFieldNameFromGetter;

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

	public static <K extends Comparable> Class<K> createKeyClass(Map<String, FieldType> keys, DefiningClassLoader classLoader) {
		List<String> keyList = new ArrayList<>(keys.keySet());
		return ClassBuilder.<K>create(classLoader, Comparable.class)
				.initialize(cb ->
						keys.forEach((key, value) ->
								cb.withField(key, value.getInternalDataType())))
				.withMethod("compareTo", compareTo(keyList))
				.withMethod("equals", asEquals(keyList))
				.withMethod("hashCode", hashCodeOfThis(keyList))
				.withMethod("toString", asString(keyList))
				.build();
	}

	public static <K extends Comparable, R> Comparator<R> createKeyComparator(Class<R> recordClass, List<String> keys, DefiningClassLoader classLoader) {
		return ClassBuilder.create(classLoader, Comparator.class)
				.withMethod("compare", compare(recordClass, keys))
				.buildClassAndCreateNewInstance();
	}

	public static <T, R> StreamMap.MapperProjection<T, R> createMapper(Class<T> recordClass, Class<R> resultClass,
			List<String> keys, List<String> fields,
			DefiningClassLoader classLoader) {
		return ClassBuilder.create(classLoader, StreamMap.MapperProjection.class)
				.withMethod("apply", () -> {
					Expression result1 = let(constructor(resultClass));
					ExpressionSequence sequence = ExpressionSequence.create();
					for (String fieldName : (Iterable<String>) Stream.concat(keys.stream(), fields.stream())::iterator) {
						sequence.add(set(
								field(result1, fieldName),
								field(cast(arg(0), recordClass), fieldName)));
					}
					return sequence.add(result1);
				})
				.buildClassAndCreateNewInstance();
	}

	public static <K extends Comparable, R> Function<R, K> createKeyFunction(Class<R> recordClass, Class<K> keyClass,
			List<String> keys,
			DefiningClassLoader classLoader) {
		return ClassBuilder.create(classLoader, Function.class)
				.withMethod("apply", () -> {
					Expression key = let(constructor(keyClass));
					ExpressionSequence sequence = ExpressionSequence.create();
					for (String keyString : keys) {
						sequence.add(set(
								field(key, keyString),
								field(cast(arg(0), recordClass), keyString)));
					}
					return sequence.add(key);
				})
				.buildClassAndCreateNewInstance();
	}

	public static <T> Class<T> createRecordClass(AggregationStructure aggregation,
			Collection<String> keys, Collection<String> fields,
			DefiningClassLoader classLoader) {
		return createRecordClass(
				keysToMap(keys.stream(), aggregation.getKeyTypes()::get),
				keysToMap(fields.stream(), aggregation.getMeasureTypes()::get),
				classLoader);
	}

	public static <T> Class<T> createRecordClass(Map<String, FieldType> keys, Map<String, FieldType> fields,
			DefiningClassLoader classLoader) {
		return ClassBuilder.create(classLoader, (Class<T>) Object.class)
				.initialize(cb ->
						keys.forEach((key, value) ->
								cb.withField(key, value.getInternalDataType())))
				.initialize(cb ->
						fields.forEach((key, value) ->
								cb.withField(key, value.getInternalDataType())))
				.withMethod("toString", asString(concat(keys.keySet(), fields.keySet())))
				.build();
	}

	public static <T> BufferSerializer<T> createBufferSerializer(AggregationStructure aggregation, Class<T> recordClass,
			List<String> keys, List<String> fields,
			DefiningClassLoader classLoader) {
		return createBufferSerializer(recordClass,
				keysToMap(keys.stream(), aggregation.getKeyTypes()::get),
				keysToMap(fields.stream(), aggregation.getMeasureTypes()::get),
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

	public static <K extends Comparable, I, O, A> StreamReducers.Reducer<K, I, O, A> aggregationReducer(AggregationStructure aggregation, Class<I> inputClass, Class<O> outputClass,
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
				.withMethod("onComplete", call(arg(0), "accept", arg(2)))
				.buildClassAndCreateNewInstance();
	}

	public static <I, O> Aggregate<O, Object> createPreaggregator(AggregationStructure aggregation, Class<I> inputClass, Class<O> outputClass,
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
						measureFields.put(measure.equals("") ? field.getName() : measure, field.getName());
					}
				}
			}
		}
		for (Method method : inputClass.getMethods()) {
			for (Annotation annotation : method.getAnnotations()) {
				if (annotation.annotationType() == Measures.class) {
					for (String measure : ((Measures) annotation).value()) {
						measureFields.put(measure.equals("") ? extractFieldNameFromGetter(method) : measure, method.getName());
					}
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

}
