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

package io.datakernel.aggregation;

import io.datakernel.aggregation.annotation.Key;
import io.datakernel.aggregation.annotation.Measures;
import io.datakernel.aggregation.fieldtype.FieldType;
import io.datakernel.aggregation.measure.Measure;
import io.datakernel.aggregation.ot.AggregationStructure;
import io.datakernel.aggregation.util.PartitionPredicate;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.codegen.ClassBuilder;
import io.datakernel.codegen.DefiningClassLoader;
import io.datakernel.codegen.PredicateDefAnd;
import io.datakernel.serializer.BinarySerializer;
import io.datakernel.serializer.SerializerBuilder;
import io.datakernel.serializer.asm.SerializerGenClass;
import io.datakernel.stream.processor.StreamReducers.Reducer;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Stream;

import static io.datakernel.codec.StructuredCodecs.ofTupleArray;
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

	public static <R> Comparator<R> createKeyComparator(Class<R> recordClass, List<String> keys, DefiningClassLoader classLoader) {
		return ClassBuilder.create(classLoader, Comparator.class)
				.withMethod("compare", compare(recordClass, keys))
				.buildClassAndCreateNewInstance();
	}

	public static <T, R> Function<T, R> createMapper(Class<T> recordClass, Class<R> resultClass,
			List<String> keys, List<String> fields,
			DefiningClassLoader classLoader) {
		return ClassBuilder.create(classLoader, Function.class)
				.withMethod("apply",
						let(constructor(resultClass), result ->
								sequenceOf(expressions -> {
									for (String fieldName : (Iterable<String>) Stream.concat(keys.stream(), fields.stream())::iterator) {
										expressions.add(set(
												property(result, fieldName),
												property(cast(arg(0), recordClass), fieldName)));
									}
									expressions.add(result);
								})))
				.buildClassAndCreateNewInstance();
	}

	public static <K extends Comparable, R> Function<R, K> createKeyFunction(Class<R> recordClass, Class<K> keyClass,
			List<String> keys,
			DefiningClassLoader classLoader) {
		return ClassBuilder.create(classLoader, Function.class)
				.withMethod("apply",
						let(constructor(keyClass), key ->
								sequenceOf(expressions -> {
									for (String keyString : keys) {
										expressions.add(
												set(
														property(key, keyString),
														property(cast(arg(0), recordClass), keyString)));
									}
									expressions.add(key);
								})))
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

	public static <T> BinarySerializer<T> createBinarySerializer(AggregationStructure aggregation, Class<T> recordClass,
			List<String> keys, List<String> fields,
			DefiningClassLoader classLoader) {
		return createBinarySerializer(recordClass,
				keysToMap(keys.stream(), aggregation.getKeyTypes()::get),
				keysToMap(fields.stream(), aggregation.getMeasureTypes()::get),
				classLoader);
	}

	private static <T> BinarySerializer<T> createBinarySerializer(Class<T> recordClass,
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

	public static <K extends Comparable, I, O, A> Reducer<K, I, O, A> aggregationReducer(AggregationStructure aggregation, Class<I> inputClass, Class<O> outputClass,
			List<String> keys, List<String> fields,
			DefiningClassLoader classLoader) {

		return ClassBuilder.create(classLoader, Reducer.class)
				.withMethod("onFirstItem",
						let(constructor(outputClass), accumulator ->
								sequenceOf(expressions -> {
									for (String key : keys) {
										expressions.add(
												set(
														property(accumulator, key),
														property(cast(arg(2), inputClass), key)
												));
									}
									for (String field : fields) {
										expressions.add(
												aggregation.getMeasure(field)
														.initAccumulatorWithAccumulator(
																property(accumulator, field),
																property(cast(arg(2), inputClass), field)
														));
									}
									expressions.add(accumulator);
								})))
				.withMethod("onNextItem",
						sequenceOf(expressions -> {
							for (String field : fields) {
								expressions.add(
										aggregation.getMeasure(field)
												.reduce(
														property(cast(arg(3), outputClass), field),
														property(cast(arg(2), inputClass), field)
												));
							}
							expressions.add(arg(3));
						}))
				.withMethod("onComplete", call(arg(0), "accept", arg(2)))
				.buildClassAndCreateNewInstance();
	}

	public static <I, O> Aggregate<O, Object> createPreaggregator(AggregationStructure aggregation, Class<I> inputClass, Class<O> outputClass,
			Map<String, String> keyFields, Map<String, String> measureFields,
			DefiningClassLoader classLoader) {

		return ClassBuilder.create(classLoader, Aggregate.class)
				.withMethod("createAccumulator",
						let(constructor(outputClass), accumulator ->
								sequenceOf(expressions -> {
									for (String key : keyFields.keySet()) {
										String inputField = keyFields.get(key);
										expressions.add(set(
												property(accumulator, key),
												property(cast(arg(0), inputClass), inputField)));
									}
									for (String measure : measureFields.keySet()) {
										String inputFields = measureFields.get(measure);
										Measure aggregateFunction = aggregation.getMeasure(measure);

										expressions.add(aggregateFunction.initAccumulatorWithValue(
												property(accumulator, measure),
												inputFields == null ? null : property(cast(arg(0), inputClass), inputFields)));
									}
									expressions.add(accumulator);
								})))
				.withMethod("accumulate",
						sequenceOf(expressions -> {
							for (String measure : measureFields.keySet()) {
								String inputFields = measureFields.get(measure);
								Measure aggregateFunction = aggregation.getMeasure(measure);

								expressions.add(aggregateFunction.accumulate(
										property(cast(arg(0), outputClass), measure),
										inputFields == null ? null : property(cast(arg(1), inputClass), inputFields)));
							}
						}))
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
					property(cast(arg(0), recordClass), keyComponent),
					property(cast(arg(1), recordClass), keyComponent)));
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

	public static StructuredCodec<PrimaryKey> getPrimaryKeyCodec(AggregationStructure aggregation) {
		StructuredCodec<?>[] keyCodec = new StructuredCodec<?>[aggregation.getKeys().size()];
		for (int i = 0; i < aggregation.getKeys().size(); i++) {
			String key = aggregation.getKeys().get(i);
			FieldType keyType = aggregation.getKeyTypes().get(key);
			keyCodec[i] = keyType.getInternalCodec();
		}
		return ofTupleArray(keyCodec)
				.transform(PrimaryKey::ofArray, PrimaryKey::getArray);
	}

}
