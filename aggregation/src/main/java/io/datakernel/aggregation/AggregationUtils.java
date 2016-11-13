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
import io.datakernel.codegen.*;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.serializer.SerializerBuilder;
import io.datakernel.serializer.asm.SerializerGenClass;
import io.datakernel.stream.processor.StreamMap;
import io.datakernel.stream.processor.StreamReducers;
import io.datakernel.util.WithValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.*;

import static com.google.common.base.Functions.forMap;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.propagate;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.toMap;
import static com.google.common.collect.Maps.transformValues;
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

	public static Map<String, Measure> projectMeasures(Map<String, Measure> measures, List<String> fields) {
		return projectMap(measures, fields);
	}

	public static Map<String, FieldType> measuresAsFields(Map<String, Measure> measures) {
		return transformValues(measures, new Function<Measure, FieldType>() {
			@Override
			public FieldType apply(Measure input) {
				return input.getFieldType();
			}
		});
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
		List<String> keyList = new ArrayList<>(keys.keySet());
		return ClassBuilder.create(classLoader, Comparable.class)
				.withFields(transformValues(keys, new Function<FieldType, Class<?>>() {
					@Override
					public Class<?> apply(FieldType field) {
						return field.getInternalDataType();
					}
				}))
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
				.withMethod("apply", new WithValue<Expression>() {
					@Override
					public Expression get() {
						Expression result1 = let(constructor(resultClass));
						ExpressionSequence sequence = ExpressionSequence.create();
						for (String fieldName : concat(keys, fields)) {
							sequence.add(set(
									field(result1, fieldName),
									field(cast(arg(0), recordClass), fieldName)));
						}
						return sequence.add(result1);
					}
				}.get())
				.buildClassAndCreateNewInstance();
	}

	public static Function createKeyFunction(final Class<?> recordClass, final Class<?> keyClass,
	                                         final List<String> keys,
	                                         DefiningClassLoader classLoader) {
		return ClassBuilder.create(classLoader, Function.class)
				.withMethod("apply", new WithValue<Expression>() {
					@Override
					public Expression get() {
						Expression key = let(constructor(keyClass));
						ExpressionSequence sequence = ExpressionSequence.create();
						for (String keyString : keys) {
							sequence.add(set(
									field(key, keyString),
									field(cast(arg(0), recordClass), keyString)));
						}
						return sequence.add(key);
					}
				}.get())
				.buildClassAndCreateNewInstance();
	}

	public static Class<?> createRecordClass(Aggregation structure,
	                                         List<String> keys, List<String> fields,
	                                         DefiningClassLoader classLoader) {
		return createRecordClass(
				projectKeys(structure.getKeyTypes(), keys),
				projectFields(structure.getMeasureTypes(), fields),
				classLoader);
	}

	public static Class<?> createRecordClass(Map<String, FieldType> keys, Map<String, FieldType> fields,
	                                         DefiningClassLoader classLoader) {
		return ClassBuilder.create(classLoader, Object.class)
				.withFields(transformValues(keys, new Function<FieldType, Class<?>>() {
					@Override
					public Class<?> apply(FieldType fieldType) {
						return fieldType.getInternalDataType();
					}
				}))
				.withFields(transformValues(fields, new Function<FieldType, Class<?>>() {
					@Override
					public Class<?> apply(FieldType fieldType) {
						return fieldType.getInternalDataType();
					}
				}))
				.withMethod("toString", asString(newArrayList(concat(keys.keySet(), fields.keySet()))))
				.build();
	}

	public static <T> BufferSerializer<T> createBufferSerializer(Aggregation aggregation, Class<T> recordClass,
	                                                             List<String> keys, List<String> fields,
	                                                             DefiningClassLoader classLoader) {
		return createBufferSerializer(recordClass,
				toMap(keys, forMap(aggregation.getKeyTypes())),
				toMap(fields, forMap(aggregation.getMeasureTypes())),
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

	public static Aggregate createPreaggregator(Aggregation aggregation, Class<?> inputClass, Class<?> outputClass,
	                                            List<String> keys, List<String> fields,
	                                            Map<String, String> outputToInputFields,
	                                            DefiningClassLoader classLoader) {

		Expression accumulator = let(constructor(outputClass));
		ExpressionSequence createAccumulatorDefExpressions = ExpressionSequence.create();
		ExpressionSequence accumulateDefExpressions = ExpressionSequence.create();

		for (String key : keys) {
			createAccumulatorDefExpressions.add(set(
					field(accumulator, key),
					field(cast(arg(0), inputClass), key)));
		}

		for (String outputField : fields) {
			String inputField = outputToInputFields != null && outputToInputFields.containsKey(outputField) ?
					outputToInputFields.get(outputField) : outputField;
			Measure aggregateFunction = aggregation.getMeasure(outputField);

			createAccumulatorDefExpressions.add(aggregateFunction.initAccumulatorWithValue(
					field(accumulator, outputField),
					inputField == null ? null : field(cast(arg(0), inputClass), inputField)));
			accumulateDefExpressions.add(aggregateFunction.accumulate(
					field(cast(arg(0), outputClass), outputField),
					inputField == null ? null : field(cast(arg(1), inputClass), inputField)));
		}

		createAccumulatorDefExpressions.add(accumulator);

		return ClassBuilder.create(classLoader, Aggregate.class)
				.withMethod("createAccumulator", sequence(createAccumulatorDefExpressions))
				.withMethod("accumulate", sequence(accumulateDefExpressions))
				.buildClassAndCreateNewInstance();
	}

	public static BiPredicate createPartitionPredicate(Class recordClass, List<String> partitioningKey,
	                                                   DefiningClassLoader classLoader) {
		if (partitioningKey.isEmpty())
			return Predicates.alwaysTrue();

		PredicateDefAnd predicate = PredicateDefAnd.create();
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
