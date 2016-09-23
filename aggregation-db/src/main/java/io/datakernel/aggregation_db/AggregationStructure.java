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

package io.datakernel.aggregation_db;

import com.google.common.base.Function;
import com.google.common.base.MoreObjects;
import io.datakernel.aggregation_db.fieldtype.FieldType;
import io.datakernel.aggregation_db.keytype.KeyType;
import io.datakernel.codegen.ClassBuilder;
import io.datakernel.codegen.DefiningClassLoader;
import io.datakernel.codegen.Expression;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.serializer.SerializerBuilder;
import io.datakernel.serializer.asm.SerializerGenClass;
import io.datakernel.stream.processor.StreamMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.*;

import static com.google.common.base.Throwables.propagate;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Lists.newArrayList;
import static io.datakernel.codegen.Expressions.*;

/**
 * Defines a structure of an aggregation.
 * It is defined by keys, fields and their types.
 * Contains methods for defining dynamic classes, that are used for different operations.
 * Provides serializer for records that have the defined structure.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class AggregationStructure {
	private final static Logger logger = LoggerFactory.getLogger(AggregationStructure.class);

	private final Map<String, KeyType> keys;
	private final Map<String, FieldType> fields;

	/**
	 * Constructs a new aggregation structure with the given class loader, keys and fields.
	 *
	 * @param keys   map of aggregation keys (key is key name)
	 * @param fields map of aggregation fields (key is field name)
	 */
	private AggregationStructure(Map<String, KeyType> keys, Map<String, FieldType> fields) {
		this.keys = new LinkedHashMap<>(keys);
		this.fields = new LinkedHashMap<>(fields);
	}

	public static AggregationStructure create(Map<String, KeyType> keys, Map<String, FieldType> fields) {
		return new AggregationStructure(keys, fields);
	}

	public Map<String, KeyType> getKeys() {
		return keys;
	}

	public KeyType getKeyType(String key) {
		return keys.get(key);
	}

	public boolean containsKey(String key) {
		return keys.containsKey(key);
	}

	public Map<String, FieldType> getFields() {
		return fields;
	}

	public FieldType getFieldType(String field) {
		return fields.get(field);
	}

	public boolean containsField(String field) {
		return fields.containsKey(field);
	}

	public Class<?> createKeyClass(List<String> keys, DefiningClassLoader classLoader) {
		logger.trace("Creating key class for keys {}", keys);
		ClassBuilder builder = ClassBuilder.create(classLoader, Comparable.class);
		for (String key : keys) {
			KeyType d = this.keys.get(key);
			builder = builder.withField(key, d.getDataType());
		}
		builder = builder
				.withMethod("compareTo", compareTo(keys))
				.withMethod("equals", asEquals(keys))
				.withMethod("hashCode", hashCodeOfThis(keys))
				.withMethod("toString", asString(keys));

		return builder.build();
	}

	public static Comparator createKeyComparator(Class<?> recordClass, List<String> keys, DefiningClassLoader classLoader) {
		Expression comparator = compare(recordClass, keys);
		ClassBuilder<Comparator> builder = ClassBuilder.create(classLoader, Comparator.class)
				.withMethod("compare", comparator);
		return builder.buildClassAndCreateNewInstance();
	}

	public static StreamMap.MapperProjection createMapper(Class<?> recordClass, Class<?> resultClass, List<String> keys,
	                                                      List<String> fields, DefiningClassLoader classLoader) {
		Expression result = let(constructor(resultClass));
		List<Expression> expressions = new ArrayList<>();
		expressions.add(result);
		for (String fieldName : concat(keys, fields)) {
			expressions.add(set(
					getter(result, fieldName),
					getter(cast(arg(0), recordClass), fieldName)));
		}
		expressions.add(result);
		Expression applyDef = sequence(expressions);
		ClassBuilder<StreamMap.MapperProjection> builder =
				ClassBuilder.create(classLoader, StreamMap.MapperProjection.class)
						.withMethod("apply", applyDef);
		return builder.buildClassAndCreateNewInstance();
	}

	public static Function createKeyFunction(Class<?> recordClass, Class<?> keyClass, List<String> keys,
	                                         DefiningClassLoader classLoader) {
		logger.trace("Creating key function for keys {}", keys);
		Expression key = let(constructor(keyClass));
		List<Expression> expressions = new ArrayList<>();
		expressions.add(key);
		for (String keyString : keys) {
			expressions.add(set(
					getter(key, keyString),
					getter(cast(arg(0), recordClass), keyString)));
		}
		expressions.add(key);
		Expression applyDef = sequence(expressions);
		ClassBuilder factory = ClassBuilder.create(classLoader, Function.class)
				.withMethod("apply", applyDef);
		return (Function) factory.buildClassAndCreateNewInstance();
	}

	public Class<?> createRecordClass(List<String> keys, List<String> fields, DefiningClassLoader classLoader) {
		logger.trace("Creating record class for keys {}, fields {}", keys, fields);
		ClassBuilder<Object> builder = ClassBuilder.create(classLoader, Object.class);
		for (String key : keys) {
			KeyType d = this.keys.get(key);
			builder = builder.withField(key, d.getDataType());
		}
		for (String field : fields) {
			FieldType m = this.fields.get(field);
			builder = builder.withField(field, m.getDataType());
		}
		return builder
				.withMethod("toString", asString(newArrayList(concat(keys, fields))))
				.build();
	}

	public Class<?> createResultClass(AggregationQuery query, DefiningClassLoader classLoader) {
		logger.trace("Creating result class for query {}", query.toString());
		ClassBuilder<Object> builder = ClassBuilder.create(classLoader, Object.class);
		List<String> resultKeys = query.getResultKeys();
		List<String> resultFields = query.getResultFields();
		for (String key : resultKeys) {
			KeyType keyType = this.keys.get(key);
			if (keyType == null) {
				throw new AggregationException("Key with the name '" + key + "' not found.");
			}
			builder = builder.withField(key, keyType.getDataType());
		}
		for (String field : resultFields) {
			FieldType fieldType = this.fields.get(field);
			if (fieldType == null) {
				throw new AggregationException("Field with the name '" + field + "' not found.");
			}
			builder = builder.withField(field, fieldType.getDataType());
		}
		return builder
				.withMethod("toString", asString(newArrayList(concat(resultKeys, resultFields))))
				.build();
	}

	public <T> BufferSerializer<T> createBufferSerializer(Class<T> recordClass, List<String> keys, List<String> fields,
	                                                      DefiningClassLoader classLoader) {
		SerializerGenClass serializerGenClass = new SerializerGenClass(recordClass);
		for (String key : keys) {
			KeyType keyType = this.keys.get(key);
			try {
				Field recordClassKey = recordClass.getField(key);
				serializerGenClass.addField(recordClassKey, keyType.serializerGen(), -1, -1);
			} catch (NoSuchFieldException e) {
				throw propagate(e);
			}
		}
		for (String field : fields) {
			FieldType fieldType = this.fields.get(field);
			try {
				Field recordClassField = recordClass.getField(field);
				serializerGenClass.addField(recordClassField, fieldType.serializerGen(), -1, -1);
			} catch (NoSuchFieldException e) {
				throw propagate(e);
			}
		}
		return SerializerBuilder.create(classLoader).build(serializerGenClass);
	}

	@Override
	public String toString() {
		return MoreObjects.toStringHelper(this)
				.add("keys", keys)
				.add("fields", fields)
				.toString();
	}
}
