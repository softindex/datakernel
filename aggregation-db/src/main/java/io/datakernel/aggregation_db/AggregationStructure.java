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
import io.datakernel.codegen.AsmBuilder;
import io.datakernel.codegen.Expression;
import io.datakernel.codegen.ExpressionComparator;
import io.datakernel.codegen.ExpressionSequence;
import io.datakernel.codegen.utils.DefiningClassLoader;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.serializer.SerializerBuilder;
import io.datakernel.serializer.asm.SerializerGenClass;
import io.datakernel.stream.processor.StreamReducers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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

	private final DefiningClassLoader classLoader;
	private final Map<String, KeyType> keys;
	private final Map<String, FieldType> fields;

	/**
	 * Constructs a new aggregation structure with the given class loader, keys and fields.
	 *
	 * @param classLoader class loader for loading dynamically created classes
	 * @param keys        map of aggregation keys (key is key name)
	 * @param fields      map of aggregation fields (key is field name)
	 */
	public AggregationStructure(DefiningClassLoader classLoader,
	                            Map<String, KeyType> keys,
	                            Map<String, FieldType> fields) {
		this.classLoader = classLoader;
		this.keys = new LinkedHashMap<>(keys);
		this.fields = new LinkedHashMap<>(fields);
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

	public Class<?> createKeyClass(List<String> keys) {
		logger.trace("Creating key class for keys {}", keys);
		AsmBuilder builder = new AsmBuilder(classLoader, Comparable.class);
		for (String key : keys) {
			KeyType d = this.keys.get(key);
			builder.field(key, d.getDataType());
		}
		builder.method("compareTo", compareTo(keys));
		builder.method("equals", asEquals(keys));
		builder.method("hashCode", hashCodeOfThis(keys));
		builder.method("toString", asString(keys));

		return builder.defineClass();
	}

	public Comparator createKeyComparator(Class<?> recordClass, List<String> keys) {
		AsmBuilder<Comparator> builder = new AsmBuilder<>(classLoader, Comparator.class);
		ExpressionComparator comparator = comparator();

		for (String key : keys) {
			comparator.add(getter(cast(arg(0), recordClass), key), getter(cast(arg(1), recordClass), key));
		}

		builder.method("compare", comparator);

		return builder.newInstance();
	}

	public Function createKeyFunction(Class<?> recordClass, Class<?> keyClass, List<String> keys) {
		logger.trace("Creating key function for keys {}", keys);
		AsmBuilder factory = new AsmBuilder<>(classLoader, Function.class);
		Expression key = let(constructor(keyClass));
		ExpressionSequence applyDef = sequence(key);
		for (String keyString : keys) {
			applyDef.add(set(
					getter(key, keyString),
					getter(cast(arg(0), recordClass), keyString)));
		}
		applyDef.add(key);
		factory.method("apply", applyDef);
		return (Function) factory.newInstance();
	}

	public Class<?> createRecordClass(List<String> keys, List<String> fields) {
		logger.trace("Creating record class for keys {}, fields {}", keys, fields);
		AsmBuilder<Object> builder = new AsmBuilder<>(classLoader, Object.class);
		for (String key : keys) {
			KeyType d = this.keys.get(key);
			builder.field(key, d.getDataType());
		}
		for (String field : fields) {
			FieldType m = this.fields.get(field);
			builder.field(field, m.getDataType());
		}
		builder.method("toString", asString(newArrayList(concat(keys, fields))));
		return builder.defineClass();
	}

	public Class<?> createResultClass(AggregationQuery query) {
		logger.trace("Creating result class for query {}", query.toString());
		AsmBuilder<Object> builder = new AsmBuilder<>(classLoader, Object.class);
		List<String> resultKeys = query.getResultKeys();
		List<String> resultFields = query.getResultFields();
		for (String key : resultKeys) {
			KeyType keyType = this.keys.get(key);
			if (keyType == null) {
				throw new AggregationException("Key with the name '" + key + "' not found.");
			}
			builder.field(key, keyType.getDataType());
		}
		for (String field : resultFields) {
			FieldType fieldType = this.fields.get(field);
			if (fieldType == null) {
				throw new AggregationException("Field with the name '" + field + "' not found.");
			}
			builder.field(field, fieldType.getDataType());
		}
		builder.method("toString", asString(newArrayList(concat(resultKeys, resultFields))));
		return builder.defineClass();
	}

	public <T> BufferSerializer<T> createBufferSerializer(Class<T> recordClass, List<String> keys, List<String> fields) {
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
		return SerializerBuilder.newDefaultInstance(classLoader).create(serializerGenClass);
	}

	@Override
	public String toString() {
		return MoreObjects.toStringHelper(this)
				.add("keys", keys)
				.add("fields", fields)
				.toString();
	}
}
