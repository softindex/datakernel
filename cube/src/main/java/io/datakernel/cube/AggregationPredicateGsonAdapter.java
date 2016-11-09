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

package io.datakernel.cube;

import com.google.gson.Gson;
import com.google.gson.JsonParseException;
import com.google.gson.TypeAdapter;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import io.datakernel.aggregation_db.AggregationPredicate;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newLinkedHashMap;
import static io.datakernel.aggregation_db.AggregationPredicates.*;

public final class AggregationPredicateGsonAdapter extends TypeAdapter<AggregationPredicate> {
	public static final String EQ = "eq";
	public static final String BETWEEN = "between";
	public static final String REGEXP = "regexp";
	public static final String AND = "and";
	public static final String OR = "or";
	public static final String NOT = "not";
	public static final String TRUE = "true";
	public static final String FALSE = "false";
	private final Map<String, TypeAdapter<?>> attributeAdapters;

	private AggregationPredicateGsonAdapter(Map<String, TypeAdapter<?>> attributeAdapters) {
		this.attributeAdapters = attributeAdapters;
	}

	public static AggregationPredicateGsonAdapter create(Gson gson, Map<String, Type> attributeTypes, Map<String, Type> measureTypes) {
		Map<String, TypeAdapter<?>> attributeAdapters = newLinkedHashMap();
		for (String attribute : attributeTypes.keySet()) {
			TypeToken<?> typeToken = TypeToken.get(attributeTypes.get(attribute));
			attributeAdapters.put(attribute, gson.getAdapter(typeToken));
		}
		for (String measure : measureTypes.keySet()) {
			TypeToken<?> typeToken = TypeToken.get(measureTypes.get(measure));
			attributeAdapters.put(measure, gson.getAdapter(typeToken));
		}
		return new AggregationPredicateGsonAdapter(attributeAdapters);
	}

	@SuppressWarnings("unchecked")
	private void writeEq(JsonWriter writer, PredicateEq predicate) throws IOException {
		writer.name(predicate.getKey());
		TypeAdapter typeAdapter = attributeAdapters.get(predicate.getKey());
		typeAdapter.write(writer, predicate.getValue());
	}

	@SuppressWarnings("unchecked")
	private void writeBetween(JsonWriter writer, PredicateBetween predicate) throws IOException {
		writer.value(predicate.getKey());
		TypeAdapter typeAdapter = attributeAdapters.get(predicate.getKey());
		typeAdapter.write(writer, predicate.getFrom());
		typeAdapter.write(writer, predicate.getTo());
	}

	@SuppressWarnings("unchecked")
	private void writeRegexp(JsonWriter writer, PredicateRegexp predicate) throws IOException {
		writer.value(predicate.getKey());
		writer.value(predicate.getRegexp());
	}

	@SuppressWarnings("unchecked")
	private void writeAnd(JsonWriter writer, PredicateAnd predicate) throws IOException {
		for (AggregationPredicate p : predicate.getPredicates()) {
			write(writer, p);
		}
	}

	@SuppressWarnings("unchecked")
	private void writeOr(JsonWriter writer, PredicateOr predicate) throws IOException {
		for (AggregationPredicate p : predicate.getPredicates()) {
			write(writer, p);
		}
	}

	@SuppressWarnings("unchecked")
	private void writeNot(JsonWriter writer, PredicateNot predicate) throws IOException {
		write(writer, predicate.getPredicate());
	}

	@Override
	public void write(JsonWriter writer, AggregationPredicate predicate) throws IOException {
		if (predicate instanceof PredicateEq) {
			writer.beginObject();
			writeEq(writer, (PredicateEq) predicate);
			writer.endObject();
		} else {
			writer.beginArray();
			if (predicate instanceof PredicateBetween) {
				writer.value(BETWEEN);
				writeBetween(writer, (PredicateBetween) predicate);
			} else if (predicate instanceof PredicateRegexp) {
				writer.value(REGEXP);
				writeRegexp(writer, (PredicateRegexp) predicate);
			} else if (predicate instanceof PredicateAnd) {
				writer.value(AND);
				writeAnd(writer, (PredicateAnd) predicate);
			} else if (predicate instanceof PredicateOr) {
				writer.value(OR);
				writeOr(writer, (PredicateOr) predicate);
			} else if (predicate instanceof PredicateNot) {
				writer.value(NOT);
				writeNot(writer, (PredicateNot) predicate);
			} else if (predicate instanceof PredicateAlwaysTrue) {
				writer.value(TRUE);
			} else if (predicate instanceof PredicateAlwaysFalse) {
				writer.value(FALSE);
			} else
				throw new IllegalArgumentException();
			writer.endArray();
		}
	}

	private AggregationPredicate readEqOfObject(JsonReader reader) throws IOException {
		List<AggregationPredicate> predicates = newArrayList();
		while (reader.hasNext()) {
			String field = reader.nextName();
			TypeAdapter typeAdapter = attributeAdapters.get(field);
			Object value = typeAdapter.read(reader);
			predicates.add(eq(field, value));
		}
		return predicates.size() == 1 ? predicates.get(0) : and(predicates);
	}

	private AggregationPredicate readEq(JsonReader reader) throws IOException {
		String field = reader.nextString();
		TypeAdapter typeAdapter = attributeAdapters.get(field);
		Object value = typeAdapter.read(reader);
		return eq(field, value);
	}

	private AggregationPredicate readBetween(JsonReader reader) throws IOException {
		String field = reader.nextString();
		TypeAdapter typeAdapter = attributeAdapters.get(field);
		Comparable from = (Comparable) typeAdapter.read(reader);
		Comparable to = (Comparable) typeAdapter.read(reader);
		return between(field, from, to);
	}

	private AggregationPredicate readRegexp(JsonReader reader) throws IOException {
		String field = reader.nextString();
		String regexp = reader.nextString();
		return regexp(field, regexp);
	}

	private AggregationPredicate readAnd(JsonReader reader) throws IOException {
		List<AggregationPredicate> predicates = newArrayList();
		while (reader.hasNext()) {
			AggregationPredicate predicate = read(reader);
			predicates.add(predicate);
		}
		return and(predicates);
	}

	private AggregationPredicate readOr(JsonReader reader) throws IOException {
		List<AggregationPredicate> predicates = newArrayList();
		while (reader.hasNext()) {
			AggregationPredicate predicate = read(reader);
			predicates.add(predicate);
		}
		return or(predicates);
	}

	private AggregationPredicate readNot(JsonReader reader) throws IOException {
		AggregationPredicate predicate = read(reader);
		return not(predicate);
	}

	@Override
	public AggregationPredicate read(JsonReader reader) throws IOException {
		AggregationPredicate predicate = null;
		if (reader.peek() == JsonToken.BEGIN_OBJECT) {
			reader.beginObject();
			predicate = readEqOfObject(reader);
			reader.endObject();
		} else {
			reader.beginArray();
			String type = reader.nextString();
			if (EQ.equals(type))
				predicate = readEq(reader);
			if (BETWEEN.equals(type))
				predicate = readBetween(reader);
			if (REGEXP.equals(type))
				predicate = readRegexp(reader);
			if (AND.equals(type))
				predicate = readAnd(reader);
			if (OR.equals(type))
				predicate = readOr(reader);
			if (NOT.equals(type))
				predicate = readNot(reader);
			if (TRUE.equals(type))
				predicate = alwaysTrue();
			if (FALSE.equals(type))
				predicate = alwaysFalse();
			if (predicate == null)
				throw new JsonParseException("Unknown predicate type " + type);
			reader.endArray();
		}
		return predicate;
	}
}
