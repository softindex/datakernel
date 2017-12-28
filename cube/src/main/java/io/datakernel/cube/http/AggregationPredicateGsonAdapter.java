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

package io.datakernel.cube.http;

import com.google.gson.JsonParseException;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import io.datakernel.aggregation.AggregationPredicate;
import io.datakernel.utils.GsonAdapters.TypeAdapterRegistry;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.*;

import static io.datakernel.aggregation.AggregationPredicates.*;
import static io.datakernel.utils.GsonAdapters.asNullable;

final class AggregationPredicateGsonAdapter extends TypeAdapter<AggregationPredicate> {
	public static final String EMPTY_STRING = "";
	public static final String SPACES = "\\s+";
	public static final String EQ = "eq";
	public static final String NOT_EQ = "notEq";
	public static final String HAS = "has";
	public static final String GE = "ge";
	public static final String GT = "gt";
	public static final String LE = "le";
	public static final String LT = "lt";
	public static final String IN = "in";
	public static final String BETWEEN = "between";
	public static final String REGEXP = "regexp";
	public static final String AND = "and";
	public static final String OR = "or";
	public static final String NOT = "not";
	public static final String TRUE = "true";
	public static final String FALSE = "false";
	public static final String EQ_SIGN = "=";
	public static final String NOT_EQ_SIGN = "<>";
	public static final String GE_SIGN = ">=";
	public static final String GT_SIGN = ">";
	public static final String LE_SIGN = "<=";
	public static final String LT_SIGN = "<";
	public static final String IN_SIGN = "IN";
	private final Map<String, TypeAdapter<?>> attributeAdapters;

	private AggregationPredicateGsonAdapter(Map<String, TypeAdapter<?>> attributeAdapters) {
		this.attributeAdapters = attributeAdapters;
	}

	public static AggregationPredicateGsonAdapter create(TypeAdapterRegistry registry,
	                                                     Map<String, Type> attributeTypes, Map<String, Type> measureTypes) {
		Map<String, TypeAdapter<?>> attributeAdapters = new LinkedHashMap<>();
		for (String attribute : attributeTypes.keySet()) {
			attributeAdapters.put(attribute, asNullable(registry.getAdapter(attributeTypes.get(attribute))));
		}
		for (String measure : measureTypes.keySet()) {
			attributeAdapters.put(measure, registry.getAdapter(measureTypes.get(measure)));
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
	private void writeNotEq(JsonWriter writer, PredicateNotEq predicate) throws IOException {
		writer.value(predicate.getKey());
		TypeAdapter typeAdapter = attributeAdapters.get(predicate.getKey());
		typeAdapter.write(writer, predicate.getValue());
	}

	@SuppressWarnings("unchecked")
	private void writeGe(JsonWriter writer, PredicateGe predicate) throws IOException {
		writer.value(predicate.getKey());
		TypeAdapter typeAdapter = attributeAdapters.get(predicate.getKey());
		typeAdapter.write(writer, predicate.getValue());
	}

	@SuppressWarnings("unchecked")
	private void writeGt(JsonWriter writer, PredicateGt predicate) throws IOException {
		writer.value(predicate.getKey());
		TypeAdapter typeAdapter = attributeAdapters.get(predicate.getKey());
		typeAdapter.write(writer, predicate.getValue());
	}

	@SuppressWarnings("unchecked")
	private void writeLe(JsonWriter writer, PredicateLe predicate) throws IOException {
		writer.value(predicate.getKey());
		TypeAdapter typeAdapter = attributeAdapters.get(predicate.getKey());
		typeAdapter.write(writer, predicate.getValue());
	}

	@SuppressWarnings("unchecked")
	private void writeLt(JsonWriter writer, PredicateLt predicate) throws IOException {
		writer.value(predicate.getKey());
		TypeAdapter typeAdapter = attributeAdapters.get(predicate.getKey());
		typeAdapter.write(writer, predicate.getValue());
	}

	@SuppressWarnings("unchecked")
	private void writeIn(JsonWriter writer, PredicateIn predicate) throws IOException {
		writer.value(predicate.getKey());
		TypeAdapter typeAdapter = attributeAdapters.get(predicate.getKey());
		for (Object value : predicate.getValues()) {
			typeAdapter.write(writer, value);
		}
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
			if (predicate instanceof PredicateNotEq) {
				writer.value(NOT_EQ);
				writeNotEq(writer, (PredicateNotEq) predicate);
			} else if (predicate instanceof PredicateGe) {
				writer.value(GE);
				writeGe(writer, (PredicateGe) predicate);
			} else if (predicate instanceof PredicateHas) {
				writer.value(HAS);
				writer.value(((PredicateHas) predicate).getKey());
			} else if (predicate instanceof PredicateGt) {
				writer.value(GT);
				writeGt(writer, (PredicateGt) predicate);
			} else if (predicate instanceof PredicateLe) {
				writer.value(LE);
				writeLe(writer, (PredicateLe) predicate);
			} else if (predicate instanceof PredicateLt) {
				writer.value(LT);
				writeLt(writer, (PredicateLt) predicate);
			} else if (predicate instanceof PredicateIn) {
				writer.value(IN);
				writeIn(writer, (PredicateIn) predicate);
			} else if (predicate instanceof PredicateBetween) {
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

	private AggregationPredicate readObjectWithAlgebraOfSetsOperator(JsonReader reader) throws IOException {
		List<AggregationPredicate> predicates = new ArrayList<>();
		while (reader.hasNext()) {
			String[] fieldWithOperator = reader.nextName().split(SPACES);
			String field = fieldWithOperator[0];
			String operator = (fieldWithOperator.length == 1) ? EMPTY_STRING : fieldWithOperator[1];
			TypeAdapter typeAdapter = attributeAdapters.get(field);
			Object value = typeAdapter.read(reader);
			AggregationPredicate comparisonPredicate;
			switch (operator) {
				case EMPTY_STRING:
				case EQ_SIGN:
					comparisonPredicate = eq(field, value);
					break;
				case NOT_EQ_SIGN:
					comparisonPredicate = notEq(field, value);
					break;
				case GE_SIGN:
					comparisonPredicate = ge(field, (Comparable) value);
					break;
				case GT_SIGN:
					comparisonPredicate = gt(field, (Comparable) value);
					break;
				case LE_SIGN:
					comparisonPredicate = le(field, (Comparable) value);
					break;
				case LT_SIGN:
					comparisonPredicate = lt(field, (Comparable) value);
					break;
				case IN_SIGN:
					comparisonPredicate = in(field, (Set) value);
					break;
				default:
					throw new IllegalArgumentException();
			}
			predicates.add(comparisonPredicate);
		}
		return predicates.size() == 1 ? predicates.get(0) : and(predicates);
	}

	private AggregationPredicate readEq(JsonReader reader) throws IOException {
		String field = reader.nextString();
		TypeAdapter typeAdapter = attributeAdapters.get(field);
		Object value = typeAdapter.read(reader);
		return eq(field, value);
	}

	private AggregationPredicate readNotEq(JsonReader reader) throws IOException {
		String field = reader.nextString();
		TypeAdapter typeAdapter = attributeAdapters.get(field);
		Object value = typeAdapter.read(reader);
		return notEq(field, value);
	}

	private AggregationPredicate readGe(JsonReader reader) throws IOException {
		String field = reader.nextString();
		TypeAdapter typeAdapter = attributeAdapters.get(field);
		Comparable value = (Comparable) typeAdapter.read(reader);
		return ge(field, value);
	}

	private AggregationPredicate readGt(JsonReader reader) throws IOException {
		String field = reader.nextString();
		TypeAdapter typeAdapter = attributeAdapters.get(field);
		Comparable value = (Comparable) typeAdapter.read(reader);
		return gt(field, value);
	}

	private AggregationPredicate readLe(JsonReader reader) throws IOException {
		String field = reader.nextString();
		TypeAdapter typeAdapter = attributeAdapters.get(field);
		Comparable value = (Comparable) typeAdapter.read(reader);
		return le(field, value);
	}

	private AggregationPredicate readLt(JsonReader reader) throws IOException {
		String field = reader.nextString();
		TypeAdapter typeAdapter = attributeAdapters.get(field);
		Comparable value = (Comparable) typeAdapter.read(reader);
		return lt(field, value);
	}

	private AggregationPredicate readIn(JsonReader reader) throws IOException {
		String field = reader.nextString();
		TypeAdapter typeAdapter = attributeAdapters.get(field);
		Set values = new LinkedHashSet();
		while (reader.hasNext()) {
			Object value = typeAdapter.read(reader);
			values.add(value);
		}
		return in(field, values);
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
		List<AggregationPredicate> predicates = new ArrayList<>();
		while (reader.hasNext()) {
			AggregationPredicate predicate = read(reader);
			predicates.add(predicate);
		}
		return and(predicates);
	}

	private AggregationPredicate readOr(JsonReader reader) throws IOException {
		List<AggregationPredicate> predicates = new ArrayList<>();
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
			predicate = readObjectWithAlgebraOfSetsOperator(reader);
			reader.endObject();
		} else {
			reader.beginArray();
			String type = reader.nextString();
			if (EQ.equals(type))
				predicate = readEq(reader);
			if (NOT_EQ.equals(type))
				predicate = readNotEq(reader);
			if (GE.equals(type))
				predicate = readGe(reader);
			if (GT.equals(type))
				predicate = readGt(reader);
			if (LE.equals(type))
				predicate = readLe(reader);
			if (LT.equals(type))
				predicate = readLt(reader);
			if (IN.equals(type))
				predicate = readIn(reader);
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
