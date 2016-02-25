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

package io.datakernel.aggregation_db.gson;

import com.google.gson.*;
import io.datakernel.aggregation_db.AggregationQuery;
import io.datakernel.aggregation_db.AggregationStructure;
import io.datakernel.aggregation_db.api.QueryException;
import io.datakernel.aggregation_db.keytype.KeyType;

import java.lang.reflect.Type;
import java.util.Map;

public final class QueryPredicatesGsonSerializer implements JsonSerializer<AggregationQuery.Predicates>,
		JsonDeserializer<AggregationQuery.Predicates> {
	private final AggregationStructure structure;

	public QueryPredicatesGsonSerializer(AggregationStructure structure) {
		this.structure = structure;
	}

	private JsonPrimitive encodeKey(String key, Object value) {
		KeyType keyType = structure.getKeyType(key);
		Object printable = keyType.getPrintable(value);
		return printable instanceof Number ? new JsonPrimitive((Number) printable) : new JsonPrimitive(printable.toString());
	}

	private Object parseKey(String key, JsonElement value) {
		KeyType keyType = structure.getKeyType(key);
		return keyType.fromString(value.getAsString());
	}

	@SuppressWarnings("ConstantConditions")
	@Override
	public AggregationQuery.Predicates deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
		if (!(json instanceof JsonObject))
			throw new QueryException("Incorrect filters format. Should be represented as a JSON object");

		JsonObject predicates = (JsonObject) json;
		AggregationQuery.Predicates queryPredicates = new AggregationQuery.Predicates();

		for (Map.Entry<String, JsonElement> entry : predicates.entrySet()) {
			JsonElement value = entry.getValue();
			String key = entry.getKey();
			if (value instanceof JsonPrimitive) {
				queryPredicates.eq(key, parseKey(key, value));
			} else if (value instanceof JsonArray && ((JsonArray) value).get(0) instanceof JsonPrimitive &&
					((JsonArray) value).get(0).getAsString().equals("between") && ((JsonArray) value).get(1) instanceof JsonArray) {
				JsonArray range = (JsonArray) ((JsonArray) value).get(1);
				queryPredicates.between(key, parseKey(key, range.get(0)), parseKey(key, range.get(1)));
			} else if (value instanceof JsonArray && ((JsonArray) value).get(0) instanceof JsonPrimitive &&
					((JsonArray) value).get(0).getAsString().equals("ne") && ((JsonArray) value).get(1) instanceof JsonPrimitive) {
				queryPredicates.ne(key, parseKey(key, ((JsonArray) value).get(1)));
			} else {
				throw new QueryException("Incorrect filters format.");
			}
		}
		return queryPredicates;
	}

	@Override
	public JsonElement serialize(AggregationQuery.Predicates queryPredicates, Type typeOfSrc, JsonSerializationContext context) {
		JsonObject predicates = new JsonObject();

		for (AggregationQuery.Predicate queryPredicate : queryPredicates.asCollection()) {
			if (queryPredicate instanceof AggregationQuery.PredicateEq) {
				Object value = ((AggregationQuery.PredicateEq) queryPredicate).value;
				predicates.add(queryPredicate.key, encodeKey(queryPredicate.key, value));
			} else if (queryPredicate instanceof AggregationQuery.PredicateBetween) {
				Object from = ((AggregationQuery.PredicateBetween) queryPredicate).from;
				Object to = ((AggregationQuery.PredicateBetween) queryPredicate).to;

				JsonArray betweenPredicate = new JsonArray();
				betweenPredicate.add(new JsonPrimitive("between"));

				JsonArray range = new JsonArray();
				range.add(encodeKey(queryPredicate.key, from));
				range.add(encodeKey(queryPredicate.key, to));
				betweenPredicate.add(range);
				predicates.add(queryPredicate.key, betweenPredicate);
			} else {
				throw new UnsupportedOperationException();
			}
		}

		return predicates;
	}
}
