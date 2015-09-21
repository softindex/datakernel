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

import java.lang.reflect.Type;
import java.util.Map;

public final class AggregationQueryGsonSerializer implements JsonSerializer<AggregationQuery>,
		JsonDeserializer<AggregationQuery> {
	@Override
	public AggregationQuery deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
		JsonObject jsonObject = (JsonObject) json;
		JsonArray keys = (JsonArray) jsonObject.get("dimensions");
		JsonArray fields = (JsonArray) jsonObject.get("measures");
		JsonObject predicates = (JsonObject) jsonObject.get("filters");
		JsonObject orderings = (JsonObject) jsonObject.get("orderings");
		AggregationQuery query = new AggregationQuery();

		if (keys != null) {
			for (JsonElement key : keys) {
				query.key(key.getAsString());
			}
		}

		if (fields != null) {
			for (JsonElement field : fields) {
				query.field(field.getAsString());
			}
		}

		if (predicates != null) {
			AggregationQuery.QueryPredicates queryPredicates = context.deserialize(predicates, AggregationQuery.QueryPredicates.class);
			query.predicates(queryPredicates);
		}

		if (orderings != null) {
			for (Map.Entry<String, JsonElement> entry : orderings.entrySet()) {
				String field = entry.getKey();
				if ("asc".equals(entry.getValue().getAsString())) {
					query.orderAsc(field);
				} else if ("desc".equals(entry.getValue().getAsString())) {
					query.orderDesc(field);
				} else
					throw new IllegalArgumentException();
			}
		}

		// for compatibility
		JsonPrimitive singleKey = (JsonPrimitive) jsonObject.get("dimension");
		if (singleKey != null) {
			query.key(singleKey.getAsString());
		}

		return query;
	}

	@Override
	public JsonElement serialize(AggregationQuery query, Type typeOfSrc, JsonSerializationContext context) {
		JsonObject result = new JsonObject();

		// for compatibility
		if (query.getResultKeys().size() == 1) {
			result.add("dimension", new JsonPrimitive(query.getResultKeys().get(0)));
		} else {
			JsonArray keys = new JsonArray();
			for (String key : query.getResultKeys()) {
				keys.add(new JsonPrimitive(key));
			}
			result.add("dimensions", keys);
		}

		JsonArray measures = new JsonArray();
		for (String field : query.getResultFields()) {
			measures.add(new JsonPrimitive(field));
		}
		result.add("measures", measures);

		JsonElement predicates = context.serialize(query.getPredicates());
		result.add("filters", predicates);

		JsonObject orderings = new JsonObject();
		for (AggregationQuery.AggregationOrdering ordering : query.getOrderings()) {
			orderings.add(ordering.getField(), new JsonPrimitive(ordering.isAsc() ? "asc" : "desc"));
		}
		result.add("orderings", orderings);

		return result;
	}
}
