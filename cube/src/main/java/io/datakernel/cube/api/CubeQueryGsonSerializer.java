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

package io.datakernel.cube.api;

import com.google.gson.*;
import io.datakernel.cube.CubeQuery;

import java.lang.reflect.Type;
import java.util.Map;

public final class CubeQueryGsonSerializer implements JsonSerializer<CubeQuery>, JsonDeserializer<CubeQuery> {
	@Override
	public CubeQuery deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
		JsonObject jsonObject = (JsonObject) json;
		JsonArray dimensions = (JsonArray) jsonObject.get("dimensions");
		JsonArray measures = (JsonArray) jsonObject.get("measures");
		JsonObject predicates = (JsonObject) jsonObject.get("filters");
		JsonObject orderings = (JsonObject) jsonObject.get("orderings");
		CubeQuery query = new CubeQuery();

		if (dimensions != null) {
			for (JsonElement dimension : dimensions) {
				query.dimension(dimension.getAsString());
			}
		}

		if (measures != null) {
			for (JsonElement measure : measures) {
				query.measure(measure.getAsString());
			}
		}

		if (predicates != null) {
			CubeQuery.CubePredicates cubePredicates = context.deserialize(predicates, CubeQuery.CubePredicates.class);
			query.predicates(cubePredicates);
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
		JsonPrimitive singleDimension = (JsonPrimitive) jsonObject.get("dimension");
		if (singleDimension != null) {
			query.dimension(singleDimension.getAsString());
		}

		return query;
	}

	@Override
	public JsonElement serialize(CubeQuery query, Type typeOfSrc, JsonSerializationContext context) {
		JsonObject result = new JsonObject();

		// for compatibility
		if (query.getResultDimensions().size() == 1) {
			result.add("dimension", new JsonPrimitive(query.getResultDimensions().get(0)));
		} else {
			JsonArray dimensions = new JsonArray();
			for (String dimension : query.getResultDimensions()) {
				dimensions.add(new JsonPrimitive(dimension));
			}
			result.add("dimensions", dimensions);
		}

		JsonArray measures = new JsonArray();
		for (String metric : query.getResultMeasures()) {
			measures.add(new JsonPrimitive(metric));
		}
		result.add("measures", measures);

		JsonElement predicates = context.serialize(query.getPredicates());
		result.add("filters", predicates);

		JsonObject orderings = new JsonObject();
		for (CubeQuery.CubeOrdering ordering : query.getOrderings()) {
			orderings.add(ordering.getField(), new JsonPrimitive(ordering.isAsc() ? "asc" : "desc"));
		}
		result.add("orderings", orderings);

		return result;
	}
}
