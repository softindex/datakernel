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
import io.datakernel.aggregation_db.AggregationStructure;
import io.datakernel.aggregation_db.api.QueryException;
import io.datakernel.aggregation_db.keytype.KeyType;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;

public final class HavingPredicateNotEqualsGsonSerializer implements JsonDeserializer<List<HavingPredicateNotEquals>> {
	private final AggregationStructure structure;

	public HavingPredicateNotEqualsGsonSerializer(AggregationStructure structure) {
		this.structure = structure;
	}

	private Object parseKey(String key, JsonElement value) {
		KeyType keyType = structure.getKeyType(key);
		return keyType.fromJson(value);
	}

	@Override
	public List<HavingPredicateNotEquals> deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
		if (!(json instanceof JsonObject))
			throw new QueryException("Incorrect having predicate format. Should be represented as a JSON object");

		JsonObject predicatesJson = (JsonObject) json;
		List<HavingPredicateNotEquals> havingPredicates = newArrayList();

		for (Map.Entry<String, JsonElement> entry : predicatesJson.entrySet()) {
			JsonElement value = entry.getValue();
			String key = entry.getKey();

			if (value instanceof JsonArray && ((JsonArray) value).get(0) instanceof JsonPrimitive &&
					((JsonArray) value).get(0).getAsString().equals("ne") && ((JsonArray) value).get(1) instanceof JsonPrimitive) {
				havingPredicates.add(HavingPredicateNotEquals.ne(key, parseKey(key, ((JsonArray) value).get(1))));
			} else {
				throw new QueryException("Incorrect 'HAVING' predicate format");
			}
		}

		return havingPredicates;
	}
}
