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

import com.google.common.base.Preconditions;
import com.google.gson.*;
import io.datakernel.cube.CubeQuery;
import io.datakernel.cube.CubeStructure;
import io.datakernel.cube.dimensiontype.DimensionType;

import java.lang.reflect.Type;
import java.util.Map;

public final class CubePredicatesGsonSerializer implements JsonSerializer<CubeQuery.CubePredicates>, JsonDeserializer<CubeQuery.CubePredicates> {
	private final CubeStructure structure;

	public CubePredicatesGsonSerializer(CubeStructure structure) {
		this.structure = structure;
	}

	private JsonPrimitive encodeDimension(String dimension, Object value) {
		DimensionType dimensionType = structure.getDimensionType(dimension);
		return new JsonPrimitive(dimensionType.toString(value));
	}

	private Object parseDimension(String dimension, JsonElement value) {
		DimensionType dimensionType = structure.getDimensionType(dimension);
		return dimensionType.toInternalRepresentation(value.getAsString());
	}

	@SuppressWarnings("ConstantConditions")
	@Override
	public CubeQuery.CubePredicates deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
		Preconditions.checkArgument(json instanceof JsonObject);

		JsonObject predicates = (JsonObject) json;
		CubeQuery.CubePredicates cubePredicates = new CubeQuery.CubePredicates();

		for (Map.Entry<String, JsonElement> entry : predicates.entrySet()) {
			JsonElement value = entry.getValue();
			String dimension = entry.getKey();
			if (value instanceof JsonPrimitive) {
				cubePredicates.eq(dimension, parseDimension(dimension, value));
			} else if (value instanceof JsonArray) {
				JsonArray range = (JsonArray) ((JsonArray) value).get(1);
				cubePredicates.between(dimension, parseDimension(dimension, range.get(0)), parseDimension(dimension, range.get(1)));
			}
		}
		return cubePredicates;
	}

	@Override
	public JsonElement serialize(CubeQuery.CubePredicates cubePredicates, Type typeOfSrc, JsonSerializationContext context) {
		JsonObject predicates = new JsonObject();

		for (CubeQuery.CubePredicate cubePredicate : cubePredicates.predicates()) {
			if (cubePredicate instanceof CubeQuery.CubePredicateEq) {
				Object value = ((CubeQuery.CubePredicateEq) cubePredicate).value;
				predicates.add(cubePredicate.dimension, encodeDimension(cubePredicate.dimension, value));
			} else if (cubePredicate instanceof CubeQuery.CubePredicateBetween) {
				Object from = ((CubeQuery.CubePredicateBetween) cubePredicate).from;
				Object to = ((CubeQuery.CubePredicateBetween) cubePredicate).to;

				JsonArray betweenPredicate = new JsonArray();
				betweenPredicate.add(new JsonPrimitive("between"));

				JsonArray range = new JsonArray();
				range.add(encodeDimension(cubePredicate.dimension, from));
				range.add(encodeDimension(cubePredicate.dimension, to));
				betweenPredicate.add(range);
				predicates.add(cubePredicate.dimension, betweenPredicate);
			} else {
				throw new UnsupportedOperationException();
			}
		}

		return predicates;
	}
}
