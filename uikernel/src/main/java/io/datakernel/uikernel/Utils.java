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

package io.datakernel.uikernel;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;

import java.util.ArrayList;
import java.util.List;

class Utils {
	static <K, R extends AbstractRecord<K>> List<R> deserializeUpdateRequest(Gson gson, String json, Class<R> type, Class<K> idType) {
		List<R> result = new ArrayList<>();
		JsonArray root = gson.fromJson(json, JsonArray.class);
		for (JsonElement element : root) {
			JsonArray arr = gson.fromJson(element, JsonArray.class);
			K id = gson.fromJson(arr.get(0), idType);
			R obj = gson.fromJson(arr.get(1), type);
			obj.setId(id);
			result.add(obj);
		}
		return result;
	}

	static <E> E checkNotNull(E object, String msg) {
		if (object == null)
			throw new NullPointerException(msg);
		return object;
	}
}
