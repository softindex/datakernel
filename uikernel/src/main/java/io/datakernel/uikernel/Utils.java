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

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.List;

public class Utils {
	public static <E extends AbstractRecord<T>, T> List<E> deserializeUpdateRequest(Gson gson, String json, Class<E> type, Class<T> idType) {
		List<E> result = new ArrayList<>();
		JsonArray root = gson.fromJson(json, JsonArray.class);
		for (JsonElement element : root) {
			JsonArray arr = gson.fromJson(element, JsonArray.class);
			T id = gson.fromJson(arr.get(0), idType);
			E obj = gson.fromJson(arr.get(1), type);
			obj.setId(id);
			result.add(obj);
		}
		return result;
	}

	public static String decodeUtf8Query(String query) {
		try {
			return URLDecoder.decode(query, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
	}
}
