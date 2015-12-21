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

import com.google.gson.*;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class Utils {

	public static LinkedHashMap<String, Object> parseAsObject(Gson gson, String json) {
		JsonElement element = gson.fromJson(json, JsonObject.class);
		return parse(element.getAsJsonObject());
	}

	public static List parseAsArray(Gson gson, String json) {
		JsonElement element = gson.fromJson(json, JsonArray.class);
		return parse(element.getAsJsonArray());
	}

	public static LinkedHashMap<String, Object> parse(JsonObject obj) {
		LinkedHashMap<String, Object> root = new LinkedHashMap<>();
		for (Map.Entry<String, JsonElement> entry : obj.entrySet()) {
			if (entry.getValue().isJsonObject()) {
				root.put(entry.getKey(), parse(entry.getValue().getAsJsonObject()));
			} else if (entry.getValue().isJsonArray()) {
				root.put(entry.getKey(), parse(entry.getValue().getAsJsonArray()));
			} else if (entry.getValue().isJsonPrimitive()) {
				root.put(entry.getKey(), parse(entry.getValue().getAsJsonPrimitive()));
			}
		}
		return root;
	}

	public static Object parse(JsonPrimitive pr) {
		Object obj;
		if (pr.isBoolean()) {
			obj = pr.getAsBoolean();
		} else if (pr.isString()) {
			obj = pr.getAsString();
		} else {
			obj = pr.getAsDouble();
		}
		return obj;
	}

	public static ArrayList<Object> parse(JsonArray arr) {
		ArrayList<Object> root = new ArrayList<>();
		for (JsonElement e : arr) {
			if (e.isJsonObject()) {
				root.add(parse(e.getAsJsonObject()));
			} else if (e.isJsonArray()) {
				root.add(parse(e.getAsJsonArray()));
			} else if (e.isJsonPrimitive()) {
				root.add(parse(e.getAsJsonPrimitive()));
			}
		}
		return root;
	}

	public static List<List<Object>> parseRecordsArray(Gson gson, String json) {
		ArrayList<List<Object>> root = new ArrayList<>();
		JsonArray arr = gson.fromJson(json, JsonArray.class);
		for (JsonElement e : arr) {
			List<Object> list = new ArrayList<>();
			for (JsonElement el : e.getAsJsonArray()) {
				if (el.isJsonPrimitive()) {
					list.add(el.getAsInt());
				} else {
					list.add(parse(el.getAsJsonObject()));
				}
			}
			root.add(list);
		}
		return root;
	}

	public static String render(Gson gson, ReadResponse model) {
		return gson.toJson(model.toMap());
	}

	public static String render(Gson gson, Map<String, Object> model) {
		return gson.toJson(model);
	}
}