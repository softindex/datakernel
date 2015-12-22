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
import com.google.gson.JsonObject;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuppressWarnings("unused")
public final class UpdateResponse<E extends HasId<T>, T> {
	private final List<E> changes;
	private final Map<T, Map<String, List<String>>> errors = new HashMap<>();

	public UpdateResponse(List<E> changes) {
		this.changes = changes;
	}

	public UpdateResponse(List<E> changes, Map<T, Map<String, List<String>>> errors) {
		this.changes = changes;
		this.errors.putAll(errors);
	}

	public void addErrors(Map<T, Map<String, List<String>>> errors) {
		this.errors.putAll(errors);
	}

	String toJson(Gson gson, Class<E> type, Class<T> idType) {
		JsonObject root = new JsonObject();

		JsonArray chang = new JsonArray();
		for (E record : changes) {
			JsonArray arr = new JsonArray();
			arr.add(gson.toJsonTree(record.getId(), idType));
			arr.add(gson.toJsonTree(record, type));
			chang.add(arr);
		}
		root.add("changes", chang);

		JsonArray errs = new JsonArray();
		for (Map.Entry<T, Map<String, List<String>>> entry : errors.entrySet()) {
			JsonArray arr = new JsonArray();
			arr.add(gson.toJsonTree(entry.getKey(), idType));
			arr.add(gson.toJsonTree(entry.getValue()));
			errs.add(arr);
		}
		root.add("errors", errs);

		return gson.toJson(root);
	}
}
