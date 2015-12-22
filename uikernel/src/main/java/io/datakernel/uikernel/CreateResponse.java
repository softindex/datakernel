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
import com.google.gson.JsonObject;

import java.util.List;
import java.util.Map;

@SuppressWarnings("unused")
public final class CreateResponse<T> {
	private T id;
	private Map<String, List<String>> errors;

	public CreateResponse(T id) {
		this.id = id;
	}

	public CreateResponse(T id, Map<String, List<String>> errors) {
		this.id = id;
		this.errors = errors;
	}

	public void setErrors(Map<String, List<String>> errors) {
		this.errors = errors;
	}

	String toJson(Gson gson, Class<T> idType) {
		JsonObject root = new JsonObject();
		if (id != null) root.add("data", gson.toJsonTree(id, idType));
		if (errors != null) root.add("errors", gson.toJsonTree(errors));
		return gson.toJson(root);
	}
}
