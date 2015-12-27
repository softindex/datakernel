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
public final class CreateResponse<K> {
	private K id;
	private Map<String, List<String>> errors;

	public CreateResponse(K id) {
		this.id = id;
	}

	public CreateResponse(K id, Map<String, List<String>> errors) {
		this.id = id;
		this.errors = errors;
	}

	public void setErrors(Map<String, List<String>> errors) {
		this.errors = errors;
	}

	JsonObject toJson(Gson gson, Class<K> idType) {
		JsonObject result = new JsonObject();
		if (id != null) result.add("data", gson.toJsonTree(id, idType));
		if (errors != null) result.add("errors", gson.toJsonTree(errors));
		return result;
	}
}
