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

import java.util.LinkedHashMap;
import java.util.Map;

public class CreateResponse {
	private Object data;
	private Object errors;

	public CreateResponse(Object data) {
		this.data = data;
	}

	public CreateResponse(Object data, Object errors) {
		this.data = data;
		this.errors = errors;
	}

	public void setErrors(Object errors) {
		this.errors = errors;
	}

	Map<String, Object> toMap() {
		Map<String, Object> map = new LinkedHashMap<>();
		if (data != null) map.put("data", data);
		if (errors != null) map.put("errors", errors);
		return map;
	}
}
