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
import java.util.List;
import java.util.Map;

public class UpdateResponse {
	private List<List<Object>> changes;
	private Object errors;

	public UpdateResponse(List<List<Object>> changes) {
		this.changes = changes;
	}

	public UpdateResponse(List<List<Object>> changes, Object errors) {
		this.changes = changes;
		this.errors = errors;
	}

	Map<String, Object> toMap() {
		Map<String, Object> map = new LinkedHashMap<>();
		if (changes != null) map.put("changes", changes);
		if (errors != null) map.put("errors", errors);
		return map;
	}
}
