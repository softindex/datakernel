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

import io.datakernel.async.ResultCallback;

import java.util.List;
import java.util.Map;

public interface Controller {
	void read(String tableName, ReadSettings settings, ResultCallback<ReadResponse> callback);

	void read(String tableName, ReadSettings settings, Integer id, ResultCallback<Map<String, Object>> callback);

	void create(String tableName, Map<String, Object> map, ResultCallback<CreateResponse> callback);

	void update(String tableName, List<List<Object>> list, ResultCallback<UpdateResponse> callback);

	void delete(String tableName, Integer id, ResultCallback<DeleteResponse> callback);
}