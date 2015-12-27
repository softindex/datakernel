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

public interface GridModel<K, R extends AbstractRecord<K>> {
	void create(R record, ResultCallback<CreateResponse<K>> callback);

	void read(K id, ReadSettings settings, ResultCallback<R> callback);

	void read(ReadSettings settings, ResultCallback<ReadResponse<K, R>> callback);

	void update(List<R> records, ResultCallback<UpdateResponse<K, R>> callback);

	void delete(K id, ResultCallback<DeleteResponse> callback);

	Class<K> getIdType();

	Class<R> getRecordType();
}