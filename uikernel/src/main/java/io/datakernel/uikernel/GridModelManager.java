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

public interface GridModelManager<E extends AbstractRecord<T>, T> {
	void read(T id, ReadSettings settings, ResultCallback<E> callback);

	void read(ReadSettings settings, ResultCallback<ReadResponse<E, T>> callback);

	void create(E object, ResultCallback<CreateResponse<T>> callback);

	void update(List<E> list, ResultCallback<UpdateResponse<E, T>> callback);

	void delete(T id, ResultCallback<DeleteResponse> callback);

	Class<E> getType();

	Class<T> getIdType();
}