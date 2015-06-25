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

package io.datakernel.hashfs.protocol.gson.commands;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.datakernel.serializer.GsonSubclassesAdapter;

public class HashFsResponseSerialization {

	public static final Gson GSON = new GsonBuilder()
			.registerTypeAdapter(HashFsResponse.class, GsonSubclassesAdapter.builder()
					.subclassField("commandType")
					.subclass("Error", HashFsResponseError.class)
					.subclass("FileList", HashFsResponseFiles.class)
					.subclass("Delete", HashFsResponseDeletedByUser.class)
					.subclass("Replicas", HashFsResponseFileReplicas.class)
					.subclass("ResponseOperationOk", HashFsResponseOperationOk.class)
					.subclass("ResponseFileUploaded", HashFsResponseFileUploaded.class)
					.subclass("ResponseForListeners", HashFsResponseForListeners.class)
					.subclass("ResponseAliveServers", HashFsResponseAliveServers.class)
					.build())
			.setPrettyPrinting()
			.enableComplexMapKeySerialization()
			.create();

}



